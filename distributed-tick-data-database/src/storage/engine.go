package storage

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"distributed-tick-data-database/src/config"
	"distributed-tick-data-database/src/domain"
)

type FreshnessMetrics struct {
	SampleCount      int     `json:"sample_count"`
	LastEventTimeNS  int64   `json:"last_event_time_ns"`
	LastIngestTimeNS int64   `json:"last_ingest_time_ns"`
	LagP50MS         float64 `json:"lag_p50_ms"`
	LagP95MS         float64 `json:"lag_p95_ms"`
	LagP99MS         float64 `json:"lag_p99_ms"`
}

type EngineStorageStats struct {
	NodeID               string              `json:"node_id"`
	Topology             string              `json:"topology"`
	NumShards            int                 `json:"num_shards"`
	TotalSegments        int                 `json:"total_segments"`
	TotalSegmentBytes    int64               `json:"total_segment_bytes"`
	TotalUncompressedCol int64               `json:"total_uncompressed_col_bytes"`
	TotalWALBytes        int64               `json:"total_wal_bytes"`
	TotalMemtableEvents  int                 `json:"total_memtable_events"`
	TotalPersistedRows   uint64              `json:"total_persisted_rows"`
	TotalDiskBytes       int64               `json:"total_disk_bytes"`
	Shards               []ShardStorageStats `json:"shards"`
}

// Engine coordinates N shards, deterministic routing, metrics, and recovery.
type Engine struct {
	cfg         config.Config
	partitioner domain.Partitioner
	shards      []*Shard

	metricsMu        sync.Mutex
	ingestLagNS      []int64
	lastEventTimeNS  int64
	lastIngestTimeNS int64
	totalBatches     uint64
	totalAccepted    uint64
	totalDuplicates  uint64
	totalRejected    uint64
	totalLate        uint64
}

func OpenEngine(cfg config.Config) (*Engine, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	part := domain.NewPartitioner(cfg.NumShards, cfg.PartitionGranularity)
	shards := make([]*Shard, cfg.NumShards)
	for i := 0; i < cfg.NumShards; i++ {
		sh, err := OpenShard(uint16(i), cfg, part)
		if err != nil {
			for j := 0; j < i; j++ {
				_ = shards[j].Close(false)
			}
			return nil, err
		}
		shards[i] = sh
	}
	return &Engine{
		cfg:         cfg,
		partitioner: part,
		shards:      shards,
		ingestLagNS: make([]int64, 0, 4096),
	}, nil
}

func (e *Engine) Config() config.Config {
	return e.cfg
}

func (e *Engine) Partitioner() domain.Partitioner {
	return e.partitioner
}

func (e *Engine) Shard(id uint16) *Shard {
	return e.shards[id]
}

// IngestBatch routes raw ticks to their owning shards by instrument_id and
// commits each shard sub-batch durably. Invalid ticks with empty instrument_id
// are validated on shard 0 so structured errors are always returned.
func (e *Engine) IngestBatch(batchID string, rawTicks []domain.RawTick, ingestTimeNS int64) (domain.BatchResult, error) {
	if len(rawTicks) > e.cfg.Ingest.MaxBatchSize {
		return domain.BatchResult{}, &domain.DomainError{
			Code:    "ERR_BATCH_TOO_LARGE",
			Field:   "batch",
			Message: fmt.Sprintf("batch size %d exceeds max_batch_size %d", len(rawTicks), e.cfg.Ingest.MaxBatchSize),
		}
	}
	if ingestTimeNS == 0 {
		ingestTimeNS = time.Now().UTC().UnixNano()
	}

	type indexedTick struct {
		origIdx int
		tick    domain.RawTick
	}
	byShard := make(map[uint16][]indexedTick)
	for i, t := range rawTicks {
		var sid uint16
		if t.InstrumentID != "" {
			sid = e.partitioner.ShardForInstrument(t.InstrumentID)
		}
		byShard[sid] = append(byShard[sid], indexedTick{origIdx: i, tick: t})
	}

	agg := domain.BatchResult{
		BatchID:    batchID,
		TotalInput: len(rawTicks),
	}

	// Deterministic shard commit order
	shardIDs := make([]int, 0, len(byShard))
	for sid := range byShard {
		shardIDs = append(shardIDs, int(sid))
	}
	sort.Ints(shardIDs)

	for _, sidInt := range shardIDs {
		sid := uint16(sidInt)
		items := byShard[sid]
		subRaw := make([]domain.RawTick, len(items))
		for i, it := range items {
			subRaw[i] = it.tick
		}
		subRes, err := e.shards[sid].CommitBatch(batchID, subRaw, ingestTimeNS)
		if err != nil {
			return agg, err
		}
		agg.Accepted = append(agg.Accepted, subRes.Accepted...)
		agg.AcceptedCount += subRes.AcceptedCount
		agg.LateAcceptedCount += subRes.LateAcceptedCount
		agg.DuplicateCount += subRes.DuplicateCount
		agg.RejectedCount += subRes.RejectedCount
		for _, be := range subRes.Errors {
			be.Index = items[be.Index].origIdx
			agg.Errors = append(agg.Errors, be)
		}
	}

	sort.Slice(agg.Errors, func(i, j int) bool {
		return agg.Errors[i].Index < agg.Errors[j].Index
	})

	e.recordFreshness(agg, ingestTimeNS)
	return agg, nil
}

func (e *Engine) recordFreshness(res domain.BatchResult, ingestTimeNS int64) {
	e.metricsMu.Lock()
	defer e.metricsMu.Unlock()
	e.totalBatches++
	e.totalAccepted += uint64(res.AcceptedCount)
	e.totalDuplicates += uint64(res.DuplicateCount)
	e.totalRejected += uint64(res.RejectedCount)
	e.totalLate += uint64(res.LateAcceptedCount)
	if ingestTimeNS > e.lastIngestTimeNS {
		e.lastIngestTimeNS = ingestTimeNS
	}
	for _, t := range res.Accepted {
		if t.EventTimeNS > e.lastEventTimeNS {
			e.lastEventTimeNS = t.EventTimeNS
		}
		lag := ingestTimeNS - t.EventTimeNS
		if lag < 0 {
			lag = 0
		}
		if len(e.ingestLagNS) < 65536 {
			e.ingestLagNS = append(e.ingestLagNS, lag)
		}
	}
}

func (e *Engine) FlushAll() error {
	for _, sh := range e.shards {
		if err := sh.Flush(); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) Close(flush bool) error {
	var firstErr error
	for _, sh := range e.shards {
		if err := sh.Close(flush); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// ScanRange scans target shards (single shard when instrumentID != "") with
// time-bucket and ZoneMap pruning, returning results in canonical order.
func (e *Engine) ScanRange(instrumentID string, startNS, endNS int64) ([]domain.CanonicalTick, int, int, int, error) {
	buckets := e.partitioner.TimeBucketsInRange(startNS, endNS)
	allowed := make(map[string]struct{}, len(buckets))
	for _, b := range buckets {
		allowed[b] = struct{}{}
	}

	var targetShards []*Shard
	if instrumentID != "" {
		sid := e.partitioner.ShardForInstrument(instrumentID)
		targetShards = []*Shard{e.shards[sid]}
	} else {
		targetShards = e.shards
	}

	var all []domain.CanonicalTick
	totalScanned := 0
	totalPruned := 0
	for _, sh := range targetShards {
		rows, scanned, pruned, err := sh.QueryScan(instrumentID, startNS, endNS, allowed)
		if err != nil {
			return nil, totalScanned, totalPruned, len(targetShards), err
		}
		all = append(all, rows...)
		totalScanned += scanned
		totalPruned += pruned
	}

	sort.Slice(all, func(i, j int) bool {
		return domain.CompareCanonicalOrder(all[i], all[j]) < 0
	})
	return all, totalScanned, totalPruned, len(targetShards), nil
}

func (e *Engine) OpenGaps() []domain.StreamGapReport {
	var all []domain.StreamGapReport
	for _, sh := range e.shards {
		all = append(all, sh.OpenGaps()...)
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].SourceID != all[j].SourceID {
			return all[i].SourceID < all[j].SourceID
		}
		if all[i].InstrumentID != all[j].InstrumentID {
			return all[i].InstrumentID < all[j].InstrumentID
		}
		return all[i].StartSeq < all[j].StartSeq
	})
	return all
}

func (e *Engine) Completeness() []domain.StreamCompleteness {
	var all []domain.StreamCompleteness
	for _, sh := range e.shards {
		all = append(all, sh.Completeness()...)
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].SourceID != all[j].SourceID {
			return all[i].SourceID < all[j].SourceID
		}
		return all[i].InstrumentID < all[j].InstrumentID
	})
	return all
}

func (e *Engine) RecoveryReports() []ShardRecoveryReport {
	out := make([]ShardRecoveryReport, len(e.shards))
	for i, sh := range e.shards {
		out[i] = sh.RecoveryReport()
	}
	return out
}

func (e *Engine) StorageStats() EngineStorageStats {
	st := EngineStorageStats{
		NodeID:    e.cfg.NodeID,
		Topology:  e.cfg.Topology,
		NumShards: len(e.shards),
		Shards:    make([]ShardStorageStats, len(e.shards)),
	}
	for i, sh := range e.shards {
		ss := sh.Stats()
		st.Shards[i] = ss
		st.TotalSegments += ss.SegmentCount
		st.TotalSegmentBytes += ss.SegmentBytes
		st.TotalUncompressedCol += ss.UncompressedColBytes
		st.TotalWALBytes += ss.WALBytes
		st.TotalMemtableEvents += ss.MemtableEvents
		st.TotalPersistedRows += ss.PersistedSegmentRows
	}
	st.TotalDiskBytes = st.TotalSegmentBytes + st.TotalWALBytes
	return st
}

func (e *Engine) Freshness() FreshnessMetrics {
	e.metricsMu.Lock()
	defer e.metricsMu.Unlock()
	fm := FreshnessMetrics{
		SampleCount:      len(e.ingestLagNS),
		LastEventTimeNS:  e.lastEventTimeNS,
		LastIngestTimeNS: e.lastIngestTimeNS,
	}
	if len(e.ingestLagNS) == 0 {
		return fm
	}
	cp := make([]int64, len(e.ingestLagNS))
	copy(cp, e.ingestLagNS)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	fm.LagP50MS = float64(percentileI64(cp, 0.50)) / 1e6
	fm.LagP95MS = float64(percentileI64(cp, 0.95)) / 1e6
	fm.LagP99MS = float64(percentileI64(cp, 0.99)) / 1e6
	return fm
}

func percentileI64(sorted []int64, p float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
