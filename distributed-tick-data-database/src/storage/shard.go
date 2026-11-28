package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"distributed-tick-data-database/src/codec"
	"distributed-tick-data-database/src/config"
	"distributed-tick-data-database/src/domain"
)

type SegmentMeta struct {
	SegmentID     string        `json:"segment_id"`
	TimeBucket    string        `json:"time_bucket"`
	RelPath       string        `json:"rel_path"`
	FileSizeBytes int64         `json:"file_size_bytes"`
	ZoneMap       codec.ZoneMap `json:"zone_map"`
	CreatedAtNS   int64         `json:"created_at_ns"`
}

type ShardManifest struct {
	Version        string        `json:"version"`
	ShardID        uint16        `json:"shard_id"`
	LastWALSeq     uint64        `json:"last_wal_seq"`
	NextSegmentSeq uint64        `json:"next_segment_seq"`
	Segments       []SegmentMeta `json:"segments"`
}

type ShardRecoveryReport struct {
	ShardID                  uint16           `json:"shard_id"`
	WALStats                 WALRecoveryStats `json:"wal_stats"`
	AppliedWALFrames         int              `json:"applied_wal_frames"`
	AppliedWALEvents         int              `json:"applied_wal_events"`
	OrphanedTempFilesCleaned int              `json:"orphaned_temp_files_cleaned"`
	SegmentsLoaded           int              `json:"segments_loaded"`
	RecoveryDurationMS       float64          `json:"recovery_duration_ms"`
}

type Shard struct {
	mu          sync.RWMutex
	shardID     uint16
	dir         string
	cfg         config.Config
	partitioner domain.Partitioner
	wal         *WAL
	walSeq      uint64
	manifest    ShardManifest
	sequencer   *domain.Sequencer
	memtable    []domain.CanonicalTick
	recovery    ShardRecoveryReport
}

func OpenShard(shardID uint16, cfg config.Config, partitioner domain.Partitioner) (*Shard, error) {
	start := time.Now()
	dir := filepath.Join(cfg.DataDir, "shards", fmt.Sprintf("shard-%02d", shardID))
	if err := os.MkdirAll(filepath.Join(dir, "segments"), 0o755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(dir, "wal"), 0o755); err != nil {
		return nil, err
	}

	manifestPath := filepath.Join(dir, "manifest.json")
	manifest := ShardManifest{
		Version: "1.0.0",
		ShardID: shardID,
	}
	if raw, err := os.ReadFile(manifestPath); err == nil {
		if err := json.Unmarshal(raw, &manifest); err != nil {
			return nil, fmt.Errorf("ERR_MANIFEST_CORRUPT (shard %d): %w", shardID, err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	seq := domain.NewSequencer(domain.SequencerConfig{
		LateEventPolicy:   cfg.Ingest.LateEventPolicy,
		MaxWatermarkLagNS: cfg.Ingest.LateEventMaxWatermarkLagMS * int64(time.Millisecond),
	}, partitioner)

	seqStatePath := filepath.Join(dir, "sequencer_state.json")
	if raw, err := os.ReadFile(seqStatePath); err == nil {
		var snap domain.SequencerSnapshot
		if err := json.Unmarshal(raw, &snap); err != nil {
			return nil, fmt.Errorf("ERR_SEQUENCER_STATE_CORRUPT (shard %d): %w", shardID, err)
		}
		seq.Restore(snap)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	walPath := filepath.Join(dir, "wal", "wal.log")
	wal, frames, walStats, err := OpenWAL(walPath, cfg.WAL.SyncMode, cfg.WAL.TruncateCorruptTailOnRecovery)
	if err != nil {
		return nil, err
	}

	// Clean orphaned .tmp files only after manifest and WAL recovery succeed.
	cleaned := cleanOrphanTempFiles(dir)

	s := &Shard{
		shardID:     shardID,
		dir:         dir,
		cfg:         cfg,
		partitioner: partitioner,
		wal:         wal,
		walSeq:      manifest.LastWALSeq,
		manifest:    manifest,
		sequencer:   seq,
		memtable:    make([]domain.CanonicalTick, 0),
	}

	appliedFrames := 0
	appliedEvents := 0
	for _, fr := range frames {
		if fr.WALSeq <= manifest.LastWALSeq {
			continue
		}
		if fr.WALSeq > s.walSeq {
			s.walSeq = fr.WALSeq
		}
		// Replay committed ticks through the sequencer to restore exact dedup/gap state
		rawTicks := make([]domain.RawTick, len(fr.Ticks))
		for i, ct := range fr.Ticks {
			rawTicks[i] = canonicalToRaw(ct)
		}
		_ = s.sequencer.ProcessBatch(fr.BatchID, rawTicks, fr.CommittedAtNS)
		s.memtable = append(s.memtable, fr.Ticks...)
		appliedFrames++
		appliedEvents += len(fr.Ticks)
	}

	s.recovery = ShardRecoveryReport{
		ShardID:                  shardID,
		WALStats:                 walStats,
		AppliedWALFrames:         appliedFrames,
		AppliedWALEvents:         appliedEvents,
		OrphanedTempFilesCleaned: cleaned,
		SegmentsLoaded:           len(manifest.Segments),
		RecoveryDurationMS:       float64(time.Since(start).Microseconds()) / 1000.0,
	}

	return s, nil
}

func canonicalToRaw(ct domain.CanonicalTick) domain.RawTick {
	return domain.RawTick{
		SchemaVersion:  ct.SchemaVersion,
		SourceID:       ct.SourceID,
		FeedSessionID:  ct.FeedSessionID,
		InstrumentID:   ct.InstrumentID,
		Venue:          ct.Venue,
		EventType:      ct.EventType,
		Side:           ct.Side,
		EventTimeNS:    ct.EventTimeNS,
		SequenceNumber: ct.SequenceNumber,
		PriceNanos:     ct.PriceNanos,
		SizeNanos:      ct.SizeNanos,
		BidPriceNanos:  ct.BidPriceNanos,
		BidSizeNanos:   ct.BidSizeNanos,
		AskPriceNanos:  ct.AskPriceNanos,
		AskSizeNanos:   ct.AskSizeNanos,
	}
}

func cleanOrphanTempFiles(root string) int {
	cleaned := 0
	_ = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info == nil || info.IsDir() {
			return nil
		}
		if strings.HasSuffix(info.Name(), ".tmp") {
			if rmErr := os.Remove(path); rmErr == nil {
				cleaned++
			}
		}
		return nil
	})
	return cleaned
}

// CommitBatch validates/dedups/tracks gaps, durably appends accepted ticks to
// the shard WAL, appends to memtable, and flushes to columnar segments if full.
func (s *Shard) CommitBatch(batchID string, rawTicks []domain.RawTick, ingestTimeNS int64) (domain.BatchResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(rawTicks) == 0 {
		return domain.BatchResult{BatchID: batchID}, nil
	}

	// Snapshot sequencer before mutation so we can roll back cleanly if WAL write fails.
	prevSnap := s.sequencer.Snapshot()
	res := s.sequencer.ProcessBatch(batchID, rawTicks, ingestTimeNS)

	if len(res.Accepted) > 0 {
		nextSeq := s.walSeq + 1
		frame := WALFrame{
			WALSeq:        nextSeq,
			BatchID:       batchID,
			CommittedAtNS: ingestTimeNS,
			Ticks:         res.Accepted,
		}
		if _, err := s.wal.Append(frame); err != nil {
			// Rollback in-memory sequencer state so failed WAL write leaves zero side effects.
			s.sequencer.Restore(prevSnap)
			return domain.BatchResult{}, fmt.Errorf("ERR_WAL_APPEND_FAILED (shard %d): %w", s.shardID, err)
		}
		s.walSeq = nextSeq
		s.memtable = append(s.memtable, res.Accepted...)
	}

	if len(s.memtable) >= s.cfg.Storage.MemtableMaxEvents || s.wal.Size() >= s.cfg.WAL.MaxSegmentBytes {
		if err := s.flushLocked(); err != nil {
			return res, err
		}
	}
	return res, nil
}

// Flush forces an atomic flush of the memtable to columnar .tseg segments and
// checkpoints manifest + sequencer state + resets WAL.
func (s *Shard) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flushLocked()
}

func (s *Shard) flushLocked() error {
	if len(s.memtable) == 0 {
		// Still checkpoint sequencer state if needed
		return s.persistStateLocked()
	}

	byBucket := make(map[string][]domain.CanonicalTick)
	for _, t := range s.memtable {
		bucket := s.partitioner.TimeBucket(t.EventTimeNS)
		byBucket[bucket] = append(byBucket[bucket], t)
	}

	buckets := make([]string, 0, len(byBucket))
	for b := range byBucket {
		buckets = append(buckets, b)
	}
	sort.Strings(buckets)

	nowNS := time.Now().UTC().UnixNano()
	newSegments := make([]SegmentMeta, 0, len(buckets))

	for _, b := range buckets {
		ticks := byBucket[b]
		encoded, zm, err := codec.EncodeSegment(ticks, s.cfg.Storage.Compression)
		if err != nil {
			return fmt.Errorf("ERR_SEGMENT_ENCODE_FAILED: %w", err)
		}
		s.manifest.NextSegmentSeq++
		segID := fmt.Sprintf("seg-%06d", s.manifest.NextSegmentSeq)
		relPath := filepath.Join("segments", b, segID+".tseg")
		fullPath := filepath.Join(s.dir, relPath)
		if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
			return err
		}
		if err := writeFileAtomic(fullPath, encoded); err != nil {
			return fmt.Errorf("ERR_SEGMENT_WRITE_FAILED: %w", err)
		}
		newSegments = append(newSegments, SegmentMeta{
			SegmentID:     segID,
			TimeBucket:    b,
			RelPath:       relPath,
			FileSizeBytes: int64(len(encoded)),
			ZoneMap:       zm,
			CreatedAtNS:   nowNS,
		})
	}

	s.manifest.Segments = append(s.manifest.Segments, newSegments...)
	s.manifest.LastWALSeq = s.walSeq

	if err := s.persistStateLocked(); err != nil {
		return err
	}
	if err := s.wal.Reset(); err != nil {
		return fmt.Errorf("ERR_WAL_RESET_FAILED: %w", err)
	}
	s.memtable = s.memtable[:0]
	return nil
}

func (s *Shard) persistStateLocked() error {
	seqBytes, err := json.Marshal(s.sequencer.Snapshot())
	if err != nil {
		return err
	}
	if err := writeFileAtomic(filepath.Join(s.dir, "sequencer_state.json"), seqBytes); err != nil {
		return err
	}
	manBytes, err := json.MarshalIndent(s.manifest, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(filepath.Join(s.dir, "manifest.json"), manBytes)
}

func writeFileAtomic(target string, data []byte) error {
	tmp := target + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, target)
}

// QueryScan scans only segments whose time bucket and ZoneMap overlap the query
// bounds plus matching memtable entries.
func (s *Shard) QueryScan(instrumentID string, startNS, endNS int64, allowedBuckets map[string]struct{}) ([]domain.CanonicalTick, int, int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var matched []domain.CanonicalTick
	segmentsScanned := 0
	segmentsPruned := 0

	for _, seg := range s.manifest.Segments {
		if len(allowedBuckets) > 0 {
			if _, ok := allowedBuckets[seg.TimeBucket]; !ok {
				segmentsPruned++
				continue
			}
		}
		if !seg.ZoneMap.OverlapsTime(startNS, endNS) || !seg.ZoneMap.ContainsInstrument(instrumentID) {
			segmentsPruned++
			continue
		}
		raw, err := os.ReadFile(filepath.Join(s.dir, seg.RelPath))
		if err != nil {
			return nil, segmentsScanned, segmentsPruned, err
		}
		ticks, _, err := codec.DecodeSegment(raw, s.cfg.Storage.EnableChecksums)
		if err != nil {
			return nil, segmentsScanned, segmentsPruned, err
		}
		segmentsScanned++
		for _, t := range ticks {
			if instrumentID != "" && t.InstrumentID != instrumentID {
				continue
			}
			if t.EventTimeNS < startNS || t.EventTimeNS > endNS {
				continue
			}
			matched = append(matched, t)
		}
	}

	for _, t := range s.memtable {
		if instrumentID != "" && t.InstrumentID != instrumentID {
			continue
		}
		if t.EventTimeNS < startNS || t.EventTimeNS > endNS {
			continue
		}
		matched = append(matched, t)
	}

	return matched, segmentsScanned, segmentsPruned, nil
}

func (s *Shard) OpenGaps() []domain.StreamGapReport {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sequencer.OpenGaps()
}

func (s *Shard) Completeness() []domain.StreamCompleteness {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sequencer.Completeness()
}

func (s *Shard) RecoveryReport() ShardRecoveryReport {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.recovery
}

type ShardStorageStats struct {
	ShardID               uint16 `json:"shard_id"`
	SegmentCount          int    `json:"segment_count"`
	SegmentBytes          int64  `json:"segment_bytes"`
	UncompressedColBytes  int64  `json:"uncompressed_col_bytes"`
	WALBytes              int64  `json:"wal_bytes"`
	MemtableEvents        int    `json:"memtable_events"`
	PersistedSegmentRows  uint64 `json:"persisted_segment_rows"`
}

func (s *Shard) Stats() ShardStorageStats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	st := ShardStorageStats{
		ShardID:        s.shardID,
		SegmentCount:   len(s.manifest.Segments),
		WALBytes:       s.wal.Size(),
		MemtableEvents: len(s.memtable),
	}
	for _, seg := range s.manifest.Segments {
		st.SegmentBytes += seg.FileSizeBytes
		st.UncompressedColBytes += int64(seg.ZoneMap.UncompressedBytes)
		st.PersistedSegmentRows += uint64(seg.ZoneMap.RowCount)
	}
	return st
}

func (s *Shard) WALPath() string {
	return s.wal.Path()
}

func (s *Shard) Dir() string {
	return s.dir
}

func (s *Shard) Close(flush bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if flush {
		if err := s.flushLocked(); err != nil {
			_ = s.wal.Close()
			return err
		}
	}
	return s.wal.Close()
}
