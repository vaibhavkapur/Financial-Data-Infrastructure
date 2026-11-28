package domain

import (
	"fmt"
	"sort"
)

// GapInterval represents an inclusive missing sequence range [StartSeq, EndSeq].
type GapInterval struct {
	StartSeq     uint64 `json:"start_seq"`
	EndSeq       uint64 `json:"end_seq"`
	DetectedAtNS int64  `json:"detected_at_ns"`
}

func (g GapInterval) Count() uint64 {
	if g.EndSeq < g.StartSeq {
		return 0
	}
	return g.EndSeq - g.StartSeq + 1
}

// StreamGapReport exposes open gap details for a (source_id, instrument_id) stream.
type StreamGapReport struct {
	SourceID     string `json:"source_id"`
	InstrumentID string `json:"instrument_id"`
	StartSeq     uint64 `json:"start_seq"`
	EndSeq       uint64 `json:"end_seq"`
	MissingCount uint64 `json:"missing_count"`
	DetectedAtNS int64  `json:"detected_at_ns"`
}

// StreamCompleteness reports exact sequence completeness per stream.
type StreamCompleteness struct {
	SourceID          string  `json:"source_id"`
	InstrumentID      string  `json:"instrument_id"`
	MinSeq            uint64  `json:"min_seq"`
	MaxSeq            uint64  `json:"max_seq"`
	ReceivedUnique    uint64  `json:"received_unique"`
	ExpectedSpan      uint64  `json:"expected_span"`
	MissingSequences  uint64  `json:"missing_sequences"`
	OpenGapIntervals  int     `json:"open_gap_intervals"`
	ResolvedGapsCount uint64  `json:"resolved_gaps_count"`
	DuplicateCount    uint64  `json:"duplicate_count"`
	LateAcceptedCount uint64  `json:"late_accepted_count"`
	LateRejectedCount uint64  `json:"late_rejected_count"`
	CompletenessRatio float64 `json:"completeness_ratio"`
	MaxEventTimeNS    int64   `json:"max_event_time_ns"`
}

// StreamState captures the full serializable state of one (source_id, instrument_id) stream.
type StreamState struct {
	SourceID          string            `json:"source_id"`
	InstrumentID      string            `json:"instrument_id"`
	Initialized       bool              `json:"initialized"`
	MinSeq            uint64            `json:"min_seq"`
	HighWaterSeq      uint64            `json:"high_water_seq"`
	MaxEventTimeNS    int64             `json:"max_event_time_ns"`
	ReceivedUnique    uint64            `json:"received_unique"`
	DuplicateCount    uint64            `json:"duplicate_count"`
	LateAcceptedCount uint64            `json:"late_accepted_count"`
	LateRejectedCount uint64            `json:"late_rejected_count"`
	ResolvedGapsCount uint64            `json:"resolved_gaps_count"`
	OpenGaps          []GapInterval     `json:"open_gaps"`
	SeenDigests       map[uint64]uint64 `json:"seen_digests"` // sequence_number -> payload digest
}

type SequencerConfig struct {
	LateEventPolicy     string // "accept_and_flag" | "reject_too_late"
	MaxWatermarkLagNS   int64
	MaxSeenDigestWindow int // 0 = unlimited (exact for prototype/tests)
}

type Disposition string

const (
	DispositionAccepted       Disposition = "accepted"
	DispositionAcceptedLate   Disposition = "accepted_late"
	DispositionDuplicate      Disposition = "duplicate"
	DispositionRejected       Disposition = "rejected"
	DispositionRejectedLate   Disposition = "rejected_too_late"
	DispositionRejectedConflict Disposition = "rejected_conflict"
)

type ProcessOutcome struct {
	Disposition Disposition
	Tick        CanonicalTick
	Err         error
}

// BatchResult summarizes domain state transitions for a single batch.
type BatchResult struct {
	BatchID           string          `json:"batch_id"`
	TotalInput        int             `json:"total_input"`
	Accepted          []CanonicalTick `json:"-"`
	AcceptedCount     int             `json:"accepted_count"`
	LateAcceptedCount int             `json:"late_accepted_count"`
	DuplicateCount    int             `json:"duplicate_count"`
	RejectedCount     int             `json:"rejected_count"`
	Errors            []BatchError    `json:"errors,omitempty"`
}

type BatchError struct {
	Index int    `json:"index"`
	Code  string `json:"code"`
	Error string `json:"error"`
}

// SequencerSnapshot is persisted alongside WAL/checkpoints to guarantee
// idempotency and gap tracking survive restarts.
type SequencerSnapshot struct {
	Streams map[string]*StreamState `json:"streams"`
}

// Sequencer is a pure, deterministic, I/O-free state machine enforcing schema
// validation, idempotency, conflict detection, gap tracking, and late-event handling.
type Sequencer struct {
	cfg         SequencerConfig
	partitioner Partitioner
	streams     map[string]*StreamState
}

func NewSequencer(cfg SequencerConfig, partitioner Partitioner) *Sequencer {
	if cfg.LateEventPolicy == "" {
		cfg.LateEventPolicy = "accept_and_flag"
	}
	return &Sequencer{
		cfg:         cfg,
		partitioner: partitioner,
		streams:     make(map[string]*StreamState),
	}
}

func (s *Sequencer) Snapshot() SequencerSnapshot {
	out := SequencerSnapshot{
		Streams: make(map[string]*StreamState, len(s.streams)),
	}
	for k, st := range s.streams {
		cp := *st
		cp.OpenGaps = make([]GapInterval, len(st.OpenGaps))
		copy(cp.OpenGaps, st.OpenGaps)
		cp.SeenDigests = make(map[uint64]uint64, len(st.SeenDigests))
		for seq, dig := range st.SeenDigests {
			cp.SeenDigests[seq] = dig
		}
		out.Streams[k] = &cp
	}
	return out
}

func (s *Sequencer) Restore(snap SequencerSnapshot) {
	s.streams = make(map[string]*StreamState, len(snap.Streams))
	for k, st := range snap.Streams {
		cp := *st
		cp.OpenGaps = make([]GapInterval, len(st.OpenGaps))
		copy(cp.OpenGaps, st.OpenGaps)
		if cp.SeenDigests == nil {
			cp.SeenDigests = make(map[uint64]uint64)
		} else {
			digs := make(map[uint64]uint64, len(st.SeenDigests))
			for seq, dig := range st.SeenDigests {
				digs[seq] = dig
			}
			cp.SeenDigests = digs
		}
		s.streams[k] = &cp
	}
}

// ProcessBatch validates and transitions state for every event in a batch.
// Individual invalid events are reported in BatchResult.Errors without silently
// dropping valid events or corrupting sequence state.
func (s *Sequencer) ProcessBatch(batchID string, rawTicks []RawTick, ingestTimeNS int64) BatchResult {
	res := BatchResult{
		BatchID:    batchID,
		TotalInput: len(rawTicks),
		Accepted:   make([]CanonicalTick, 0, len(rawTicks)),
	}
	for i, raw := range rawTicks {
		outcome := s.ProcessOne(batchID, raw, ingestTimeNS)
		switch outcome.Disposition {
		case DispositionAccepted:
			res.Accepted = append(res.Accepted, outcome.Tick)
			res.AcceptedCount++
		case DispositionAcceptedLate:
			res.Accepted = append(res.Accepted, outcome.Tick)
			res.AcceptedCount++
			res.LateAcceptedCount++
		case DispositionDuplicate:
			res.DuplicateCount++
		case DispositionRejected, DispositionRejectedLate, DispositionRejectedConflict:
			res.RejectedCount++
			code := "ERR_UNKNOWN"
			if de, ok := outcome.Err.(*DomainError); ok {
				code = de.Code
			}
			res.Errors = append(res.Errors, BatchError{
				Index: i,
				Code:  code,
				Error: outcome.Err.Error(),
			})
		}
	}
	return res
}

func (s *Sequencer) ProcessOne(batchID string, raw RawTick, ingestTimeNS int64) ProcessOutcome {
	if err := ValidateRawTick(raw); err != nil {
		return ProcessOutcome{Disposition: DispositionRejected, Err: err}
	}

	key := StreamKey{SourceID: raw.SourceID, InstrumentID: raw.InstrumentID}.String()
	st, ok := s.streams[key]
	if !ok {
		st = &StreamState{
			SourceID:     raw.SourceID,
			InstrumentID: raw.InstrumentID,
			SeenDigests:  make(map[uint64]uint64),
		}
		s.streams[key] = st
	}

	digest := raw.PayloadDigest()
	seq := raw.SequenceNumber

	// 1. Check idempotency / duplicate sequence
	if prevDigest, seen := st.SeenDigests[seq]; seen {
		if prevDigest == digest {
			st.DuplicateCount++
			return ProcessOutcome{Disposition: DispositionDuplicate}
		}
		return ProcessOutcome{
			Disposition: DispositionRejectedConflict,
			Err: &DomainError{
				Code:  "ERR_DUPLICATE_SEQUENCE_CONFLICT",
				Field: "sequence_number",
				Message: fmt.Sprintf("sequence %d for %s already seen with different payload digest (existing=%x, incoming=%x)",
					seq, key, prevDigest, digest),
			},
		}
	}

	// 2. Determine whether this is a late event (sequence-late or watermark-late)
	isSeqLate := st.Initialized && seq < st.HighWaterSeq
	isTimeLate := st.Initialized && raw.EventTimeNS < st.MaxEventTimeNS
	exceedsLag := st.Initialized && s.cfg.MaxWatermarkLagNS > 0 && (st.MaxEventTimeNS-raw.EventTimeNS) > s.cfg.MaxWatermarkLagNS

	if exceedsLag && s.cfg.LateEventPolicy == "reject_too_late" {
		st.LateRejectedCount++
		return ProcessOutcome{
			Disposition: DispositionRejectedLate,
			Err: &DomainError{
				Code:  "ERR_EVENT_TOO_LATE",
				Field: "event_time_ns",
				Message: fmt.Sprintf("event_time_ns %d lags stream watermark %d by %dns (max allowed: %dns)",
					raw.EventTimeNS, st.MaxEventTimeNS, st.MaxEventTimeNS-raw.EventTimeNS, s.cfg.MaxWatermarkLagNS),
			},
		}
	}

	lateArrival := isSeqLate || isTimeLate

	// 3. Update sequence watermark and gap intervals
	if !st.Initialized {
		st.Initialized = true
		st.MinSeq = seq
		st.HighWaterSeq = seq
		st.MaxEventTimeNS = raw.EventTimeNS
		if seq > 1 {
			// If first observed sequence is > 1, track leading gap [1, seq-1]
			st.OpenGaps = append(st.OpenGaps, GapInterval{
				StartSeq:     1,
				EndSeq:       seq - 1,
				DetectedAtNS: ingestTimeNS,
			})
			st.MinSeq = 1
		}
	} else {
		if seq < st.MinSeq {
			st.MinSeq = seq
		}
		if seq > st.HighWaterSeq {
			if seq > st.HighWaterSeq+1 {
				st.OpenGaps = append(st.OpenGaps, GapInterval{
					StartSeq:     st.HighWaterSeq + 1,
					EndSeq:       seq - 1,
					DetectedAtNS: ingestTimeNS,
				})
			}
			st.HighWaterSeq = seq
		} else if seq < st.HighWaterSeq {
			// Attempt to fill or split an existing open gap
			filled := fillGap(st, seq)
			if filled {
				st.ResolvedGapsCount++
			} else {
				// Sequence was below high water and not in an open gap -> already seen before pruning window
				st.DuplicateCount++
				return ProcessOutcome{Disposition: DispositionDuplicate}
			}
		}
		if raw.EventTimeNS > st.MaxEventTimeNS {
			st.MaxEventTimeNS = raw.EventTimeNS
		}
	}

	st.SeenDigests[seq] = digest
	st.ReceivedUnique++
	if lateArrival {
		st.LateAcceptedCount++
	}

	shardID := s.partitioner.ShardForInstrument(raw.InstrumentID)
	partKey := s.partitioner.PartitionKey(raw.EventTimeNS, shardID)

	ct := CanonicalTick{
		SchemaVersion:  raw.SchemaVersion,
		SourceID:       raw.SourceID,
		FeedSessionID:  raw.FeedSessionID,
		BatchID:        batchID,
		InstrumentID:   raw.InstrumentID,
		Venue:          raw.Venue,
		EventType:      raw.EventType,
		Side:           raw.Side,
		EventTimeNS:    raw.EventTimeNS,
		IngestTimeNS:   ingestTimeNS,
		SequenceNumber: raw.SequenceNumber,
		PriceNanos:     raw.PriceNanos,
		SizeNanos:      raw.SizeNanos,
		BidPriceNanos:  raw.BidPriceNanos,
		BidSizeNanos:   raw.BidSizeNanos,
		AskPriceNanos:  raw.AskPriceNanos,
		AskSizeNanos:   raw.AskSizeNanos,
		LateArrival:    lateArrival,
		ShardID:        shardID,
		PartitionKey:   partKey,
	}

	disp := DispositionAccepted
	if lateArrival {
		disp = DispositionAcceptedLate
	}
	return ProcessOutcome{
		Disposition: disp,
		Tick:        ct,
	}
}

func fillGap(st *StreamState, seq uint64) bool {
	for i, g := range st.OpenGaps {
		if seq < g.StartSeq || seq > g.EndSeq {
			continue
		}
		if g.StartSeq == g.EndSeq {
			// Entire single-sequence gap closed
			st.OpenGaps = append(st.OpenGaps[:i], st.OpenGaps[i+1:]...)
		} else if seq == g.StartSeq {
			st.OpenGaps[i].StartSeq = seq + 1
		} else if seq == g.EndSeq {
			st.OpenGaps[i].EndSeq = seq - 1
		} else {
			// Split gap [StartSeq, EndSeq] into [StartSeq, seq-1] and [seq+1, EndSeq]
			left := GapInterval{StartSeq: g.StartSeq, EndSeq: seq - 1, DetectedAtNS: g.DetectedAtNS}
			right := GapInterval{StartSeq: seq + 1, EndSeq: g.EndSeq, DetectedAtNS: g.DetectedAtNS}
			tail := append([]GapInterval{right}, st.OpenGaps[i+1:]...)
			st.OpenGaps = append(st.OpenGaps[:i], left)
			st.OpenGaps = append(st.OpenGaps, tail...)
		}
		return true
	}
	return false
}

// OpenGaps returns all currently unresolved sequence gaps sorted deterministically.
func (s *Sequencer) OpenGaps() []StreamGapReport {
	var reports []StreamGapReport
	for _, st := range s.streams {
		for _, g := range st.OpenGaps {
			reports = append(reports, StreamGapReport{
				SourceID:     st.SourceID,
				InstrumentID: st.InstrumentID,
				StartSeq:     g.StartSeq,
				EndSeq:       g.EndSeq,
				MissingCount: g.Count(),
				DetectedAtNS: g.DetectedAtNS,
			})
		}
	}
	sort.Slice(reports, func(i, j int) bool {
		if reports[i].SourceID != reports[j].SourceID {
			return reports[i].SourceID < reports[j].SourceID
		}
		if reports[i].InstrumentID != reports[j].InstrumentID {
			return reports[i].InstrumentID < reports[j].InstrumentID
		}
		return reports[i].StartSeq < reports[j].StartSeq
	})
	return reports
}

// Completeness returns per-stream completeness metrics sorted by (source_id, instrument_id).
func (s *Sequencer) Completeness() []StreamCompleteness {
	out := make([]StreamCompleteness, 0, len(s.streams))
	for _, st := range s.streams {
		if !st.Initialized {
			continue
		}
		var missing uint64
		for _, g := range st.OpenGaps {
			missing += g.Count()
		}
		expected := st.HighWaterSeq - st.MinSeq + 1
		ratio := 1.0
		if expected > 0 {
			ratio = float64(st.ReceivedUnique) / float64(expected)
		}
		out = append(out, StreamCompleteness{
			SourceID:          st.SourceID,
			InstrumentID:      st.InstrumentID,
			MinSeq:            st.MinSeq,
			MaxSeq:            st.HighWaterSeq,
			ReceivedUnique:    st.ReceivedUnique,
			ExpectedSpan:      expected,
			MissingSequences:  missing,
			OpenGapIntervals:  len(st.OpenGaps),
			ResolvedGapsCount: st.ResolvedGapsCount,
			DuplicateCount:    st.DuplicateCount,
			LateAcceptedCount: st.LateAcceptedCount,
			LateRejectedCount: st.LateRejectedCount,
			CompletenessRatio: ratio,
			MaxEventTimeNS:    st.MaxEventTimeNS,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].SourceID != out[j].SourceID {
			return out[i].SourceID < out[j].SourceID
		}
		return out[i].InstrumentID < out[j].InstrumentID
	})
	return out
}
