package domain

import "sort"

// ReferenceOracle is a minimal, naive in-memory reference replay implementation.
// It uses no sharding, no WAL, no compression, and no zone-map pruning, serving
// as the independent correctness baseline against which the storage and query
// engines are continuously verified.
type ReferenceOracle struct {
	seq   *Sequencer
	ticks []CanonicalTick
}

func NewReferenceOracle(cfg SequencerConfig, partitioner Partitioner) *ReferenceOracle {
	return &ReferenceOracle{
		seq:   NewSequencer(cfg, partitioner),
		ticks: make([]CanonicalTick, 0),
	}
}

func (o *ReferenceOracle) Ingest(batchID string, raw []RawTick, ingestTimeNS int64) BatchResult {
	res := o.seq.ProcessBatch(batchID, raw, ingestTimeNS)
	o.ticks = append(o.ticks, res.Accepted...)
	return res
}

func (o *ReferenceOracle) QueryRange(instrumentID string, startNS, endNS int64, limit int) []CanonicalTick {
	var matched []CanonicalTick
	for _, t := range o.ticks {
		if instrumentID != "" && t.InstrumentID != instrumentID {
			continue
		}
		if t.EventTimeNS < startNS || t.EventTimeNS > endNS {
			continue
		}
		matched = append(matched, t)
	}
	sort.Slice(matched, func(i, j int) bool {
		return CompareCanonicalOrder(matched[i], matched[j]) < 0
	})
	if limit > 0 && len(matched) > limit {
		matched = matched[:limit]
	}
	return matched
}

func (o *ReferenceOracle) AllOrdered() []CanonicalTick {
	out := make([]CanonicalTick, len(o.ticks))
	copy(out, o.ticks)
	sort.Slice(out, func(i, j int) bool {
		return CompareCanonicalOrder(out[i], out[j]) < 0
	})
	return out
}

func (o *ReferenceOracle) OpenGaps() []StreamGapReport {
	return o.seq.OpenGaps()
}

func (o *ReferenceOracle) Completeness() []StreamCompleteness {
	return o.seq.Completeness()
}
