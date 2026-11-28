package tests

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"

	"distributed-tick-data-database/src/domain"
	"distributed-tick-data-database/src/ingest"
)

type expectedRefFixture struct {
	TotalInputEvents    int      `json:"total_input_events"`
	AcceptedUnique      int      `json:"accepted_unique_events"`
	DuplicateEvents     int      `json:"duplicate_events"`
	LateEventsAccepted  int      `json:"late_events_accepted"`
	ExpectedAAPLSeqs    []uint64 `json:"expected_aapl_ordered_sequences"`
	ExpectedMSFTSeqs    []uint64 `json:"expected_msft_ordered_sequences"`
	ExpectedOpenGaps    []struct {
		SourceID     string `json:"source_id"`
		InstrumentID string `json:"instrument_id"`
		StartSeq     uint64 `json:"start_seq"`
		EndSeq       uint64 `json:"end_seq"`
	} `json:"expected_open_gaps"`
	ExpectedCompleteness map[string]struct {
		ReceivedUnique    uint64  `json:"received_unique"`
		ExpectedSpan      uint64  `json:"expected_span"`
		MissingSequences  uint64  `json:"missing_sequences"`
		CompletenessRatio float64 `json:"completeness_ratio"`
	} `json:"expected_completeness"`
}

func TestReferenceFixtureMatchesHandCalculatedOracle(t *testing.T) {
	ticks, parseErrs, err := ingest.ReadJSONLFile(filepath.Join("..", "data", "fixtures", "valid_reference_ticks.jsonl"))
	if err != nil {
		t.Fatalf("read reference fixture: %v", err)
	}
	if len(parseErrs) != 0 {
		t.Fatalf("unexpected parse errors: %v", parseErrs)
	}

	expRaw, err := os.ReadFile(filepath.Join("..", "data", "fixtures", "expected_reference_result.json"))
	if err != nil {
		t.Fatalf("read expected result: %v", err)
	}
	var exp expectedRefFixture
	if err := json.Unmarshal(expRaw, &exp); err != nil {
		t.Fatalf("unmarshal expected result: %v", err)
	}

	part := domain.NewPartitioner(4, "hour")
	oracle := domain.NewReferenceOracle(domain.SequencerConfig{
		LateEventPolicy:   "accept_and_flag",
		MaxWatermarkLagNS: 3600_000_000_000,
	}, part)
	res := oracle.Ingest("ref-01", ticks, 1764324001000000000)

	if res.TotalInput != exp.TotalInputEvents {
		t.Fatalf("total input: got %d want %d", res.TotalInput, exp.TotalInputEvents)
	}
	if res.AcceptedCount != exp.AcceptedUnique {
		t.Fatalf("accepted: got %d want %d", res.AcceptedCount, exp.AcceptedUnique)
	}
	if res.DuplicateCount != exp.DuplicateEvents {
		t.Fatalf("duplicates: got %d want %d", res.DuplicateCount, exp.DuplicateEvents)
	}
	if res.LateAcceptedCount != exp.LateEventsAccepted {
		t.Fatalf("late accepted: got %d want %d", res.LateAcceptedCount, exp.LateEventsAccepted)
	}

	// Verify AAPL and MSFT replay sequence order
	aaplRows := oracle.QueryRange("AAPL", 0, 1764325000000000000, 100)
	if len(aaplRows) != len(exp.ExpectedAAPLSeqs) {
		t.Fatalf("AAPL rows: got %d want %d", len(aaplRows), len(exp.ExpectedAAPLSeqs))
	}
	for i, wantSeq := range exp.ExpectedAAPLSeqs {
		if aaplRows[i].SequenceNumber != wantSeq {
			t.Fatalf("AAPL[%d] seq: got %d want %d", i, aaplRows[i].SequenceNumber, wantSeq)
		}
	}

	msftRows := oracle.QueryRange("MSFT", 0, 1764325000000000000, 100)
	if len(msftRows) != len(exp.ExpectedMSFTSeqs) {
		t.Fatalf("MSFT rows: got %d want %d", len(msftRows), len(exp.ExpectedMSFTSeqs))
	}
	for i, wantSeq := range exp.ExpectedMSFTSeqs {
		if msftRows[i].SequenceNumber != wantSeq {
			t.Fatalf("MSFT[%d] seq: got %d want %d", i, msftRows[i].SequenceNumber, wantSeq)
		}
	}

	// Verify exact open gaps
	gaps := oracle.OpenGaps()
	if len(gaps) != len(exp.ExpectedOpenGaps) {
		t.Fatalf("open gaps count: got %d want %d (%+v)", len(gaps), len(exp.ExpectedOpenGaps), gaps)
	}
	if gaps[0].InstrumentID != exp.ExpectedOpenGaps[0].InstrumentID ||
		gaps[0].StartSeq != exp.ExpectedOpenGaps[0].StartSeq ||
		gaps[0].EndSeq != exp.ExpectedOpenGaps[0].EndSeq {
		t.Fatalf("unexpected open gap: got %+v want %+v", gaps[0], exp.ExpectedOpenGaps[0])
	}

	// Verify completeness ratios
	for _, c := range oracle.Completeness() {
		want, ok := exp.ExpectedCompleteness[c.InstrumentID]
		if !ok {
			t.Fatalf("unexpected instrument in completeness: %s", c.InstrumentID)
		}
		if c.ReceivedUnique != want.ReceivedUnique || c.ExpectedSpan != want.ExpectedSpan || c.MissingSequences != want.MissingSequences {
			t.Fatalf("%s completeness mismatch: got %+v want %+v", c.InstrumentID, c, want)
		}
		if math.Abs(c.CompletenessRatio-want.CompletenessRatio) > 1e-9 {
			t.Fatalf("%s ratio: got %v want %v", c.InstrumentID, c.CompletenessRatio, want.CompletenessRatio)
		}
	}
}

func TestDuplicateSequenceAndConflictAndLateEventPolicies(t *testing.T) {
	part := domain.NewPartitioner(4, "hour")
	baseTick := func(seq uint64, ts int64, price int64) domain.RawTick {
		return domain.RawTick{
			SchemaVersion:  domain.SchemaVersionV1,
			SourceID:       "feed-x",
			FeedSessionID:  "s-1",
			InstrumentID:   "NVDA",
			Venue:          "XNAS",
			EventType:      domain.EventTypeTrade,
			Side:           domain.SideBuy,
			EventTimeNS:    ts,
			SequenceNumber: seq,
			PriceNanos:     price,
			SizeNanos:      10_000_000_000,
			BidPriceNanos:  price - 10_000_000,
			BidSizeNanos:   100_000_000_000,
			AskPriceNanos:  price + 10_000_000,
			AskSizeNanos:   100_000_000_000,
		}
	}

	t0 := int64(1764320400_000_000_000)

	// Case 1: accept_and_flag policy with gap split, duplicate, and conflict
	seq := domain.NewSequencer(domain.SequencerConfig{
		LateEventPolicy:   "accept_and_flag",
		MaxWatermarkLagNS: 5_000_000_000, // 5s
	}, part)

	// Ingest seq 1, then jump to seq 5 -> open gap [2, 4]
	r1 := seq.ProcessBatch("b1", []domain.RawTick{
		baseTick(1, t0, 138_000_000_000),
		baseTick(5, t0+10_000_000_000, 138_500_000_000),
	}, t0+11_000_000_000)
	if r1.AcceptedCount != 2 || len(seq.OpenGaps()) != 1 {
		t.Fatalf("expected 2 accepted and 1 gap, got accepted=%d gaps=%+v", r1.AcceptedCount, seq.OpenGaps())
	}
	if seq.OpenGaps()[0].StartSeq != 2 || seq.OpenGaps()[0].EndSeq != 4 {
		t.Fatalf("expected gap [2,4], got %+v", seq.OpenGaps()[0])
	}

	// Late event seq 3 splits gap [2,4] into [2,2] and [4,4]
	oLate := seq.ProcessOne("b2", baseTick(3, t0+2_000_000_000, 138_200_000_000), t0+12_000_000_000)
	if oLate.Disposition != domain.DispositionAcceptedLate || !oLate.Tick.LateArrival {
		t.Fatalf("expected late accepted tick, got %+v", oLate)
	}
	gaps := seq.OpenGaps()
	if len(gaps) != 2 || gaps[0].StartSeq != 2 || gaps[0].EndSeq != 2 || gaps[1].StartSeq != 4 || gaps[1].EndSeq != 4 {
		t.Fatalf("expected split gaps [2,2] and [4,4], got %+v", gaps)
	}

	// Exact duplicate of seq 3 is idempotently deduplicated
	oDup := seq.ProcessOne("b3", baseTick(3, t0+2_000_000_000, 138_200_000_000), t0+13_000_000_000)
	if oDup.Disposition != domain.DispositionDuplicate {
		t.Fatalf("expected duplicate disposition, got %s", oDup.Disposition)
	}

	// Conflicting payload on same seq 3 is explicitly rejected with ERR_DUPLICATE_SEQUENCE_CONFLICT
	oConflict := seq.ProcessOne("b4", baseTick(3, t0+2_000_000_000, 999_000_000_000), t0+14_000_000_000)
	if oConflict.Disposition != domain.DispositionRejectedConflict {
		t.Fatalf("expected conflict rejection, got %s", oConflict.Disposition)
	}
	de, ok := oConflict.Err.(*domain.DomainError)
	if !ok || de.Code != "ERR_DUPLICATE_SEQUENCE_CONFLICT" {
		t.Fatalf("expected ERR_DUPLICATE_SEQUENCE_CONFLICT, got %v", oConflict.Err)
	}

	// Case 2: reject_too_late policy rejects events beyond watermark lag
	seqStrict := domain.NewSequencer(domain.SequencerConfig{
		LateEventPolicy:   "reject_too_late",
		MaxWatermarkLagNS: 5_000_000_000, // 5s
	}, part)
	_ = seqStrict.ProcessOne("s1", baseTick(1, t0, 138_000_000_000), t0)
	_ = seqStrict.ProcessOne("s2", baseTick(4, t0+10_000_000_000, 138_400_000_000), t0+10_000_000_000)
	// Lag = 9s > 5s -> must reject
	oTooLate := seqStrict.ProcessOne("s3", baseTick(2, t0+1_000_000_000, 138_100_000_000), t0+11_000_000_000)
	if oTooLate.Disposition != domain.DispositionRejectedLate {
		t.Fatalf("expected DispositionRejectedLate, got %s", oTooLate.Disposition)
	}
	de2, ok := oTooLate.Err.(*domain.DomainError)
	if !ok || de2.Code != "ERR_EVENT_TOO_LATE" {
		t.Fatalf("expected ERR_EVENT_TOO_LATE, got %v", oTooLate.Err)
	}
}

func TestInvalidAndBoundaryFixtures(t *testing.T) {
	// Every line in invalid_ticks.jsonl must be rejected with a structured DomainError
	invTicks, parseErrs, err := ingest.ReadJSONLFile(filepath.Join("..", "data", "fixtures", "invalid_ticks.jsonl"))
	if err != nil {
		t.Fatalf("read invalid fixture: %v", err)
	}
	if len(parseErrs) != 0 {
		t.Fatalf("unexpected JSON parse errors in invalid_ticks.jsonl: %v", parseErrs)
	}
	if len(invTicks) != 10 {
		t.Fatalf("expected 10 invalid ticks, got %d", len(invTicks))
	}
	for i, tk := range invTicks {
		vErr := domain.ValidateRawTick(tk)
		if vErr == nil {
			t.Fatalf("invalid fixture line %d unexpectedly passed validation: %+v", i+1, tk)
		}
		if _, ok := vErr.(*domain.DomainError); !ok {
			t.Fatalf("line %d returned non-DomainError: %T %v", i+1, vErr, vErr)
		}
	}

	// Every line in boundary_ticks.jsonl must be accepted and cross UTC hour boundary cleanly
	bndTicks, parseErrs, err := ingest.ReadJSONLFile(filepath.Join("..", "data", "fixtures", "boundary_ticks.jsonl"))
	if err != nil {
		t.Fatalf("read boundary fixture: %v", err)
	}
	if len(parseErrs) != 0 || len(bndTicks) != 3 {
		t.Fatalf("expected 3 valid boundary ticks, got %d (parseErrs=%v)", len(bndTicks), parseErrs)
	}
	part := domain.NewPartitioner(4, "hour")
	seq := domain.NewSequencer(domain.SequencerConfig{LateEventPolicy: "accept_and_flag"}, part)
	res := seq.ProcessBatch("bnd", bndTicks, 1764324001000000000)
	if res.AcceptedCount != 3 || res.RejectedCount != 0 {
		t.Fatalf("boundary ticks failed: %+v", res)
	}
	if res.Accepted[0].PartitionKey == res.Accepted[1].PartitionKey {
		t.Fatalf("expected hour partition boundary split between tick 0 (%s) and tick 1 (%s)",
			res.Accepted[0].PartitionKey, res.Accepted[1].PartitionKey)
	}
}
