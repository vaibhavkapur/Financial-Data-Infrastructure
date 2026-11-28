package tests

import (
	"bytes"
	"strings"
	"testing"

	"distributed-tick-data-database/src/codec"
	"distributed-tick-data-database/src/domain"
	"distributed-tick-data-database/src/ingest"
)

func buildCanonicalSample(n int) []domain.CanonicalTick {
	part := domain.NewPartitioner(4, "hour")
	seq := domain.NewSequencer(domain.SequencerConfig{LateEventPolicy: "accept_and_flag"}, part)
	raw := ingest.GenerateSyntheticStream(ingest.SyntheticOptions{
		Seed:            42,
		Instruments:     []string{"AAPL", "MSFT", "BTC-USD"},
		SourceID:        "codec-feed",
		FeedSessionID:   "codec-sess",
		StartTimeNS:     1764320400000000000,
		Count:           n,
		TickIntervalNS:  500_000,
		LateReorderRate: 0.05,
	})
	res := seq.ProcessBatch("codec-batch", raw, 1764320499000000000)
	return res.Accepted
}

func TestColumnarSegmentCodecRoundTripAndCorruptionDetection(t *testing.T) {
	ticks := buildCanonicalSample(1200)

	for _, comp := range []string{"delta_dict_gzip", "delta_dict_raw"} {
		data, zm, err := codec.EncodeSegment(ticks, comp)
		if err != nil {
			t.Fatalf("encode (%s): %v", comp, err)
		}
		if int(zm.RowCount) != len(ticks) {
			t.Fatalf("row count: got %d want %d", zm.RowCount, len(ticks))
		}

		// ZoneMap-only read must succeed without decompressing payload
		zmOnly, err := codec.ReadZoneMap(data)
		if err != nil {
			t.Fatalf("read zonemap (%s): %v", comp, err)
		}
		if zmOnly.RowCount != zm.RowCount || zmOnly.MinEventTimeNS != zm.MinEventTimeNS || zmOnly.MaxEventTimeNS != zm.MaxEventTimeNS {
			t.Fatalf("zonemap mismatch: %+v vs %+v", zmOnly, zm)
		}

		decoded, _, err := codec.DecodeSegment(data, true)
		if err != nil {
			t.Fatalf("decode (%s): %v", comp, err)
		}
		if len(decoded) != len(ticks) {
			t.Fatalf("decoded len: got %d want %d", len(decoded), len(ticks))
		}

		// Verify bit-for-bit corruption detection on payload
		corrupt := make([]byte, len(data))
		copy(corrupt, data)
		corrupt[len(corrupt)-1] ^= 0xFF
		if _, _, err := codec.DecodeSegment(corrupt, true); err == nil || !strings.Contains(err.Error(), "CRC32") {
			t.Fatalf("expected CRC32 error on corrupt payload (%s), got %v", comp, err)
		}
	}
}

func TestParquetCodecRoundTripAndMagicValidation(t *testing.T) {
	ticks := buildCanonicalSample(750)

	var buf bytes.Buffer
	n, err := codec.WriteParquet(&buf, ticks)
	if err != nil {
		t.Fatalf("WriteParquet: %v", err)
	}
	raw := buf.Bytes()
	if int64(len(raw)) != n {
		t.Fatalf("bytes written mismatch: %d vs %d", len(raw), n)
	}
	if string(raw[:4]) != "PAR1" || string(raw[len(raw)-4:]) != "PAR1" {
		t.Fatalf("invalid PAR1 magic bytes")
	}

	decoded, err := codec.ReadParquet(raw)
	if err != nil {
		t.Fatalf("ReadParquet: %v", err)
	}
	if len(decoded) != len(ticks) {
		t.Fatalf("decoded row count: got %d want %d", len(decoded), len(ticks))
	}
	if domain.LogicalDigest(decoded) != domain.LogicalDigest(ticks) {
		t.Fatalf("parquet round-trip logical digest mismatch")
	}
	for i := range ticks {
		if decoded[i] != ticks[i] {
			t.Fatalf("row %d mismatch:\ngot:  %+v\nwant: %+v", i, decoded[i], ticks[i])
		}
	}

	// Empty parquet file must also round-trip cleanly
	var emptyBuf bytes.Buffer
	if _, err := codec.WriteParquet(&emptyBuf, nil); err != nil {
		t.Fatalf("WriteParquet empty: %v", err)
	}
	emptyDecoded, err := codec.ReadParquet(emptyBuf.Bytes())
	if err != nil || len(emptyDecoded) != 0 {
		t.Fatalf("ReadParquet empty: rows=%d err=%v", len(emptyDecoded), err)
	}

	// Corrupted trailing magic must fail cleanly
	bad := append([]byte(nil), raw...)
	bad[len(bad)-1] = 'X'
	if _, err := codec.ReadParquet(bad); err == nil {
		t.Fatalf("expected error on corrupt parquet magic")
	}
}
