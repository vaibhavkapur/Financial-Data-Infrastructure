package tests

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"distributed-tick-data-database/src/codec"
	"distributed-tick-data-database/src/config"
	"distributed-tick-data-database/src/domain"
	"distributed-tick-data-database/src/ingest"
	"distributed-tick-data-database/src/query"
	"distributed-tick-data-database/src/storage"
)

func TestReproducibilityDeterministicAcrossCleanRunsAndFormats(t *testing.T) {
	runOnce := func(dir string) (string, string, string, string) {
		cfg := config.Default()
		cfg.DataDir = dir
		cfg.Storage.MemtableMaxEvents = 300

		raw := ingest.GenerateSyntheticStream(ingest.SyntheticOptions{
			Seed:            9999,
			Instruments:     []string{"AAPL", "MSFT", "ESZ6", "BTC-USD"},
			SourceID:        "repro-feed",
			FeedSessionID:   "repro-s1",
			StartTimeNS:     1764320400000000000,
			Count:           1000,
			TickIntervalNS:  1_000_000,
			DuplicateRate:   0.03,
			GapRate:         0.01,
			LateReorderRate: 0.04,
		})

		eng, err := storage.OpenEngine(cfg)
		if err != nil {
			t.Fatalf("OpenEngine: %v", err)
		}
		defer eng.Close(true)

		fixedIngestTS := int64(1764324000000000000)
		for i := 0; i < len(raw); i += 250 {
			end := i + 250
			if end > len(raw) {
				end = len(raw)
			}
			if _, err := eng.IngestBatch("repro-b", raw[i:end], fixedIngestTS); err != nil {
				t.Fatalf("IngestBatch: %v", err)
			}
		}
		if err := eng.FlushAll(); err != nil {
			t.Fatalf("FlushAll: %v", err)
		}

		qsvc := query.NewService(cfg.Query, eng)
		req := query.RangeRequest{
			StartTimeNS: raw[0].EventTimeNS - 1_000_000,
			EndTimeNS:   raw[len(raw)-1].EventTimeNS + 10_000_000_000,
			Limit:       5000,
		}
		jr, err := qsvc.ExportReplay(context.Background(), req, "jsonl", filepath.Join(dir, "out.jsonl"))
		if err != nil {
			t.Fatalf("export jsonl: %v", err)
		}
		cr, err := qsvc.ExportReplay(context.Background(), req, "csv", filepath.Join(dir, "out.csv"))
		if err != nil {
			t.Fatalf("export csv: %v", err)
		}
		pqPath := filepath.Join(dir, "out.parquet")
		pr, err := qsvc.ExportReplay(context.Background(), req, "parquet", pqPath)
		if err != nil {
			t.Fatalf("export parquet: %v", err)
		}
		pqBytes, err := os.ReadFile(pqPath)
		if err != nil {
			t.Fatalf("read parquet: %v", err)
		}
		decoded, err := codec.ReadParquet(pqBytes)
		if err != nil {
			t.Fatalf("decode parquet: %v", err)
		}
		if domain.LogicalDigest(decoded) != pr.LogicalSHA256 {
			t.Fatalf("parquet decoded logical digest mismatch")
		}
		return jr.LogicalSHA256, jr.FileSHA256, cr.FileSHA256, pr.ExportID
	}

	log1, jf1, cf1, id1 := runOnce(t.TempDir())
	log2, jf2, cf2, id2 := runOnce(t.TempDir())

	if log1 != log2 || jf1 != jf2 || cf1 != cf2 || id1 != id2 {
		t.Fatalf("non-deterministic output across identical runs:\nrun1: %s %s %s %s\nrun2: %s %s %s %s",
			log1, jf1, cf1, id1, log2, jf2, cf2, id2)
	}
}

func TestOperationalMissingConfigInvalidInputsAndRerunIdempotency(t *testing.T) {
	// 1. Missing config file -> ERR_CONFIG_NOT_FOUND
	_, err := config.Load(filepath.Join(t.TempDir(), "does_not_exist.json"))
	var cfgErr *config.ConfigError
	if !errors.As(err, &cfgErr) || cfgErr.Code != "ERR_CONFIG_NOT_FOUND" {
		t.Fatalf("expected ERR_CONFIG_NOT_FOUND, got %v", err)
	}

	// 2. Malformed JSON config -> ERR_CONFIG_PARSE_FAILED
	badJSONPath := filepath.Join(t.TempDir(), "bad.json")
	_ = os.WriteFile(badJSONPath, []byte("{not valid json"), 0o644)
	_, err = config.Load(badJSONPath)
	if !errors.As(err, &cfgErr) || cfgErr.Code != "ERR_CONFIG_PARSE_FAILED" {
		t.Fatalf("expected ERR_CONFIG_PARSE_FAILED, got %v", err)
	}

	// 3. Invalid config field -> ERR_CONFIG_INVALID
	badCfg := config.Default()
	badCfg.NumShards = 0
	if err := badCfg.Validate(); !errors.As(err, &cfgErr) || cfgErr.Code != "ERR_CONFIG_INVALID" {
		t.Fatalf("expected ERR_CONFIG_INVALID, got %v", err)
	}

	// 4. Unsupported export format -> ERR_UNSUPPORTED_EXPORT_FORMAT
	cfg := config.Default()
	cfg.DataDir = t.TempDir()
	eng, err := storage.OpenEngine(cfg)
	if err != nil {
		t.Fatalf("OpenEngine: %v", err)
	}
	defer eng.Close(true)
	qsvc := query.NewService(cfg.Query, eng)
	_, err = qsvc.ExportReplay(context.Background(), query.RangeRequest{
		StartTimeNS: 1764320400000000000,
		EndTimeNS:   1764320400000000000 + int64(time.Minute),
	}, "xml", filepath.Join(t.TempDir(), "bad.xml"))
	var qErr *query.QueryError
	if !errors.As(err, &qErr) || qErr.Code != "ERR_UNSUPPORTED_EXPORT_FORMAT" {
		t.Fatalf("expected ERR_UNSUPPORTED_EXPORT_FORMAT, got %v", err)
	}

	// 5. Full dataset rerun idempotency
	raw := ingest.GenerateSyntheticStream(ingest.SyntheticOptions{
		Seed:        55,
		Instruments: []string{"AAPL", "MSFT"},
		StartTimeNS: 1764320400000000000,
		Count:       200,
	})
	r1, err := eng.IngestBatch("run-1", raw, 1764324000000000000)
	if err != nil || r1.AcceptedCount != 200 {
		t.Fatalf("first ingest failed: res=%+v err=%v", r1, err)
	}
	r2, err := eng.IngestBatch("run-2-rerun", raw, 1764324005000000000)
	if err != nil || r2.AcceptedCount != 0 || r2.DuplicateCount != 200 {
		t.Fatalf("rerun was not idempotent: res=%+v err=%v", r2, err)
	}
}
