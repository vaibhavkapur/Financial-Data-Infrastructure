package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"distributed-tick-data-database/src/config"
	"distributed-tick-data-database/src/domain"
	"distributed-tick-data-database/src/ingest"
	"distributed-tick-data-database/src/query"
	"distributed-tick-data-database/src/server"
	"distributed-tick-data-database/src/storage"
)

func TestNodeRestartPreservesFlushedAndWALOnlyDataAndIdempotency(t *testing.T) {
	cfg := config.Default()
	cfg.DataDir = t.TempDir()
	cfg.Storage.MemtableMaxEvents = 250 // ensures some shards flush while trailing batches stay WAL-only

	raw := ingest.GenerateSyntheticStream(ingest.SyntheticOptions{
		Seed:            101,
		Instruments:     []string{"AAPL", "MSFT", "NVDA", "SPY"},
		SourceID:        "restart-feed",
		FeedSessionID:   "rs-01",
		StartTimeNS:     1764320400000000000,
		Count:           1500,
		TickIntervalNS:  2_000_000,
		DuplicateRate:   0.04,
		GapRate:         0.01,
		LateReorderRate: 0.05,
	})

	part := domain.NewPartitioner(cfg.NumShards, cfg.PartitionGranularity)
	oracle := domain.NewReferenceOracle(domain.SequencerConfig{LateEventPolicy: cfg.Ingest.LateEventPolicy}, part)

	eng, err := storage.OpenEngine(cfg)
	if err != nil {
		t.Fatalf("OpenEngine: %v", err)
	}
	for i := 0; i < len(raw); i += 200 {
		end := i + 200
		if end > len(raw) {
			end = len(raw)
		}
		bid := "batch"
		ts := int64(1764324000000000000) + int64(i)
		oracle.Ingest(bid, raw[i:end], ts)
		if _, err := eng.IngestBatch(bid, raw[i:end], ts); err != nil {
			t.Fatalf("IngestBatch: %v", err)
		}
	}

	// Simulate abrupt crash/restart WITHOUT flushing trailing memtables!
	if err := eng.Close(false); err != nil {
		t.Fatalf("Close(false): %v", err)
	}

	reopened, err := storage.OpenEngine(cfg)
	if err != nil {
		t.Fatalf("OpenEngine after restart: %v", err)
	}
	defer reopened.Close(true)

	// Confirm WAL recovery replayed unflushed events
	recoveredTotal := 0
	for _, rep := range reopened.RecoveryReports() {
		recoveredTotal += rep.AppliedWALEvents
	}
	if recoveredTotal == 0 {
		t.Fatalf("expected non-zero WAL events recovered after ungraceful shutdown")
	}

	// Query full range and assert exact match against ReferenceOracle (zero lost ticks!)
	qsvc := query.NewService(cfg.Query, reopened)
	resp, err := qsvc.QueryRange(context.Background(), query.RangeRequest{
		StartTimeNS: raw[0].EventTimeNS - 1_000_000_000,
		EndTimeNS:   raw[len(raw)-1].EventTimeNS + 10_000_000_000,
		Limit:       10000,
	})
	if err != nil {
		t.Fatalf("QueryRange after restart: %v", err)
	}
	oracleRows := oracle.AllOrdered()
	if resp.RowCount != len(oracleRows) {
		t.Fatalf("row count after restart: got %d want %d", resp.RowCount, len(oracleRows))
	}
	if resp.LogicalSHA256 != domain.LogicalDigest(oracleRows) {
		t.Fatalf("logical digest mismatch after restart: got %s want %s", resp.LogicalSHA256, domain.LogicalDigest(oracleRows))
	}

	// Re-ingesting an already committed batch after restart must be 100% deduplicated
	dupRes, err := reopened.IngestBatch("post-restart-replay", raw[:200], 1764324999000000000)
	if err != nil {
		t.Fatalf("post-restart duplicate ingest: %v", err)
	}
	if dupRes.AcceptedCount != 0 || dupRes.DuplicateCount != 200 {
		t.Fatalf("expected 0 accepted and 200 duplicates after restart, got %+v", dupRes)
	}
}

func TestPartialBatchWriteRecoveryAndOrphanTempCleanup(t *testing.T) {
	cfg := config.Default()
	cfg.DataDir = t.TempDir()
	cfg.NumShards = 1
	cfg.Storage.MemtableMaxEvents = 5000 // keep everything in WAL for this crash test

	raw := ingest.GenerateSyntheticStream(ingest.SyntheticOptions{
		Seed:        202,
		Instruments: []string{"AAPL"},
		StartTimeNS: 1764320400000000000,
		Count:       300,
	})

	eng, err := storage.OpenEngine(cfg)
	if err != nil {
		t.Fatalf("OpenEngine: %v", err)
	}
	// Commit 2 valid batches (200 ticks)
	if _, err := eng.IngestBatch("good-1", raw[:100], 1764320500000000000); err != nil {
		t.Fatalf("good-1: %v", err)
	}
	if _, err := eng.IngestBatch("good-2", raw[100:200], 1764320501000000000); err != nil {
		t.Fatalf("good-2: %v", err)
	}
	walPath := eng.Shard(0).WALPath()
	shardDir := eng.Shard(0).Dir()
	if err := eng.Close(false); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// 1. Inject simulated partial batch write (truncated WAL frame) at tail of wal.log
	f, err := os.OpenFile(walPath, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open WAL for corruption: %v", err)
	}
	corruptTail := []byte("TWAL\x99\x04\x00\x00\xde\xad\xbe\xeftruncated_partial_batch_payload")
	if _, err := f.Write(corruptTail); err != nil {
		t.Fatalf("append corrupt tail: %v", err)
	}
	_ = f.Close()

	// 2. Drop an orphaned .tseg.tmp file simulating a crash mid-segment flush
	orphanPath := filepath.Join(shardDir, "segments", "2025-11-28T09", "seg-orphan.tseg.tmp")
	if err := os.MkdirAll(filepath.Dir(orphanPath), 0o755); err != nil {
		t.Fatalf("mkdir orphan dir: %v", err)
	}
	if err := os.WriteFile(orphanPath, []byte("incomplete segment"), 0o644); err != nil {
		t.Fatalf("write orphan tmp: %v", err)
	}

	// 3. First verify strict mode (TruncateCorruptTailOnRecovery=false) fails explicitly
	strictCfg := cfg
	strictCfg.WAL.TruncateCorruptTailOnRecovery = false
	if _, err := storage.OpenEngine(strictCfg); err == nil || !strings.Contains(err.Error(), "ERR_WAL_CORRUPT_TAIL") {
		t.Fatalf("expected ERR_WAL_CORRUPT_TAIL when truncate disabled, got %v", err)
	}

	// 4. Reopen with recovery truncation enabled -> recovers both committed batches, truncates partial tail, cleans orphan tmp
	recovered, err := storage.OpenEngine(cfg)
	if err != nil {
		t.Fatalf("OpenEngine recovery: %v", err)
	}
	defer recovered.Close(true)

	rep := recovered.RecoveryReports()[0]
	if !rep.WALStats.CorruptTailDetected || rep.WALStats.TruncatedCorruptBytes != int64(len(corruptTail)) {
		t.Fatalf("expected corrupt tail detection (%d bytes), got %+v", len(corruptTail), rep.WALStats)
	}
	if rep.AppliedWALEvents != 200 {
		t.Fatalf("expected 200 committed events recovered, got %d", rep.AppliedWALEvents)
	}
	if rep.OrphanedTempFilesCleaned < 1 {
		t.Fatalf("expected orphaned .tmp cleanup, got %d", rep.OrphanedTempFilesCleaned)
	}
	if _, err := os.Stat(orphanPath); !os.IsNotExist(err) {
		t.Fatalf("orphan temp file still exists")
	}

	// 5. Commit third batch after recovery and verify all 300 ticks query cleanly
	if _, err := recovered.IngestBatch("good-3", raw[200:300], 1764320502000000000); err != nil {
		t.Fatalf("good-3 after recovery: %v", err)
	}
	qsvc := query.NewService(cfg.Query, recovered)
	resp, err := qsvc.QueryRange(context.Background(), query.RangeRequest{
		InstrumentID: "AAPL",
		StartTimeNS:  raw[0].EventTimeNS - 1,
		EndTimeNS:    raw[len(raw)-1].EventTimeNS + 1,
		Limit:        1000,
	})
	if err != nil {
		t.Fatalf("QueryRange after recovery: %v", err)
	}
	if resp.RowCount != 300 {
		t.Fatalf("expected 300 ticks after recovery + post-recovery batch, got %d", resp.RowCount)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func TestHTTPAPIAndClickHouseAdapterIntegration(t *testing.T) {
	cfg := config.Default()
	cfg.DataDir = t.TempDir()
	eng, err := storage.OpenEngine(cfg)
	if err != nil {
		t.Fatalf("OpenEngine: %v", err)
	}
	defer eng.Close(true)
	pipe := ingest.NewPipeline(cfg.Ingest, eng, 1)
	defer pipe.Close()
	qsvc := query.NewService(cfg.Query, eng)
	srv := server.New(eng, pipe, qsvc)
	handler := srv.Handler()

	// Health check (in-process via httptest.ResponseRecorder, no network socket required)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("health failed: status=%d body=%s", rec.Code, rec.Body.String())
	}

	// Ingest via HTTP handler
	sample := ingest.GenerateSyntheticStream(ingest.SyntheticOptions{
		Seed:        7,
		Instruments: []string{"AAPL"},
		StartTimeNS: 1764320400000000000,
		Count:       25,
	})
	bodyBytes, _ := json.Marshal(map[string]any{
		"batch_id": "http-b1",
		"ticks":    sample,
	})
	ingRec := httptest.NewRecorder()
	handler.ServeHTTP(ingRec, httptest.NewRequest(http.MethodPost, "/v1/ingest", bytes.NewReader(bodyBytes)))
	if ingRec.Code != http.StatusOK {
		t.Fatalf("HTTP ingest failed: status=%d body=%s", ingRec.Code, ingRec.Body.String())
	}

	// Query via HTTP handler
	qRec := httptest.NewRecorder()
	handler.ServeHTTP(qRec, httptest.NewRequest(http.MethodGet, "/v1/query?instrument_id=AAPL&start_time_ns=1764320400000000000&end_time_ns=1764324000000000000&limit=100", nil))
	if qRec.Code != http.StatusOK {
		t.Fatalf("HTTP query failed: status=%d body=%s", qRec.Code, qRec.Body.String())
	}
	var parsed query.RangeResponse
	if err := json.NewDecoder(qRec.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode query response: %v", err)
	}
	if parsed.RowCount != 25 {
		t.Fatalf("HTTP query row count: got %d want 25", parsed.RowCount)
	}

	// Metrics and gaps endpoints
	gRec := httptest.NewRecorder()
	handler.ServeHTTP(gRec, httptest.NewRequest(http.MethodGet, "/v1/gaps", nil))
	if gRec.Code != http.StatusOK {
		t.Fatalf("HTTP gaps failed: status=%d", gRec.Code)
	}
	mRec := httptest.NewRecorder()
	handler.ServeHTTP(mRec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if mRec.Code != http.StatusOK {
		t.Fatalf("HTTP metrics failed: status=%d", mRec.Code)
	}

	// ClickHouse HTTP adapter verified against in-process mock RoundTripper
	var receivedQuery string
	var receivedLines int
	chAdapter := ingest.NewClickHouseSinkAdapter("http://clickhouse.mock:8123", "tickdb", "market_ticks")
	chAdapter.Client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		receivedQuery = r.URL.Query().Get("query")
		var buf bytes.Buffer
		_, _ = buf.ReadFrom(r.Body)
		lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
		receivedLines = len(lines)
		rw := httptest.NewRecorder()
		rw.WriteHeader(http.StatusOK)
		return rw.Result(), nil
	})
	if err := chAdapter.InsertBatch(context.Background(), parsed.Ticks); err != nil {
		t.Fatalf("ClickHouse adapter insert failed: %v", err)
	}
	if receivedQuery != "INSERT INTO tickdb.market_ticks FORMAT JSONEachRow" || receivedLines != 25 {
		t.Fatalf("unexpected ClickHouse request: query=%q lines=%d", receivedQuery, receivedLines)
	}
}
