package tests

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"distributed-tick-data-database/src/config"
	"distributed-tick-data-database/src/domain"
	"distributed-tick-data-database/src/ingest"
	"distributed-tick-data-database/src/query"
	"distributed-tick-data-database/src/storage"
)

func TestLargeRangeQueryGuardsAndBoundedPagination(t *testing.T) {
	cfg := config.Default()
	cfg.DataDir = t.TempDir()
	cfg.NumShards = 4
	cfg.Query.MaxRangeDurationHours = 2
	cfg.Query.MaxLimitRows = 100
	cfg.Query.DefaultLimitRows = 50
	cfg.Query.MaxScanPartitions = 6

	eng, err := storage.OpenEngine(cfg)
	if err != nil {
		t.Fatalf("OpenEngine: %v", err)
	}
	defer eng.Close(true)

	baseTS := int64(1764320400_000_000_000)
	raw := ingest.GenerateSyntheticStream(ingest.SyntheticOptions{
		Seed:           303,
		Instruments:    []string{"AAPL", "MSFT"},
		StartTimeNS:    baseTS,
		Count:          260,
		TickIntervalNS: 10_000_000, // 10ms -> all within 1 hour
	})
	if _, err := eng.IngestBatch("b1", raw, baseTS+10_000_000_000); err != nil {
		t.Fatalf("IngestBatch: %v", err)
	}
	if err := eng.FlushAll(); err != nil {
		t.Fatalf("FlushAll: %v", err)
	}

	qsvc := query.NewService(cfg.Query, eng)

	// 1. Range duration exceeding max_range_duration_hours -> ERR_QUERY_RANGE_TOO_LARGE
	_, err = qsvc.QueryRange(context.Background(), query.RangeRequest{
		InstrumentID: "AAPL",
		StartTimeNS:  baseTS,
		EndTimeNS:    baseTS + int64(5*time.Hour),
		Limit:        50,
	})
	var qErr *query.QueryError
	if !errors.As(err, &qErr) || qErr.Code != "ERR_QUERY_RANGE_TOO_LARGE" {
		t.Fatalf("expected ERR_QUERY_RANGE_TOO_LARGE, got %v", err)
	}

	// 2. Limit exceeding max_limit_rows -> ERR_QUERY_LIMIT_EXCEEDED
	_, err = qsvc.QueryRange(context.Background(), query.RangeRequest{
		InstrumentID: "AAPL",
		StartTimeNS:  baseTS,
		EndTimeNS:    baseTS + int64(time.Hour),
		Limit:        500,
	})
	if !errors.As(err, &qErr) || qErr.Code != "ERR_QUERY_LIMIT_EXCEEDED" {
		t.Fatalf("expected ERR_QUERY_LIMIT_EXCEEDED, got %v", err)
	}

	// 3. Cross-shard scan exceeding max_scan_partitions (2 hours = 3 buckets * 4 shards = 12 > 6)
	_, err = qsvc.QueryRange(context.Background(), query.RangeRequest{
		StartTimeNS: baseTS,
		EndTimeNS:   baseTS + int64(2*time.Hour),
		Limit:       50,
	})
	if !errors.As(err, &qErr) || qErr.Code != "ERR_QUERY_PARTITION_BUDGET_EXCEEDED" {
		t.Fatalf("expected ERR_QUERY_PARTITION_BUDGET_EXCEEDED, got %v", err)
	}

	// 4. Inverted range -> ERR_QUERY_INVALID_RANGE
	_, err = qsvc.QueryRange(context.Background(), query.RangeRequest{
		StartTimeNS: baseTS + 1000,
		EndTimeNS:   baseTS,
		Limit:       50,
	})
	if !errors.As(err, &qErr) || qErr.Code != "ERR_QUERY_INVALID_RANGE" {
		t.Fatalf("expected ERR_QUERY_INVALID_RANGE, got %v", err)
	}

	// 5. Bounded cursor pagination retrieves all 260 rows across 4 pages with zero duplicates or gaps
	var collected []domain.CanonicalTick
	var cursor *query.PageCursor
	pages := 0
	for {
		pages++
		resp, err := qsvc.QueryRange(context.Background(), query.RangeRequest{
			StartTimeNS: baseTS,
			EndTimeNS:   baseTS + int64(30*time.Minute),
			Limit:       75,
			AfterCursor: cursor,
		})
		if err != nil {
			t.Fatalf("page %d failed: %v", pages, err)
		}
		collected = append(collected, resp.Ticks...)
		if !resp.HasMore {
			break
		}
		cursor = resp.NextCursor
	}
	if len(collected) != 260 || pages != 4 {
		t.Fatalf("paginated scan got %d rows in %d pages; want 260 rows in 4 pages", len(collected), pages)
	}
}

func TestIngestionBackpressureRejectsExplicitlyWithZeroSilentLoss(t *testing.T) {
	cfg := config.Default()
	cfg.DataDir = t.TempDir()
	cfg.Ingest.QueueCapacityBatches = 2
	cfg.Ingest.BackpressureTimeoutMS = 30

	eng, err := storage.OpenEngine(cfg)
	if err != nil {
		t.Fatalf("OpenEngine: %v", err)
	}
	defer eng.Close(true)

	// Single worker with 80ms commit delay guarantees queue saturation under concurrent burst
	pipe := ingest.NewPipelineWithCommitter(cfg.Ingest, eng, 1, 80*time.Millisecond)
	defer pipe.Close()

	baseTS := int64(1764320400_000_000_000)
	const totalBatches = 12
	const ticksPerBatch = 10

	var (
		wg               sync.WaitGroup
		ackBatches       atomic.Int64
		ackTicks         atomic.Int64
		backpressureHits atomic.Int64
		otherErrors      atomic.Int64
	)

	for b := 0; b < totalBatches; b++ {
		wg.Add(1)
		go func(batchIdx int) {
			defer wg.Done()
			inst := fmt.Sprintf("SYM-%02d", batchIdx)
			ticks := make([]domain.RawTick, ticksPerBatch)
			for i := 0; i < ticksPerBatch; i++ {
				ticks[i] = domain.RawTick{
					SchemaVersion:  domain.SchemaVersionV1,
					SourceID:       "bp-feed",
					FeedSessionID:  "bp-s1",
					InstrumentID:   inst,
					Venue:          "XNAS",
					EventType:      domain.EventTypeTrade,
					Side:           domain.SideBuy,
					EventTimeNS:    baseTS + int64(batchIdx*1000+i)*1_000_000,
					SequenceNumber: uint64(i + 1),
					PriceNanos:     100_000_000_000,
					SizeNanos:      1_000_000_000,
					BidPriceNanos:  99_990_000_000,
					BidSizeNanos:   10_000_000_000,
					AskPriceNanos:  100_010_000_000,
					AskSizeNanos:   10_000_000_000,
				}
			}
			res, err := pipe.Submit(context.Background(), fmt.Sprintf("bp-%02d", batchIdx), ticks, baseTS+50_000_000_000)
			if err != nil {
				var bpErr *ingest.BackpressureError
				if errors.As(err, &bpErr) && bpErr.Code == "ERR_BACKPRESSURE_QUEUE_FULL" {
					backpressureHits.Add(1)
					return
				}
				otherErrors.Add(1)
				return
			}
			ackBatches.Add(1)
			ackTicks.Add(int64(res.AcceptedCount))
		}(b)
	}
	wg.Wait()

	if otherErrors.Load() != 0 {
		t.Fatalf("unexpected non-backpressure errors: %d", otherErrors.Load())
	}
	if backpressureHits.Load() == 0 {
		t.Fatalf("expected backpressure rejections under saturated queue, got 0")
	}
	if ackBatches.Load() == 0 {
		t.Fatalf("expected some batches to commit, got 0")
	}
	metrics := pipe.Metrics()
	if metrics.BackpressureRejects != uint64(backpressureHits.Load()) {
		t.Fatalf("metrics mismatch: %d vs %d", metrics.BackpressureRejects, backpressureHits.Load())
	}

	// Verify ZERO silently lost accepted ticks: queried rows MUST equal acknowledged ticks!
	qsvc := query.NewService(cfg.Query, eng)
	resp, err := qsvc.QueryRange(context.Background(), query.RangeRequest{
		StartTimeNS: baseTS,
		EndTimeNS:   baseTS + int64(time.Hour),
		Limit:       1000,
	})
	if err != nil {
		t.Fatalf("QueryRange: %v", err)
	}
	if int64(resp.RowCount) != ackTicks.Load() {
		t.Fatalf("SILENT TICK LOSS: queried %d rows, acknowledged %d ticks", resp.RowCount, ackTicks.Load())
	}
}
