package query

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"distributed-tick-data-database/src/codec"
	"distributed-tick-data-database/src/config"
	"distributed-tick-data-database/src/domain"
	"distributed-tick-data-database/src/storage"
)

// QueryError is a structured, bounded-query safety error.
type QueryError struct {
	Code    string `json:"code"`
	Field   string `json:"field,omitempty"`
	Message string `json:"message"`
}

func (e *QueryError) Error() string {
	if e.Field != "" {
		return fmt.Sprintf("%s (%s): %s", e.Code, e.Field, e.Message)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// PageCursor enables deterministic bounded pagination over large ranges.
type PageCursor struct {
	EventTimeNS    int64  `json:"event_time_ns"`
	InstrumentID   string `json:"instrument_id"`
	SourceID       string `json:"source_id"`
	SequenceNumber uint64 `json:"sequence_number"`
}

type RangeRequest struct {
	InstrumentID string      `json:"instrument_id,omitempty"`
	StartTimeNS  int64       `json:"start_time_ns"`
	EndTimeNS    int64       `json:"end_time_ns"`
	Limit        int         `json:"limit,omitempty"`
	AfterCursor  *PageCursor `json:"after_cursor,omitempty"`
}

type RangeResponse struct {
	Ticks           []domain.CanonicalTick `json:"ticks"`
	RowCount        int                    `json:"row_count"`
	HasMore         bool                   `json:"has_more"`
	NextCursor      *PageCursor            `json:"next_cursor,omitempty"`
	SegmentsScanned int                    `json:"segments_scanned"`
	SegmentsPruned  int                    `json:"segments_pruned"`
	ShardsScanned   int                    `json:"shards_scanned"`
	DurationMS      float64                `json:"duration_ms"`
	LogicalSHA256   string                 `json:"logical_sha256"`
}

type ExportReceipt struct {
	ExportID      string  `json:"export_id"`
	Format        string  `json:"format"`
	OutputPath    string  `json:"output_path"`
	RowCount      int     `json:"row_count"`
	BytesWritten  int64   `json:"bytes_written"`
	LogicalSHA256 string  `json:"logical_sha256"`
	FileSHA256    string  `json:"file_sha256"`
	DurationMS    float64 `json:"duration_ms"`
}

type Service struct {
	cfg    config.QueryConfig
	engine *storage.Engine
}

func NewService(cfg config.QueryConfig, engine *storage.Engine) *Service {
	return &Service{
		cfg:    cfg,
		engine: engine,
	}
}

// QueryRange executes a bounded historical range query with strict safety limits
// on time span, row limit, partition scan budget, and execution timeout.
func (s *Service) QueryRange(ctx context.Context, req RangeRequest) (RangeResponse, error) {
	start := time.Now()
	if req.EndTimeNS < req.StartTimeNS {
		return RangeResponse{}, &QueryError{
			Code:    "ERR_QUERY_INVALID_RANGE",
			Field:   "end_time_ns",
			Message: fmt.Sprintf("end_time_ns (%d) must be >= start_time_ns (%d)", req.EndTimeNS, req.StartTimeNS),
		}
	}

	maxSpan := time.Duration(s.cfg.MaxRangeDurationHours) * time.Hour
	span := time.Duration(req.EndTimeNS - req.StartTimeNS)
	if span > maxSpan {
		return RangeResponse{}, &QueryError{
			Code:  "ERR_QUERY_RANGE_TOO_LARGE",
			Field: "time_range",
			Message: fmt.Sprintf("requested range %.2fh exceeds max_range_duration_hours (%dh); split query into bounded windows",
				span.Hours(), s.cfg.MaxRangeDurationHours),
		}
	}

	limit := req.Limit
	if limit < 0 {
		return RangeResponse{}, &QueryError{
			Code:    "ERR_QUERY_INVALID_LIMIT",
			Field:   "limit",
			Message: "limit must be >= 0",
		}
	}
	if limit == 0 {
		limit = s.cfg.DefaultLimitRows
	}
	if limit > s.cfg.MaxLimitRows {
		return RangeResponse{}, &QueryError{
			Code:    "ERR_QUERY_LIMIT_EXCEEDED",
			Field:   "limit",
			Message: fmt.Sprintf("requested limit %d exceeds max_limit_rows (%d); use cursor pagination", limit, s.cfg.MaxLimitRows),
		}
	}

	buckets := s.engine.Partitioner().TimeBucketsInRange(req.StartTimeNS, req.EndTimeNS)
	shardsToScan := s.engine.Config().NumShards
	if req.InstrumentID != "" {
		shardsToScan = 1
	}
	partitionScanCount := len(buckets) * shardsToScan
	if partitionScanCount > s.cfg.MaxScanPartitions {
		return RangeResponse{}, &QueryError{
			Code:  "ERR_QUERY_PARTITION_BUDGET_EXCEEDED",
			Field: "partitions",
			Message: fmt.Sprintf("query would scan %d partitions (%d buckets x %d shards), exceeding max_scan_partitions (%d)",
				partitionScanCount, len(buckets), shardsToScan, s.cfg.MaxScanPartitions),
		}
	}

	qCtx, cancel := context.WithTimeout(ctx, time.Duration(s.cfg.TimeoutMS)*time.Millisecond)
	defer cancel()
	select {
	case <-qCtx.Done():
		return RangeResponse{}, &QueryError{
			Code:    "ERR_QUERY_TIMEOUT",
			Message: qCtx.Err().Error(),
		}
	default:
	}

	rows, scanned, pruned, shardsUsed, err := s.engine.ScanRange(req.InstrumentID, req.StartTimeNS, req.EndTimeNS)
	if err != nil {
		return RangeResponse{}, err
	}

	select {
	case <-qCtx.Done():
		return RangeResponse{}, &QueryError{
			Code:    "ERR_QUERY_TIMEOUT",
			Message: qCtx.Err().Error(),
		}
	default:
	}

	// Apply cursor filter if paginating
	if req.AfterCursor != nil {
		pivot := domain.CanonicalTick{
			EventTimeNS:    req.AfterCursor.EventTimeNS,
			InstrumentID:   req.AfterCursor.InstrumentID,
			SourceID:       req.AfterCursor.SourceID,
			SequenceNumber: req.AfterCursor.SequenceNumber,
		}
		filtered := rows[:0]
		for _, r := range rows {
			if domain.CompareCanonicalOrder(r, pivot) > 0 {
				filtered = append(filtered, r)
			}
		}
		rows = filtered
	}

	hasMore := false
	var nextCursor *PageCursor
	if len(rows) > limit {
		hasMore = true
		rows = rows[:limit]
		last := rows[len(rows)-1]
		nextCursor = &PageCursor{
			EventTimeNS:    last.EventTimeNS,
			InstrumentID:   last.InstrumentID,
			SourceID:       last.SourceID,
			SequenceNumber: last.SequenceNumber,
		}
	}

	return RangeResponse{
		Ticks:           rows,
		RowCount:        len(rows),
		HasMore:         hasMore,
		NextCursor:      nextCursor,
		SegmentsScanned: scanned,
		SegmentsPruned:  pruned,
		ShardsScanned:   shardsUsed,
		DurationMS:      float64(time.Since(start).Microseconds()) / 1000.0,
		LogicalSHA256:   domain.LogicalDigest(rows),
	}, nil
}

// ExportReplay exports a bounded historical range in canonical replay order to
// JSONL, CSV, or Apache Parquet, returning stable cryptographic digests.
func (s *Service) ExportReplay(ctx context.Context, req RangeRequest, format string, outputPath string) (ExportReceipt, error) {
	start := time.Now()
	switch format {
	case "jsonl", "csv", "parquet":
	default:
		return ExportReceipt{}, &QueryError{
			Code:    "ERR_UNSUPPORTED_EXPORT_FORMAT",
			Field:   "format",
			Message: fmt.Sprintf("unsupported export format %q (supported: jsonl, csv, parquet)", format),
		}
	}

	resp, err := s.QueryRange(ctx, req)
	if err != nil {
		return ExportReceipt{}, err
	}

	var buf bytes.Buffer
	switch format {
	case "jsonl":
		bw := bufio.NewWriter(&buf)
		for _, t := range resp.Ticks {
			b, err := json.Marshal(t)
			if err != nil {
				return ExportReceipt{}, err
			}
			_, _ = bw.Write(b)
			_ = bw.WriteByte('\n')
		}
		if err := bw.Flush(); err != nil {
			return ExportReceipt{}, err
		}
	case "csv":
		cw := csv.NewWriter(&buf)
		_ = cw.Write([]string{
			"schema_version", "source_id", "feed_session_id", "batch_id",
			"instrument_id", "venue", "event_type", "side",
			"event_time_ns", "ingest_time_ns", "sequence_number",
			"price_nanos", "size_nanos", "bid_price_nanos", "bid_size_nanos",
			"ask_price_nanos", "ask_size_nanos", "late_arrival", "shard_id", "partition_key",
		})
		for _, t := range resp.Ticks {
			_ = cw.Write([]string{
				t.SchemaVersion, t.SourceID, t.FeedSessionID, t.BatchID,
				t.InstrumentID, t.Venue, string(t.EventType), string(t.Side),
				strconv.FormatInt(t.EventTimeNS, 10),
				strconv.FormatInt(t.IngestTimeNS, 10),
				strconv.FormatUint(t.SequenceNumber, 10),
				strconv.FormatInt(t.PriceNanos, 10),
				strconv.FormatInt(t.SizeNanos, 10),
				strconv.FormatInt(t.BidPriceNanos, 10),
				strconv.FormatInt(t.BidSizeNanos, 10),
				strconv.FormatInt(t.AskPriceNanos, 10),
				strconv.FormatInt(t.AskSizeNanos, 10),
				strconv.FormatBool(t.LateArrival),
				strconv.FormatUint(uint64(t.ShardID), 10),
				t.PartitionKey,
			})
		}
		cw.Flush()
		if err := cw.Error(); err != nil {
			return ExportReceipt{}, err
		}
	case "parquet":
		if _, err := codec.WriteParquet(&buf, resp.Ticks); err != nil {
			return ExportReceipt{}, err
		}
	}

	raw := buf.Bytes()
	if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
		return ExportReceipt{}, err
	}
	if err := os.WriteFile(outputPath, raw, 0o644); err != nil {
		return ExportReceipt{}, err
	}

	fileHash := sha256.Sum256(raw)
	exportIDHash := sha256.Sum256([]byte(fmt.Sprintf("%s|%s|%d|%d|%s", req.InstrumentID, format, req.StartTimeNS, req.EndTimeNS, resp.LogicalSHA256)))

	return ExportReceipt{
		ExportID:      "exp-" + hex.EncodeToString(exportIDHash[:8]),
		Format:        format,
		OutputPath:    outputPath,
		RowCount:      resp.RowCount,
		BytesWritten:  int64(len(raw)),
		LogicalSHA256: resp.LogicalSHA256,
		FileSHA256:    hex.EncodeToString(fileHash[:]),
		DurationMS:    float64(time.Since(start).Microseconds()) / 1000.0,
	}, nil
}
