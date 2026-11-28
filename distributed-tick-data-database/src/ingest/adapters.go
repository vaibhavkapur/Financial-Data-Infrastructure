package ingest

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"distributed-tick-data-database/src/domain"
)

// SyntheticOptions controls deterministic synthetic tick stream generation.
type SyntheticOptions struct {
	Seed              int64
	Instruments       []string
	SourceID          string
	FeedSessionID     string
	StartTimeNS       int64
	Count             int
	TickIntervalNS    int64   // average step per tick
	DuplicateRate     float64 // probability [0, 1) of emitting an exact duplicate
	GapRate           float64 // probability [0, 1) of skipping a sequence number
	LateReorderRate   float64 // probability [0, 1) of deferring a tick to arrive later
}

// GenerateSyntheticStream produces a fully deterministic stream of RawTick events
// from a fixed seed, including optional controlled duplicates, sequence gaps, and
// out-of-order late arrivals.
func GenerateSyntheticStream(opts SyntheticOptions) []domain.RawTick {
	if len(opts.Instruments) == 0 {
		opts.Instruments = []string{"AAPL", "MSFT", "NVDA", "SPY"}
	}
	if opts.SourceID == "" {
		opts.SourceID = "sim-feed-a"
	}
	if opts.FeedSessionID == "" {
		opts.FeedSessionID = "sim-session-01"
	}
	if opts.StartTimeNS == 0 {
		opts.StartTimeNS = 1764320400000000000
	}
	if opts.TickIntervalNS <= 0 {
		opts.TickIntervalNS = 1_000_000 // 1ms
	}

	rng := rand.New(rand.NewSource(opts.Seed))
	seqs := make(map[string]uint64, len(opts.Instruments))
	prices := make(map[string]int64, len(opts.Instruments))
	venues := map[string]string{
		"AAPL":    "XNAS",
		"MSFT":    "XNAS",
		"NVDA":    "XNAS",
		"SPY":     "ARCX",
		"QQQ":     "XNAS",
		"ESZ6":    "XCME",
		"BTC-USD": "COINBASE",
		"ETH-USD": "COINBASE",
		"EURUSD":  "EBS",
	}
	basePrices := map[string]int64{
		"AAPL":    185_000_000_000,
		"MSFT":    412_000_000_000,
		"NVDA":    138_000_000_000,
		"SPY":     595_000_000_000,
		"QQQ":     505_000_000_000,
		"ESZ6":    6020_000_000_000,
		"BTC-USD": 95000_000_000_000,
		"ETH-USD": 3400_000_000_000,
		"EURUSD":  1_085_000_000,
	}

	for _, inst := range opts.Instruments {
		if bp, ok := basePrices[inst]; ok {
			prices[inst] = bp
		} else {
			prices[inst] = 100_000_000_000
		}
	}

	out := make([]domain.RawTick, 0, opts.Count)
	var deferred []domain.RawTick
	ts := opts.StartTimeNS

	for i := 0; i < opts.Count; i++ {
		inst := opts.Instruments[rng.Intn(len(opts.Instruments))]
		seqs[inst]++
		if opts.GapRate > 0 && rng.Float64() < opts.GapRate {
			// Skip one sequence number to create a real observable gap
			seqs[inst]++
		}

		ts += opts.TickIntervalNS + int64(rng.Intn(int(opts.TickIntervalNS/2+1)))
		step := int64(rng.Intn(21)-10) * 10_000_000 // +/- $0.10 in nanodollars
		p := prices[inst] + step
		if p < 100_000_000 {
			p = 100_000_000
		}
		prices[inst] = p

		venue := venues[inst]
		if venue == "" {
			venue = "XSIM"
		}

		spread := int64(10_000_000) // $0.01
		bid := p - spread
		if bid < 10_000_000 {
			bid = 10_000_000
		}
		ask := p + spread

		evType := domain.EventTypeTrade
		side := domain.SideBuy
		tradePrice := p
		tradeSize := int64(1+rng.Intn(200)) * 1_000_000_000
		rType := rng.Float64()
		if rType < 0.35 {
			evType = domain.EventTypeQuote
			side = domain.SideNone
			tradePrice = 0
			tradeSize = 0
		} else if rType < 0.45 {
			evType = domain.EventTypeOrderBookDelta
			if rng.Intn(2) == 0 {
				side = domain.SideSell
			}
		} else if rng.Intn(2) == 0 {
			side = domain.SideSell
		}

		tick := domain.RawTick{
			SchemaVersion:  domain.SchemaVersionV1,
			SourceID:       opts.SourceID,
			FeedSessionID:  opts.FeedSessionID,
			InstrumentID:   inst,
			Venue:          venue,
			EventType:      evType,
			Side:           side,
			EventTimeNS:    ts,
			SequenceNumber: seqs[inst],
			PriceNanos:     tradePrice,
			SizeNanos:      tradeSize,
			BidPriceNanos:  bid,
			BidSizeNanos:   int64(10+rng.Intn(500)) * 1_000_000_000,
			AskPriceNanos:  ask,
			AskSizeNanos:   int64(10+rng.Intn(500)) * 1_000_000_000,
		}

		if opts.LateReorderRate > 0 && rng.Float64() < opts.LateReorderRate && len(deferred) < 32 {
			deferred = append(deferred, tick)
		} else {
			out = append(out, tick)
			if opts.DuplicateRate > 0 && rng.Float64() < opts.DuplicateRate {
				out = append(out, tick)
			}
		}

		// Periodically flush a deferred tick as a late arrival
		if len(deferred) > 0 && i%17 == 0 {
			out = append(out, deferred[0])
			deferred = deferred[1:]
		}
	}

	out = append(out, deferred...)
	return out
}

// ReadJSONLFile parses a newline-delimited JSON file of RawTick records.
type JSONLReadError struct {
	LineNumber int    `json:"line_number"`
	Error      string `json:"error"`
}

func ReadJSONLFile(path string) ([]domain.RawTick, []JSONLReadError, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()
	return ReadJSONL(f)
}

func ReadJSONL(r io.Reader) ([]domain.RawTick, []JSONLReadError, error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 4*1024*1024)
	var ticks []domain.RawTick
	var parseErrs []JSONLReadError
	lineNo := 0
	for sc.Scan() {
		lineNo++
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var t domain.RawTick
		if err := json.Unmarshal([]byte(line), &t); err != nil {
			parseErrs = append(parseErrs, JSONLReadError{LineNumber: lineNo, Error: err.Error()})
			continue
		}
		ticks = append(ticks, t)
	}
	if err := sc.Err(); err != nil {
		return ticks, parseErrs, err
	}
	return ticks, parseErrs, nil
}

// WriteRawJSONL writes RawTicks to a JSONL file deterministically.
func WriteRawJSONL(path string, ticks []domain.RawTick) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	bw := bufio.NewWriter(f)
	for _, t := range ticks {
		b, err := json.Marshal(t)
		if err != nil {
			return err
		}
		if _, err := bw.Write(b); err != nil {
			return err
		}
		if err := bw.WriteByte('\n'); err != nil {
			return err
		}
	}
	return bw.Flush()
}

// ClickHouseSinkAdapter exports accepted CanonicalTicks to a ClickHouse HTTP
// interface using INSERT ... FORMAT JSONEachRow. Kept cleanly separated behind
// an adapter boundary so local/CI tests do not depend on a running ClickHouse
// instance.
type ClickHouseSinkAdapter struct {
	Endpoint string
	Database string
	Table    string
	Client   *http.Client
}

func NewClickHouseSinkAdapter(endpoint, database, table string) *ClickHouseSinkAdapter {
	if database == "" {
		database = "tickdb"
	}
	if table == "" {
		table = "market_ticks"
	}
	return &ClickHouseSinkAdapter{
		Endpoint: strings.TrimRight(endpoint, "/"),
		Database: database,
		Table:    table,
		Client:   &http.Client{Timeout: 10 * time.Second},
	}
}

type clickHouseRow struct {
	SchemaVersion  string `json:"schema_version"`
	SourceID       string `json:"source_id"`
	FeedSessionID  string `json:"feed_session_id"`
	BatchID        string `json:"batch_id"`
	InstrumentID   string `json:"instrument_id"`
	Venue          string `json:"venue"`
	EventType      string `json:"event_type"`
	Side           string `json:"side"`
	EventTimeNS    int64  `json:"event_time_ns"`
	IngestTimeNS   int64  `json:"ingest_time_ns"`
	SequenceNumber uint64 `json:"sequence_number"`
	PriceNanos     int64  `json:"price_nanos"`
	SizeNanos      int64  `json:"size_nanos"`
	BidPriceNanos  int64  `json:"bid_price_nanos"`
	BidSizeNanos   int64  `json:"bid_size_nanos"`
	AskPriceNanos  int64  `json:"ask_price_nanos"`
	AskSizeNanos   int64  `json:"ask_size_nanos"`
	LateArrival    uint8  `json:"late_arrival"`
	ShardID        uint16 `json:"shard_id"`
}

func (a *ClickHouseSinkAdapter) InsertBatch(ctx context.Context, ticks []domain.CanonicalTick) error {
	if len(ticks) == 0 {
		return nil
	}
	var body bytes.Buffer
	for _, t := range ticks {
		late := uint8(0)
		if t.LateArrival {
			late = 1
		}
		row := clickHouseRow{
			SchemaVersion:  t.SchemaVersion,
			SourceID:       t.SourceID,
			FeedSessionID:  t.FeedSessionID,
			BatchID:        t.BatchID,
			InstrumentID:   t.InstrumentID,
			Venue:          t.Venue,
			EventType:      string(t.EventType),
			Side:           string(t.Side),
			EventTimeNS:    t.EventTimeNS,
			IngestTimeNS:   t.IngestTimeNS,
			SequenceNumber: t.SequenceNumber,
			PriceNanos:     t.PriceNanos,
			SizeNanos:      t.SizeNanos,
			BidPriceNanos:  t.BidPriceNanos,
			BidSizeNanos:   t.BidSizeNanos,
			AskPriceNanos:  t.AskPriceNanos,
			AskSizeNanos:   t.AskSizeNanos,
			LateArrival:    late,
			ShardID:        t.ShardID,
		}
		b, err := json.Marshal(row)
		if err != nil {
			return err
		}
		body.Write(b)
		body.WriteByte('\n')
	}

	q := fmt.Sprintf("INSERT INTO %s.%s FORMAT JSONEachRow", a.Database, a.Table)
	reqURL := fmt.Sprintf("%s/?query=%s", a.Endpoint, url.QueryEscape(q))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, &body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	resp, err := a.Client.Do(req)
	if err != nil {
		return fmt.Errorf("ERR_CLICKHOUSE_UNREACHABLE: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("ERR_CLICKHOUSE_INSERT_FAILED (status=%d): %s", resp.StatusCode, strings.TrimSpace(string(errBody)))
	}
	return nil
}
