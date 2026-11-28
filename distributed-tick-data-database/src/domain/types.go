package domain

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
)

const (
	SchemaVersionV1 = "1.0.0"

	// Valid UTC epoch nanosecond bounds: 2000-01-01T00:00:00Z .. 2100-01-01T00:00:00Z
	MinValidEventTimeNS int64 = 946684800_000_000_000
	MaxValidEventTimeNS int64 = 4102444800_000_000_000
)

type EventType string

const (
	EventTypeTrade          EventType = "trade"
	EventTypeQuote          EventType = "quote"
	EventTypeOrderBookDelta EventType = "order_book_delta"
)

func (e EventType) Valid() bool {
	switch e {
	case EventTypeTrade, EventTypeQuote, EventTypeOrderBookDelta:
		return true
	default:
		return false
	}
}

func (e EventType) Code() uint8 {
	switch e {
	case EventTypeTrade:
		return 1
	case EventTypeQuote:
		return 2
	case EventTypeOrderBookDelta:
		return 3
	default:
		return 0
	}
}

func EventTypeFromCode(code uint8) EventType {
	switch code {
	case 1:
		return EventTypeTrade
	case 2:
		return EventTypeQuote
	case 3:
		return EventTypeOrderBookDelta
	default:
		return ""
	}
}

type Side string

const (
	SideNone Side = "none"
	SideBuy  Side = "buy"
	SideSell Side = "sell"
)

func (s Side) Valid() bool {
	switch s {
	case SideNone, SideBuy, SideSell:
		return true
	default:
		return false
	}
}

func (s Side) Code() uint8 {
	switch s {
	case SideNone:
		return 0
	case SideBuy:
		return 1
	case SideSell:
		return 2
	default:
		return 255
	}
}

func SideFromCode(code uint8) Side {
	switch code {
	case 0:
		return SideNone
	case 1:
		return SideBuy
	case 2:
		return SideSell
	default:
		return ""
	}
}

// DomainError is a structured, machine-readable validation or domain error.
type DomainError struct {
	Code    string `json:"code"`
	Field   string `json:"field,omitempty"`
	Message string `json:"message"`
}

func (e *DomainError) Error() string {
	if e.Field != "" {
		return fmt.Sprintf("%s (%s): %s", e.Code, e.Field, e.Message)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// RawTick is the external input contract emitted by market-data source adapters.
type RawTick struct {
	SchemaVersion  string    `json:"schema_version"`
	SourceID       string    `json:"source_id"`
	FeedSessionID  string    `json:"feed_session_id"`
	InstrumentID   string    `json:"instrument_id"`
	Venue          string    `json:"venue"`
	EventType      EventType `json:"event_type"`
	Side           Side      `json:"side"`
	EventTimeNS    int64     `json:"event_time_ns"`
	SequenceNumber uint64    `json:"sequence_number"`
	PriceNanos     int64     `json:"price_nanos"`
	SizeNanos      int64     `json:"size_nanos"`
	BidPriceNanos  int64     `json:"bid_price_nanos"`
	BidSizeNanos   int64     `json:"bid_size_nanos"`
	AskPriceNanos  int64     `json:"ask_price_nanos"`
	AskSizeNanos   int64     `json:"ask_size_nanos"`
}

// CanonicalTick is the validated, enriched, durable record stored by the engine.
type CanonicalTick struct {
	SchemaVersion  string    `json:"schema_version"`
	SourceID       string    `json:"source_id"`
	FeedSessionID  string    `json:"feed_session_id"`
	BatchID        string    `json:"batch_id"`
	InstrumentID   string    `json:"instrument_id"`
	Venue          string    `json:"venue"`
	EventType      EventType `json:"event_type"`
	Side           Side      `json:"side"`
	EventTimeNS    int64     `json:"event_time_ns"`
	IngestTimeNS   int64     `json:"ingest_time_ns"`
	SequenceNumber uint64    `json:"sequence_number"`
	PriceNanos     int64     `json:"price_nanos"`
	SizeNanos      int64     `json:"size_nanos"`
	BidPriceNanos  int64     `json:"bid_price_nanos"`
	BidSizeNanos   int64     `json:"bid_size_nanos"`
	AskPriceNanos  int64     `json:"ask_price_nanos"`
	AskSizeNanos   int64     `json:"ask_size_nanos"`
	LateArrival    bool      `json:"late_arrival"`
	ShardID        uint16    `json:"shard_id"`
	PartitionKey   string    `json:"partition_key"`
}

// StreamKey uniquely identifies a monotonic sequence stream.
type StreamKey struct {
	SourceID     string `json:"source_id"`
	InstrumentID string `json:"instrument_id"`
}

func (k StreamKey) String() string {
	return k.SourceID + "|" + k.InstrumentID
}

// PayloadDigest returns a deterministic 64-bit digest of the market payload for
// distinguishing exact idempotent duplicates from conflicting sequence reuse.
func (t RawTick) PayloadDigest() uint64 {
	h := sha256.New()
	writeStr := func(s string) {
		var b [4]byte
		binary.LittleEndian.PutUint32(b[:], uint32(len(s)))
		h.Write(b[:])
		h.Write([]byte(s))
	}
	writeI64 := func(v int64) {
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], uint64(v))
		h.Write(b[:])
	}
	writeStr(t.SchemaVersion)
	writeStr(t.SourceID)
	writeStr(t.FeedSessionID)
	writeStr(t.InstrumentID)
	writeStr(t.Venue)
	writeStr(string(t.EventType))
	writeStr(string(t.Side))
	writeI64(t.EventTimeNS)
	writeI64(int64(t.SequenceNumber))
	writeI64(t.PriceNanos)
	writeI64(t.SizeNanos)
	writeI64(t.BidPriceNanos)
	writeI64(t.BidSizeNanos)
	writeI64(t.AskPriceNanos)
	writeI64(t.AskSizeNanos)
	sum := h.Sum(nil)
	return binary.LittleEndian.Uint64(sum[:8])
}

// LogicalDigest computes a deterministic SHA-256 hex digest over the logical market
// fields of a slice of CanonicalTicks (excluding volatile ingest_time_ns / batch_id),
// enabling exact reproducibility verification across runs and storage formats.
func LogicalDigest(ticks []CanonicalTick) string {
	h := sha256.New()
	var buf [8]byte
	writeStr := func(s string) {
		binary.LittleEndian.PutUint32(buf[:4], uint32(len(s)))
		h.Write(buf[:4])
		h.Write([]byte(s))
	}
	writeI64 := func(v int64) {
		binary.LittleEndian.PutUint64(buf[:], uint64(v))
		h.Write(buf[:])
	}
	for _, t := range ticks {
		writeStr(t.SchemaVersion)
		writeStr(t.SourceID)
		writeStr(t.FeedSessionID)
		writeStr(t.InstrumentID)
		writeStr(t.Venue)
		writeStr(string(t.EventType))
		writeStr(string(t.Side))
		writeI64(t.EventTimeNS)
		writeI64(int64(t.SequenceNumber))
		writeI64(t.PriceNanos)
		writeI64(t.SizeNanos)
		writeI64(t.BidPriceNanos)
		writeI64(t.BidSizeNanos)
		writeI64(t.AskPriceNanos)
		writeI64(t.AskSizeNanos)
		if t.LateArrival {
			h.Write([]byte{1})
		} else {
			h.Write([]byte{0})
		}
	}
	return hex.EncodeToString(h.Sum(nil))
}

// ValidateRawTick enforces the canonical input data contract.
func ValidateRawTick(t RawTick) error {
	if t.SchemaVersion != SchemaVersionV1 {
		return &DomainError{
			Code:    "ERR_UNSUPPORTED_SCHEMA_VERSION",
			Field:   "schema_version",
			Message: fmt.Sprintf("unsupported schema version %q (supported: %q)", t.SchemaVersion, SchemaVersionV1),
		}
	}
	if t.SourceID == "" {
		return &DomainError{Code: "ERR_MISSING_SOURCE_ID", Field: "source_id", Message: "source_id is required"}
	}
	if t.FeedSessionID == "" {
		return &DomainError{Code: "ERR_MISSING_FEED_SESSION_ID", Field: "feed_session_id", Message: "feed_session_id is required"}
	}
	if t.InstrumentID == "" {
		return &DomainError{Code: "ERR_MISSING_INSTRUMENT_ID", Field: "instrument_id", Message: "instrument_id is required"}
	}
	if t.Venue == "" {
		return &DomainError{Code: "ERR_MISSING_VENUE", Field: "venue", Message: "venue is required"}
	}
	if !t.EventType.Valid() {
		return &DomainError{
			Code:    "ERR_INVALID_EVENT_TYPE",
			Field:   "event_type",
			Message: fmt.Sprintf("invalid event_type %q", t.EventType),
		}
	}
	if !t.Side.Valid() {
		return &DomainError{
			Code:    "ERR_INVALID_SIDE",
			Field:   "side",
			Message: fmt.Sprintf("invalid side %q", t.Side),
		}
	}
	if t.EventTimeNS < MinValidEventTimeNS || t.EventTimeNS > MaxValidEventTimeNS {
		return &DomainError{
			Code:    "ERR_INVALID_EVENT_TIME",
			Field:   "event_time_ns",
			Message: fmt.Sprintf("event_time_ns %d out of valid UTC epoch nanosecond bounds [%d, %d]", t.EventTimeNS, MinValidEventTimeNS, MaxValidEventTimeNS),
		}
	}
	if t.SequenceNumber == 0 {
		return &DomainError{
			Code:    "ERR_INVALID_SEQUENCE_NUMBER",
			Field:   "sequence_number",
			Message: "sequence_number must be >= 1",
		}
	}
	if t.BidPriceNanos < 0 || t.AskPriceNanos < 0 || t.BidSizeNanos < 0 || t.AskSizeNanos < 0 {
		return &DomainError{
			Code:    "ERR_NEGATIVE_BOOK_VALUE",
			Field:   "bid/ask",
			Message: "bid/ask prices and sizes must be >= 0",
		}
	}
	if t.BidPriceNanos > 0 && t.AskPriceNanos > 0 && t.BidPriceNanos > t.AskPriceNanos {
		return &DomainError{
			Code:    "ERR_CROSSED_QUOTE",
			Field:   "bid_price_nanos",
			Message: fmt.Sprintf("bid_price_nanos (%d) exceeds ask_price_nanos (%d)", t.BidPriceNanos, t.AskPriceNanos),
		}
	}
	switch t.EventType {
	case EventTypeTrade:
		if t.PriceNanos <= 0 {
			return &DomainError{Code: "ERR_INVALID_TRADE_PRICE", Field: "price_nanos", Message: "trade price_nanos must be > 0"}
		}
		if t.SizeNanos <= 0 {
			return &DomainError{Code: "ERR_INVALID_TRADE_SIZE", Field: "size_nanos", Message: "trade size_nanos must be > 0"}
		}
		if t.Side != SideBuy && t.Side != SideSell {
			return &DomainError{Code: "ERR_INVALID_TRADE_SIDE", Field: "side", Message: "trade side must be 'buy' or 'sell'"}
		}
	case EventTypeQuote:
		if t.BidPriceNanos <= 0 && t.AskPriceNanos <= 0 {
			return &DomainError{Code: "ERR_EMPTY_QUOTE", Field: "bid_price_nanos", Message: "quote must specify positive bid_price_nanos or ask_price_nanos"}
		}
	case EventTypeOrderBookDelta:
		if t.PriceNanos <= 0 {
			return &DomainError{Code: "ERR_INVALID_DELTA_PRICE", Field: "price_nanos", Message: "order_book_delta price_nanos must be > 0"}
		}
		if t.SizeNanos < 0 {
			return &DomainError{Code: "ERR_INVALID_DELTA_SIZE", Field: "size_nanos", Message: "order_book_delta size_nanos must be >= 0"}
		}
	}
	return nil
}

// CompareCanonicalOrder defines the total deterministic ordering for ticks in
// storage segments, query results, and replay exports.
func CompareCanonicalOrder(a, b CanonicalTick) int {
	if a.EventTimeNS != b.EventTimeNS {
		if a.EventTimeNS < b.EventTimeNS {
			return -1
		}
		return 1
	}
	if a.InstrumentID != b.InstrumentID {
		if a.InstrumentID < b.InstrumentID {
			return -1
		}
		return 1
	}
	if a.SourceID != b.SourceID {
		if a.SourceID < b.SourceID {
			return -1
		}
		return 1
	}
	if a.SequenceNumber != b.SequenceNumber {
		if a.SequenceNumber < b.SequenceNumber {
			return -1
		}
		return 1
	}
	return 0
}
