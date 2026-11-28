package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

const SupportedConfigVersion = "1.0.0"

// ConfigError is a typed operational error for missing or invalid configuration.
type ConfigError struct {
	Code    string `json:"code"`
	Field   string `json:"field,omitempty"`
	Message string `json:"message"`
}

func (e *ConfigError) Error() string {
	if e.Field != "" {
		return fmt.Sprintf("%s (%s): %s", e.Code, e.Field, e.Message)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

type WALConfig struct {
	SyncMode                      string `json:"sync_mode"` // "fsync_per_batch" | "os_buffered"
	MaxSegmentBytes               int64  `json:"max_segment_bytes"`
	TruncateCorruptTailOnRecovery bool   `json:"truncate_corrupt_tail_on_recovery"`
}

type StorageConfig struct {
	MemtableMaxEvents int    `json:"memtable_max_events"`
	Compression       string `json:"compression"` // "delta_dict_gzip" | "delta_dict_raw"
	EnableChecksums   bool   `json:"enable_checksums"`
}

type IngestConfig struct {
	MaxBatchSize              int    `json:"max_batch_size"`
	QueueCapacityBatches      int    `json:"queue_capacity_batches"`
	BackpressureTimeoutMS     int    `json:"backpressure_timeout_ms"`
	LateEventPolicy           string `json:"late_event_policy"` // "accept_and_flag" | "reject_too_late"
	LateEventMaxWatermarkLagMS int64  `json:"late_event_max_watermark_lag_ms"`
}

type QueryConfig struct {
	MaxRangeDurationHours int `json:"max_range_duration_hours"`
	MaxLimitRows          int `json:"max_limit_rows"`
	MaxScanPartitions     int `json:"max_scan_partitions"`
	DefaultLimitRows      int `json:"default_limit_rows"`
	TimeoutMS             int `json:"timeout_ms"`
}

type GeneratorConfig struct {
	Seed          int64    `json:"seed"`
	Instruments   []string `json:"instruments"`
	SourceID      string   `json:"source_id"`
	FeedSessionID string   `json:"feed_session_id"`
	StartTimeNS   int64    `json:"start_time_ns"`
}

type Config struct {
	ConfigVersion        string          `json:"config_version"`
	NodeID               string          `json:"node_id"`
	Topology             string          `json:"topology"`
	DataDir              string          `json:"data_dir"`
	NumShards            int             `json:"num_shards"`
	PartitionGranularity string          `json:"partition_granularity"` // "hour" | "day"
	WAL                  WALConfig       `json:"wal"`
	Storage              StorageConfig   `json:"storage"`
	Ingest               IngestConfig    `json:"ingest"`
	Query                QueryConfig     `json:"query"`
	Generator            GeneratorConfig `json:"generator"`
}

func Default() Config {
	return Config{
		ConfigVersion:        SupportedConfigVersion,
		NodeID:               "node-local-01",
		Topology:             "single-node-multi-shard",
		DataDir:              "./data/runtime",
		NumShards:            4,
		PartitionGranularity: "hour",
		WAL: WALConfig{
			SyncMode:                      "fsync_per_batch",
			MaxSegmentBytes:               64 * 1024 * 1024,
			TruncateCorruptTailOnRecovery: true,
		},
		Storage: StorageConfig{
			MemtableMaxEvents: 16384,
			Compression:       "delta_dict_gzip",
			EnableChecksums:   true,
		},
		Ingest: IngestConfig{
			MaxBatchSize:               10000,
			QueueCapacityBatches:       64,
			BackpressureTimeoutMS:      250,
			LateEventPolicy:            "accept_and_flag",
			LateEventMaxWatermarkLagMS: 3600000,
		},
		Query: QueryConfig{
			MaxRangeDurationHours: 24,
			MaxLimitRows:          100000,
			MaxScanPartitions:     64,
			DefaultLimitRows:      10000,
			TimeoutMS:             5000,
		},
		Generator: GeneratorConfig{
			Seed:          42,
			Instruments:   []string{"AAPL", "MSFT", "NVDA", "SPY", "ESZ6", "BTC-USD", "ETH-USD", "EURUSD"},
			SourceID:      "sim-feed-a",
			FeedSessionID: "2026-11-28-session-01",
			StartTimeNS:   1764320400000000000,
		},
	}
}

func Load(path string) (Config, error) {
	if path == "" {
		return Config{}, &ConfigError{
			Code:    "ERR_CONFIG_PATH_EMPTY",
			Field:   "config_path",
			Message: "configuration file path must not be empty",
		}
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Config{}, &ConfigError{
				Code:    "ERR_CONFIG_NOT_FOUND",
				Field:   "config_path",
				Message: fmt.Sprintf("configuration file does not exist: %s", path),
			}
		}
		return Config{}, &ConfigError{
			Code:    "ERR_CONFIG_READ_FAILED",
			Field:   "config_path",
			Message: err.Error(),
		}
	}
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return Config{}, &ConfigError{
			Code:    "ERR_CONFIG_PARSE_FAILED",
			Message: fmt.Sprintf("invalid JSON in configuration %s: %v", path, err),
		}
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) Validate() error {
	if c.ConfigVersion != SupportedConfigVersion {
		return &ConfigError{
			Code:    "ERR_CONFIG_UNSUPPORTED_VERSION",
			Field:   "config_version",
			Message: fmt.Sprintf("got %q, expected %q", c.ConfigVersion, SupportedConfigVersion),
		}
	}
	if c.NodeID == "" {
		return &ConfigError{Code: "ERR_CONFIG_INVALID", Field: "node_id", Message: "must not be empty"}
	}
	if c.DataDir == "" {
		return &ConfigError{Code: "ERR_CONFIG_INVALID", Field: "data_dir", Message: "must not be empty"}
	}
	if c.NumShards < 1 || c.NumShards > 1024 {
		return &ConfigError{Code: "ERR_CONFIG_INVALID", Field: "num_shards", Message: "must be in [1, 1024]"}
	}
	if c.PartitionGranularity != "hour" && c.PartitionGranularity != "day" {
		return &ConfigError{Code: "ERR_CONFIG_INVALID", Field: "partition_granularity", Message: "must be 'hour' or 'day'"}
	}
	if c.WAL.SyncMode != "fsync_per_batch" && c.WAL.SyncMode != "os_buffered" {
		return &ConfigError{Code: "ERR_CONFIG_INVALID", Field: "wal.sync_mode", Message: "must be 'fsync_per_batch' or 'os_buffered'"}
	}
	if c.WAL.MaxSegmentBytes < 1024 {
		return &ConfigError{Code: "ERR_CONFIG_INVALID", Field: "wal.max_segment_bytes", Message: "must be >= 1024"}
	}
	if c.Storage.MemtableMaxEvents < 1 {
		return &ConfigError{Code: "ERR_CONFIG_INVALID", Field: "storage.memtable_max_events", Message: "must be >= 1"}
	}
	if c.Storage.Compression != "delta_dict_gzip" && c.Storage.Compression != "delta_dict_raw" {
		return &ConfigError{Code: "ERR_CONFIG_INVALID", Field: "storage.compression", Message: "must be 'delta_dict_gzip' or 'delta_dict_raw'"}
	}
	if c.Ingest.MaxBatchSize < 1 {
		return &ConfigError{Code: "ERR_CONFIG_INVALID", Field: "ingest.max_batch_size", Message: "must be >= 1"}
	}
	if c.Ingest.QueueCapacityBatches < 1 {
		return &ConfigError{Code: "ERR_CONFIG_INVALID", Field: "ingest.queue_capacity_batches", Message: "must be >= 1"}
	}
	if c.Ingest.BackpressureTimeoutMS < 1 {
		return &ConfigError{Code: "ERR_CONFIG_INVALID", Field: "ingest.backpressure_timeout_ms", Message: "must be >= 1"}
	}
	if c.Ingest.LateEventPolicy != "accept_and_flag" && c.Ingest.LateEventPolicy != "reject_too_late" {
		return &ConfigError{Code: "ERR_CONFIG_INVALID", Field: "ingest.late_event_policy", Message: "must be 'accept_and_flag' or 'reject_too_late'"}
	}
	if c.Ingest.LateEventMaxWatermarkLagMS < 0 {
		return &ConfigError{Code: "ERR_CONFIG_INVALID", Field: "ingest.late_event_max_watermark_lag_ms", Message: "must be >= 0"}
	}
	if c.Query.MaxRangeDurationHours < 1 {
		return &ConfigError{Code: "ERR_CONFIG_INVALID", Field: "query.max_range_duration_hours", Message: "must be >= 1"}
	}
	if c.Query.MaxLimitRows < 1 {
		return &ConfigError{Code: "ERR_CONFIG_INVALID", Field: "query.max_limit_rows", Message: "must be >= 1"}
	}
	if c.Query.MaxScanPartitions < 1 {
		return &ConfigError{Code: "ERR_CONFIG_INVALID", Field: "query.max_scan_partitions", Message: "must be >= 1"}
	}
	if c.Query.DefaultLimitRows < 1 || c.Query.DefaultLimitRows > c.Query.MaxLimitRows {
		return &ConfigError{Code: "ERR_CONFIG_INVALID", Field: "query.default_limit_rows", Message: "must be in [1, query.max_limit_rows]"}
	}
	if c.Query.TimeoutMS < 1 {
		return &ConfigError{Code: "ERR_CONFIG_INVALID", Field: "query.timeout_ms", Message: "must be >= 1"}
	}
	return nil
}
