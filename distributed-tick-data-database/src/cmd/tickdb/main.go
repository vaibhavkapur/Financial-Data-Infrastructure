package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"time"

	"distributed-tick-data-database/src/codec"
	"distributed-tick-data-database/src/config"
	"distributed-tick-data-database/src/domain"
	"distributed-tick-data-database/src/ingest"
	"distributed-tick-data-database/src/query"
	"distributed-tick-data-database/src/server"
	"distributed-tick-data-database/src/storage"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(2)
	}
	sub := os.Args[1]
	args := os.Args[2:]

	var err error
	switch sub {
	case "demo":
		err = cmdDemo(args)
	case "benchmark":
		err = cmdBenchmark(args)
	case "generate":
		err = cmdGenerate(args)
	case "ingest":
		err = cmdIngest(args)
	case "query":
		err = cmdQuery(args)
	case "replay":
		err = cmdReplay(args)
	case "gaps":
		err = cmdGaps(args)
	case "serve":
		err = cmdServe(args)
	default:
		printUsage()
		os.Exit(2)
	}

	if err != nil {
		enc, _ := json.Marshal(map[string]any{
			"level":   "error",
			"command": sub,
			"error":   err.Error(),
		})
		fmt.Fprintln(os.Stderr, string(enc))
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintf(os.Stderr, `tickdb — Distributed Tick Data Database prototype CLI

Usage:
  tickdb <command> [flags]

Commands:
  demo        Run end-to-end reproducible demo and oracle comparison
  benchmark   Run ingestion, disk/compression, query p99, and WAL recovery benchmarks
  generate    Generate deterministic synthetic tick JSONL stream
  ingest      Ingest a JSONL file into local sharded storage
  query       Execute a bounded historical range query
  replay      Export bounded replay to jsonl, csv, or parquet
  gaps        Report open sequence gaps and stream completeness metrics
  serve       Start HTTP API server (/health, /metrics, /v1/ingest, /v1/query, /v1/gaps)
`)
}

type RuntimeMetadata struct {
	RecordedAtUTC string `json:"recorded_at_utc"`
	GoVersion     string `json:"go_version"`
	GOOS          string `json:"goos"`
	GOARCH        string `json:"goarch"`
	NumCPU        int    `json:"num_cpu"`
	Topology      string `json:"topology"`
	NodeID        string `json:"node_id"`
	NumShards     int    `json:"num_shards"`
	ConfigVersion string `json:"config_version"`
	Seed          int64  `json:"seed"`
}

func runtimeMeta(cfg config.Config) RuntimeMetadata {
	return RuntimeMetadata{
		RecordedAtUTC: time.Now().UTC().Format(time.RFC3339),
		GoVersion:     runtime.Version(),
		GOOS:          runtime.GOOS,
		GOARCH:        runtime.GOARCH,
		NumCPU:        runtime.NumCPU(),
		Topology:      cfg.Topology,
		NodeID:        cfg.NodeID,
		NumShards:     cfg.NumShards,
		ConfigVersion: cfg.ConfigVersion,
		Seed:          cfg.Generator.Seed,
	}
}

func cmdDemo(args []string) error {
	fs := flag.NewFlagSet("demo", flag.ContinueOnError)
	cfgPath := fs.String("config", "config/default.json", "Path to config JSON")
	fixturePath := fs.String("fixture", "data/fixtures/valid_reference_ticks.jsonl", "Reference fixture JSONL")
	reportPath := fs.String("report", "reports/demo_replay_comparison.json", "Path to write raw demo comparison report")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	demoDir, err := os.MkdirTemp("", "tickdb-demo-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(demoDir)

	// Part 1: Hand-calculated reference fixture vs ReferenceOracle
	refCfg := cfg
	refCfg.DataDir = filepath.Join(demoDir, "ref_run")
	refTicks, parseErrs, err := ingest.ReadJSONLFile(*fixturePath)
	if err != nil {
		return err
	}
	if len(parseErrs) > 0 {
		return fmt.Errorf("unexpected parse errors in reference fixture: %v", parseErrs)
	}

	part := domain.NewPartitioner(refCfg.NumShards, refCfg.PartitionGranularity)
	oracle := domain.NewReferenceOracle(domain.SequencerConfig{
		LateEventPolicy:   refCfg.Ingest.LateEventPolicy,
		MaxWatermarkLagNS: refCfg.Ingest.LateEventMaxWatermarkLagMS * int64(time.Millisecond),
	}, part)
	refIngestTime := int64(1764324001000000000)
	oracleRes := oracle.Ingest("ref-batch-01", refTicks, refIngestTime)

	eng, err := storage.OpenEngine(refCfg)
	if err != nil {
		return err
	}
	pipe := ingest.NewPipeline(refCfg.Ingest, eng, 1)
	engRes, err := pipe.Submit(context.Background(), "ref-batch-01", refTicks, refIngestTime)
	if err != nil {
		pipe.Close()
		_ = eng.Close(false)
		return err
	}
	pipe.Close()
	if err := eng.FlushAll(); err != nil {
		_ = eng.Close(false)
		return err
	}
	qsvc := query.NewService(refCfg.Query, eng)
	qResp, err := qsvc.QueryRange(context.Background(), query.RangeRequest{
		StartTimeNS: 1764320400000000000,
		EndTimeNS:   1764324000000000000,
		Limit:       1000,
	})
	if err != nil {
		_ = eng.Close(false)
		return err
	}

	oracleRows := oracle.AllOrdered()
	oracleDigest := domain.LogicalDigest(oracleRows)
	refMatch := oracleDigest == qResp.LogicalSHA256 && len(oracleRows) == qResp.RowCount
	refGaps := eng.OpenGaps()
	refComp := eng.Completeness()
	_ = eng.Close(true)

	// Part 2: 5,000-tick deterministic synthetic stream with duplicates, gaps, late arrivals + restart + Parquet export
	synCfg := cfg
	synCfg.DataDir = filepath.Join(demoDir, "syn_run")
	synCfg.Storage.MemtableMaxEvents = 1000 // force multiple segment flushes

	rawStream := ingest.GenerateSyntheticStream(ingest.SyntheticOptions{
		Seed:            synCfg.Generator.Seed,
		Instruments:     synCfg.Generator.Instruments,
		SourceID:        synCfg.Generator.SourceID,
		FeedSessionID:   synCfg.Generator.FeedSessionID,
		StartTimeNS:     synCfg.Generator.StartTimeNS,
		Count:           5000,
		TickIntervalNS:  1_000_000, // 1ms
		DuplicateRate:   0.03,
		GapRate:         0.005,
		LateReorderRate: 0.04,
	})

	synOracle := domain.NewReferenceOracle(domain.SequencerConfig{
		LateEventPolicy:   synCfg.Ingest.LateEventPolicy,
		MaxWatermarkLagNS: synCfg.Ingest.LateEventMaxWatermarkLagMS * int64(time.Millisecond),
	}, part)
	synEng, err := storage.OpenEngine(synCfg)
	if err != nil {
		return err
	}
	synPipe := ingest.NewPipeline(synCfg.Ingest, synEng, 1)

	batchSize := 500
	for i := 0; i < len(rawStream); i += batchSize {
		end := i + batchSize
		if end > len(rawStream) {
			end = len(rawStream)
		}
		batchID := fmt.Sprintf("demo-batch-%04d", i/batchSize+1)
		ingestTS := synCfg.Generator.StartTimeNS + int64(end)*1_000_000 + 5_000_000
		synOracle.Ingest(batchID, rawStream[i:end], ingestTS)
		if _, err := synPipe.Submit(context.Background(), batchID, rawStream[i:end], ingestTS); err != nil {
			synPipe.Close()
			_ = synEng.Close(false)
			return err
		}
	}
	synPipe.Close()

	// Simulate abrupt restart WITHOUT flushing trailing memtable -> verify WAL recovery!
	if err := synEng.Close(false); err != nil {
		return err
	}
	synEng, err = storage.OpenEngine(synCfg)
	if err != nil {
		return err
	}
	defer synEng.Close(true)

	recoveryReports := synEng.RecoveryReports()
	synQ := query.NewService(synCfg.Query, synEng)

	// Export full range to JSONL, CSV, and Parquet
	minTS := rawStream[0].EventTimeNS - 1_000_000_000
	maxTS := rawStream[len(rawStream)-1].EventTimeNS + 1_000_000_000
	rangeReq := query.RangeRequest{
		StartTimeNS: minTS,
		EndTimeNS:   maxTS,
		Limit:       100000,
	}

	jsonlReceipt, err := synQ.ExportReplay(context.Background(), rangeReq, "jsonl", filepath.Join(demoDir, "replay.jsonl"))
	if err != nil {
		return err
	}
	csvReceipt, err := synQ.ExportReplay(context.Background(), rangeReq, "csv", filepath.Join(demoDir, "replay.csv"))
	if err != nil {
		return err
	}
	parquetPath := filepath.Join(demoDir, "replay.parquet")
	pqReceipt, err := synQ.ExportReplay(context.Background(), rangeReq, "parquet", parquetPath)
	if err != nil {
		return err
	}

	// Verify Parquet round-trip equals oracle
	pqBytes, err := os.ReadFile(parquetPath)
	if err != nil {
		return err
	}
	pqDecoded, err := codec.ReadParquet(pqBytes)
	if err != nil {
		return err
	}
	synOracleRows := synOracle.AllOrdered()
	synOracleDigest := domain.LogicalDigest(synOracleRows)
	pqDigest := domain.LogicalDigest(pqDecoded)
	synMatch := synOracleDigest == jsonlReceipt.LogicalSHA256 &&
		synOracleDigest == csvReceipt.LogicalSHA256 &&
		synOracleDigest == pqReceipt.LogicalSHA256 &&
		synOracleDigest == pqDigest &&
		len(synOracleRows) == len(pqDecoded)

	report := map[string]any{
		"report_version": "1.0.0",
		"runtime":        runtimeMeta(cfg),
		"reference_fixture": map[string]any{
			"fixture_path":       *fixturePath,
			"input_events":       len(refTicks),
			"oracle_accepted":    oracleRes.AcceptedCount,
			"engine_accepted":    engRes.AcceptedCount,
			"engine_duplicates":  engRes.DuplicateCount,
			"engine_late":        engRes.LateAcceptedCount,
			"oracle_sha256":      oracleDigest,
			"engine_sha256":      qResp.LogicalSHA256,
			"exact_match":        refMatch,
			"open_gaps":          refGaps,
			"completeness":       refComp,
		},
		"synthetic_demo_with_restart": map[string]any{
			"raw_input_events":    len(rawStream),
			"oracle_unique_rows":  len(synOracleRows),
			"oracle_sha256":       synOracleDigest,
			"parquet_roundtrip_sha256": pqDigest,
			"exact_match_all_formats":  synMatch,
			"exports": map[string]any{
				"jsonl":   jsonlReceipt,
				"csv":     csvReceipt,
				"parquet": pqReceipt,
			},
			"storage_stats":    synEng.StorageStats(),
			"recovery_reports": recoveryReports,
			"open_gap_count":   len(synEng.OpenGaps()),
			"completeness":     synEng.Completeness(),
		},
	}

	if err := writePrettyJSON(*reportPath, report); err != nil {
		return err
	}
	if !refMatch || !synMatch {
		return fmt.Errorf("ERR_DEMO_MISMATCH: reference_match=%v synthetic_match=%v", refMatch, synMatch)
	}

	enc, _ := json.MarshalIndent(report, "", "  ")
	fmt.Println(string(enc))
	return nil
}

func cmdBenchmark(args []string) error {
	fs := flag.NewFlagSet("benchmark", flag.ContinueOnError)
	cfgPath := fs.String("config", "config/benchmark.json", "Path to benchmark config JSON")
	numEvents := fs.Int("events", 100000, "Total synthetic ticks to ingest")
	batchSize := fs.Int("batch-size", 2000, "Ticks per ingestion batch")
	numQueries := fs.Int("queries", 300, "Number of bounded range queries to sample for latency percentiles")
	outputPath := fs.String("output", "reports/benchmark_results.json", "Path to write raw benchmark JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	benchDir, err := os.MkdirTemp("", "tickdb-bench-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(benchDir)
	cfg.DataDir = filepath.Join(benchDir, "storage")

	// Generate deterministic dataset
	genStart := time.Now()
	rawTicks := ingest.GenerateSyntheticStream(ingest.SyntheticOptions{
		Seed:            cfg.Generator.Seed,
		Instruments:     cfg.Generator.Instruments,
		SourceID:        cfg.Generator.SourceID,
		FeedSessionID:   cfg.Generator.FeedSessionID,
		StartTimeNS:     cfg.Generator.StartTimeNS,
		Count:           *numEvents,
		TickIntervalNS:  100_000, // 100us -> 100k ticks span ~10-15s, let's span across multiple hours!
		DuplicateRate:   0.01,
		GapRate:         0.001,
		LateReorderRate: 0.02,
	})
	// Re-generate with 150ms interval so 100k ticks span ~4-5 UTC hour partitions (testing real partition pruning!)
	rawTicks = ingest.GenerateSyntheticStream(ingest.SyntheticOptions{
		Seed:            cfg.Generator.Seed,
		Instruments:     cfg.Generator.Instruments,
		SourceID:        cfg.Generator.SourceID,
		FeedSessionID:   cfg.Generator.FeedSessionID,
		StartTimeNS:     cfg.Generator.StartTimeNS,
		Count:           *numEvents,
		TickIntervalNS:  150_000_000, // 150ms -> 100k ticks span ~5.2 hours
		DuplicateRate:   0.01,
		GapRate:         0.001,
		LateReorderRate: 0.02,
	})
	genMS := float64(time.Since(genStart).Microseconds()) / 1000.0

	// Measure baseline raw JSONL size for compression ratio comparison
	rawJSONLPath := filepath.Join(benchDir, "baseline_raw.jsonl")
	if err := ingest.WriteRawJSONL(rawJSONLPath, rawTicks); err != nil {
		return err
	}
	rawStat, err := os.Stat(rawJSONLPath)
	if err != nil {
		return err
	}
	rawJSONLBytes := rawStat.Size()

	// 1. Ingestion benchmark
	eng, err := storage.OpenEngine(cfg)
	if err != nil {
		return err
	}
	pipe := ingest.NewPipeline(cfg.Ingest, eng, 1)

	ingStart := time.Now()
	totalAccepted := 0
	totalDups := 0
	totalLate := 0
	batches := 0
	for i := 0; i < len(rawTicks); i += *batchSize {
		end := i + *batchSize
		if end > len(rawTicks) {
			end = len(rawTicks)
		}
		batches++
		res, err := pipe.Submit(context.Background(), fmt.Sprintf("bench-%05d", batches), rawTicks[i:end], time.Now().UTC().UnixNano())
		if err != nil {
			pipe.Close()
			_ = eng.Close(false)
			return err
		}
		totalAccepted += res.AcceptedCount
		totalDups += res.DuplicateCount
		totalLate += res.LateAcceptedCount
	}
	pipe.Close()
	ingDur := time.Since(ingStart)
	if err := eng.FlushAll(); err != nil {
		_ = eng.Close(false)
		return err
	}
	ingWithFlushDur := time.Since(ingStart)

	diskStats := eng.StorageStats()
	compressionRatioVsJSONL := 0.0
	if diskStats.TotalSegmentBytes > 0 {
		compressionRatioVsJSONL = float64(rawJSONLBytes) / float64(diskStats.TotalSegmentBytes)
	}
	bytesPerTick := 0.0
	if totalAccepted > 0 {
		bytesPerTick = float64(diskStats.TotalSegmentBytes) / float64(totalAccepted)
	}

	// Measure Parquet export size on a 20,000-tick window
	qsvc := query.NewService(cfg.Query, eng)
	minTS := rawTicks[0].EventTimeNS
	maxTS := rawTicks[len(rawTicks)-1].EventTimeNS
	pqReceipt, err := qsvc.ExportReplay(context.Background(), query.RangeRequest{
		StartTimeNS: minTS,
		EndTimeNS:   minTS + int64(2*time.Hour),
		Limit:       20000,
	}, "parquet", filepath.Join(benchDir, "sample.parquet"))
	if err != nil {
		_ = eng.Close(false)
		return err
	}

	// 2. Bounded historical range query latency benchmark (single-instrument 5-minute windows)
	rng := rand.New(rand.NewSource(cfg.Generator.Seed + 99))
	spanNS := maxTS - minTS
	winNS := int64(5 * time.Minute)
	latenciesMS := make([]float64, 0, *numQueries)
	totalRowsReturned := 0
	totalSegsScanned := 0
	totalSegsPruned := 0

	for i := 0; i < *numQueries; i++ {
		inst := cfg.Generator.Instruments[rng.Intn(len(cfg.Generator.Instruments))]
		offset := int64(0)
		if spanNS > winNS {
			offset = rng.Int63n(spanNS - winNS)
		}
		qStart := minTS + offset
		qEnd := qStart + winNS

		t0 := time.Now()
		resp, err := qsvc.QueryRange(context.Background(), query.RangeRequest{
			InstrumentID: inst,
			StartTimeNS:  qStart,
			EndTimeNS:    qEnd,
			Limit:        10000,
		})
		if err != nil {
			_ = eng.Close(false)
			return err
		}
		latenciesMS = append(latenciesMS, float64(time.Since(t0).Microseconds())/1000.0)
		totalRowsReturned += resp.RowCount
		totalSegsScanned += resp.SegmentsScanned
		totalSegsPruned += resp.SegmentsPruned
	}
	sort.Float64s(latenciesMS)

	// 3. Recovery under load benchmark: write 10,000 additional ticks into WAL (unflushed),
	// append a simulated partial corrupt WAL tail to shard 0, close without flush, and measure recovery.
	recTicks := ingest.GenerateSyntheticStream(ingest.SyntheticOptions{
		Seed:          cfg.Generator.Seed + 777,
		Instruments:   cfg.Generator.Instruments,
		SourceID:      "sim-recovery-feed",
		FeedSessionID: "rec-session-01",
		StartTimeNS:   maxTS + int64(time.Minute),
		Count:         10000,
		TickIntervalNS: 1_000_000,
	})
	for i := 0; i < len(recTicks); i += 1000 {
		end := i + 1000
		if end > len(recTicks) {
			end = len(recTicks)
		}
		if _, err := eng.IngestBatch(fmt.Sprintf("rec-wal-%03d", i/1000+1), recTicks[i:end], time.Now().UTC().UnixNano()); err != nil {
			_ = eng.Close(false)
			return err
		}
	}
	walPathShard0 := eng.Shard(0).WALPath()
	// Close WITHOUT flushing so all 10,000 ticks reside in WAL
	if err := eng.Close(false); err != nil {
		return err
	}
	// Inject 37 truncated partial-write bytes at the tail of shard 0 WAL
	wf, err := os.OpenFile(walPathShard0, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	_, _ = wf.Write([]byte("TWAL\xff\x00\x00\x00\x12\x34\x56\x78partial_uncommitted_batch_tail"))
	_ = wf.Sync()
	_ = wf.Close()

	recStart := time.Now()
	engRecovered, err := storage.OpenEngine(cfg)
	if err != nil {
		return err
	}
	recDurMS := float64(time.Since(recStart).Microseconds()) / 1000.0
	recReports := engRecovered.RecoveryReports()

	// Verify zero accepted recovery ticks were lost
	recQ := query.NewService(cfg.Query, engRecovered)
	recResp, err := recQ.QueryRange(context.Background(), query.RangeRequest{
		StartTimeNS: maxTS + int64(time.Minute),
		EndTimeNS:   maxTS + int64(time.Hour),
		Limit:       20000,
	})
	if err != nil {
		_ = engRecovered.Close(false)
		return err
	}
	_ = engRecovered.Close(true)

	pruningRatio := 0.0
	if totalSegsScanned+totalSegsPruned > 0 {
		pruningRatio = float64(totalSegsPruned) / float64(totalSegsScanned+totalSegsPruned)
	}

	result := map[string]any{
		"report_version": "1.0.0",
		"disclaimer":     "Measured locally on single-node multi-shard topology; single-node tests are not distributed multi-host scalability evidence.",
		"runtime":        runtimeMeta(cfg),
		"workload": map[string]any{
			"total_raw_events":       len(rawTicks),
			"accepted_unique_events": totalAccepted,
			"duplicate_events":       totalDups,
			"late_events_accepted":   totalLate,
			"batch_size":             *batchSize,
			"batches_committed":      batches,
			"wal_sync_mode":          cfg.WAL.SyncMode,
			"compression":            cfg.Storage.Compression,
			"generation_ms":          genMS,
		},
		"ingestion": map[string]any{
			"ingest_duration_ms":                  float64(ingDur.Microseconds()) / 1000.0,
			"ingest_plus_final_flush_duration_ms": float64(ingWithFlushDur.Microseconds()) / 1000.0,
			"throughput_raw_ticks_per_sec":        float64(len(rawTicks)) / ingDur.Seconds(),
			"throughput_accepted_ticks_per_sec":   float64(totalAccepted) / ingDur.Seconds(),
			"throughput_including_flush_tps":      float64(totalAccepted) / ingWithFlushDur.Seconds(),
		},
		"storage_and_compression": map[string]any{
			"raw_jsonl_bytes":            rawJSONLBytes,
			"uncompressed_columnar_bytes": diskStats.TotalUncompressedCol,
			"compressed_segment_bytes":   diskStats.TotalSegmentBytes,
			"compression_ratio_vs_jsonl": compressionRatioVsJSONL,
			"bytes_per_accepted_tick":    bytesPerTick,
			"total_segments":             diskStats.TotalSegments,
			"parquet_sample_export":      pqReceipt,
		},
		"query_latency": map[string]any{
			"queries_executed":     *numQueries,
			"window_minutes":       5,
			"total_rows_returned":  totalRowsReturned,
			"avg_rows_per_query":   float64(totalRowsReturned) / float64(*numQueries),
			"segments_scanned":     totalSegsScanned,
			"segments_pruned":      totalSegsPruned,
			"segment_pruning_ratio": pruningRatio,
			"p50_ms":               percentileF64(latenciesMS, 0.50),
			"p95_ms":               percentileF64(latenciesMS, 0.95),
			"p99_ms":               percentileF64(latenciesMS, 0.99),
			"max_ms":               latenciesMS[len(latenciesMS)-1],
		},
		"recovery_under_load": map[string]any{
			"wal_events_before_crash":     len(recTicks),
			"recovered_events_queried":    recResp.RowCount,
			"zero_accepted_ticks_lost":    recResp.RowCount == len(recTicks),
			"corrupt_tail_injected_bytes": 42,
			"total_recovery_duration_ms":  recDurMS,
			"shard_reports":               recReports,
		},
	}

	if err := writePrettyJSON(*outputPath, result); err != nil {
		return err
	}
	enc, _ := json.MarshalIndent(result, "", "  ")
	fmt.Println(string(enc))
	return nil
}

func cmdGenerate(args []string) error {
	fs := flag.NewFlagSet("generate", flag.ContinueOnError)
	cfgPath := fs.String("config", "config/default.json", "Path to config JSON")
	count := fs.Int("count", 1000, "Number of synthetic ticks")
	outPath := fs.String("out", "data/generated_ticks.jsonl", "Output JSONL path")
	dupRate := fs.Float64("dup-rate", 0.02, "Duplicate injection probability")
	gapRate := fs.Float64("gap-rate", 0.005, "Gap injection probability")
	lateRate := fs.Float64("late-rate", 0.03, "Late reorder injection probability")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	ticks := ingest.GenerateSyntheticStream(ingest.SyntheticOptions{
		Seed:            cfg.Generator.Seed,
		Instruments:     cfg.Generator.Instruments,
		SourceID:        cfg.Generator.SourceID,
		FeedSessionID:   cfg.Generator.FeedSessionID,
		StartTimeNS:     cfg.Generator.StartTimeNS,
		Count:           *count,
		TickIntervalNS:  1_000_000,
		DuplicateRate:   *dupRate,
		GapRate:         *gapRate,
		LateReorderRate: *lateRate,
	})
	if err := os.MkdirAll(filepath.Dir(*outPath), 0o755); err != nil {
		return err
	}
	if err := ingest.WriteRawJSONL(*outPath, ticks); err != nil {
		return err
	}
	out, _ := json.MarshalIndent(map[string]any{
		"output_path": *outPath,
		"tick_count":  len(ticks),
		"seed":        cfg.Generator.Seed,
	}, "", "  ")
	fmt.Println(string(out))
	return nil
}

func cmdIngest(args []string) error {
	fs := flag.NewFlagSet("ingest", flag.ContinueOnError)
	cfgPath := fs.String("config", "config/default.json", "Path to config JSON")
	inputPath := fs.String("input", "", "Input JSONL file path")
	batchSize := fs.Int("batch-size", 1000, "Batch size")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *inputPath == "" {
		return fmt.Errorf("ERR_MISSING_INPUT: --input is required")
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	ticks, parseErrs, err := ingest.ReadJSONLFile(*inputPath)
	if err != nil {
		return err
	}
	eng, err := storage.OpenEngine(cfg)
	if err != nil {
		return err
	}
	defer eng.Close(true)
	pipe := ingest.NewPipeline(cfg.Ingest, eng, 1)
	defer pipe.Close()

	var totalAcc, totalDup, totalRej, totalLate int
	for i := 0; i < len(ticks); i += *batchSize {
		end := i + *batchSize
		if end > len(ticks) {
			end = len(ticks)
		}
		res, err := pipe.Submit(context.Background(), fmt.Sprintf("cli-%05d", i / *batchSize+1), ticks[i:end], time.Now().UTC().UnixNano())
		if err != nil {
			return err
		}
		totalAcc += res.AcceptedCount
		totalDup += res.DuplicateCount
		totalRej += res.RejectedCount
		totalLate += res.LateAcceptedCount
	}
	if err := eng.FlushAll(); err != nil {
		return err
	}
	out, _ := json.MarshalIndent(map[string]any{
		"input_path":   *inputPath,
		"parsed_ticks": len(ticks),
		"parse_errors": len(parseErrs),
		"accepted":     totalAcc,
		"duplicates":   totalDup,
		"late":         totalLate,
		"rejected":     totalRej,
		"open_gaps":    eng.OpenGaps(),
		"storage":      eng.StorageStats(),
	}, "", "  ")
	fmt.Println(string(out))
	return nil
}

func cmdQuery(args []string) error {
	fs := flag.NewFlagSet("query", flag.ContinueOnError)
	cfgPath := fs.String("config", "config/default.json", "Path to config JSON")
	inst := fs.String("instrument", "", "Optional instrument ID filter")
	startNS := fs.Int64("start", 0, "Start event_time_ns (inclusive)")
	endNS := fs.Int64("end", 0, "End event_time_ns (inclusive)")
	limit := fs.Int("limit", 100, "Max rows to return")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	eng, err := storage.OpenEngine(cfg)
	if err != nil {
		return err
	}
	defer eng.Close(false)
	qsvc := query.NewService(cfg.Query, eng)
	resp, err := qsvc.QueryRange(context.Background(), query.RangeRequest{
		InstrumentID: *inst,
		StartTimeNS:  *startNS,
		EndTimeNS:    *endNS,
		Limit:        *limit,
	})
	if err != nil {
		return err
	}
	out, _ := json.MarshalIndent(resp, "", "  ")
	fmt.Println(string(out))
	return nil
}

func cmdReplay(args []string) error {
	fs := flag.NewFlagSet("replay", flag.ContinueOnError)
	cfgPath := fs.String("config", "config/default.json", "Path to config JSON")
	inst := fs.String("instrument", "", "Optional instrument ID filter")
	startNS := fs.Int64("start", 0, "Start event_time_ns (inclusive)")
	endNS := fs.Int64("end", 0, "End event_time_ns (inclusive)")
	limit := fs.Int("limit", 10000, "Max rows to export")
	format := fs.String("format", "parquet", "Export format: jsonl, csv, parquet")
	outPath := fs.String("out", "data/replay.parquet", "Output file path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	eng, err := storage.OpenEngine(cfg)
	if err != nil {
		return err
	}
	defer eng.Close(false)
	qsvc := query.NewService(cfg.Query, eng)
	receipt, err := qsvc.ExportReplay(context.Background(), query.RangeRequest{
		InstrumentID: *inst,
		StartTimeNS:  *startNS,
		EndTimeNS:    *endNS,
		Limit:        *limit,
	}, *format, *outPath)
	if err != nil {
		return err
	}
	out, _ := json.MarshalIndent(receipt, "", "  ")
	fmt.Println(string(out))
	return nil
}

func cmdGaps(args []string) error {
	fs := flag.NewFlagSet("gaps", flag.ContinueOnError)
	cfgPath := fs.String("config", "config/default.json", "Path to config JSON")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	eng, err := storage.OpenEngine(cfg)
	if err != nil {
		return err
	}
	defer eng.Close(false)
	out, _ := json.MarshalIndent(map[string]any{
		"open_gaps":    eng.OpenGaps(),
		"completeness": eng.Completeness(),
		"freshness":    eng.Freshness(),
	}, "", "  ")
	fmt.Println(string(out))
	return nil
}

func cmdServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	cfgPath := fs.String("config", "config/default.json", "Path to config JSON")
	listen := fs.String("listen", ":8081", "HTTP listen address")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	eng, err := storage.OpenEngine(cfg)
	if err != nil {
		return err
	}
	defer eng.Close(true)
	pipe := ingest.NewPipeline(cfg.Ingest, eng, 2)
	defer pipe.Close()
	qsvc := query.NewService(cfg.Query, eng)
	srv := server.New(eng, pipe, qsvc)
	fmt.Fprintf(os.Stderr, "{\"level\":\"info\",\"msg\":\"tickdb HTTP server listening\",\"addr\":%q}\n", *listen)
	return http.ListenAndServe(*listen, srv.Handler())
}

func writePrettyJSON(path string, v any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return os.WriteFile(path, b, 0o644)
}

func percentileF64(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
