#!/usr/bin/env python3
"""Deterministic synthetic benchmark measuring ingestion, as-of lookup, and replay throughput.

Records actual measured results (distinguished explicitly from synthetic fixture setup
and engineering targets) into ``reports/benchmark_results.json``.
"""

from __future__ import annotations

import json
import platform
import statistics
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from sec_master.config import PlatformConfig, StorageConfig, load_config
from sec_master.models import (
    EventEnvelope,
    EventType,
    IdentifierNamespace,
    SCHEMA_VERSION,
    make_canonical_instrument_id,
    make_canonical_listing_id,
    make_deterministic_event_id,
)
from sec_master.service import SecurityMasterService
from sec_master.validation import compute_isin_check_digit


def _generate_synthetic_universe(n_instruments: int) -> List[EventEnvelope]:
    base_valid = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    base_obs = datetime(2024, 1, 1, 6, 0, 0, tzinfo=timezone.utc)
    events: List[EventEnvelope] = []
    for i in range(n_instruments):
        iid = make_canonical_instrument_id(f"BENCH_ISSUER_{i:05d}", "EQUITY")
        lid = make_canonical_listing_id(iid, "XNYS", "USD")
        ticker = f"B{i:04d}"
        isin_body = f"US{i:09d}"
        isin = isin_body + compute_isin_check_digit(isin_body)
        figi = f"BBG{i:09d}"

        events.append(
            EventEnvelope(
                event_id=make_deterministic_event_id("EXCHANGE_OFFICIAL", f"B-INS-{i}", EventType.INSTRUMENT_UPSERT.value),
                event_type=EventType.INSTRUMENT_UPSERT,
                schema_version=SCHEMA_VERSION,
                source_system="EXCHANGE_OFFICIAL",
                source_event_id=f"B-INS-{i}",
                valid_from=base_valid,
                valid_to=None,
                observed_at=base_obs + timedelta(seconds=i),
                payload={
                    "instrument_id": iid,
                    "name": f"Benchmark Synthetic Corp {i}",
                    "asset_class": "EQUITY",
                    "issuer_name": f"Benchmark Issuer {i}",
                    "country_of_risk": "US",
                    "status": "ACTIVE",
                },
            )
        )
        events.append(
            EventEnvelope(
                event_id=make_deterministic_event_id("EXCHANGE_OFFICIAL", f"B-LST-{i}", EventType.LISTING_UPSERT.value),
                event_type=EventType.LISTING_UPSERT,
                schema_version=SCHEMA_VERSION,
                source_system="EXCHANGE_OFFICIAL",
                source_event_id=f"B-LST-{i}",
                valid_from=base_valid,
                valid_to=None,
                observed_at=base_obs + timedelta(seconds=i),
                payload={
                    "listing_id": lid,
                    "instrument_id": iid,
                    "mic": "XNYS",
                    "ticker": ticker,
                    "currency": "USD",
                    "is_primary": True,
                    "status": "ACTIVE",
                },
            )
        )
        events.append(
            EventEnvelope(
                event_id=make_deterministic_event_id("EXCHANGE_OFFICIAL", f"B-ISIN-{i}", EventType.IDENTIFIER_MAP.value),
                event_type=EventType.IDENTIFIER_MAP,
                schema_version=SCHEMA_VERSION,
                source_system="EXCHANGE_OFFICIAL",
                source_event_id=f"B-ISIN-{i}",
                valid_from=base_valid,
                valid_to=None,
                observed_at=base_obs + timedelta(seconds=i),
                payload={
                    "instrument_id": iid,
                    "listing_id": None,
                    "namespace": "ISIN",
                    "identifier_value": isin,
                    "mic": None,
                },
            )
        )
        events.append(
            EventEnvelope(
                event_id=make_deterministic_event_id("EXCHANGE_OFFICIAL", f"B-FIGI-{i}", EventType.IDENTIFIER_MAP.value),
                event_type=EventType.IDENTIFIER_MAP,
                schema_version=SCHEMA_VERSION,
                source_system="EXCHANGE_OFFICIAL",
                source_event_id=f"B-FIGI-{i}",
                valid_from=base_valid,
                valid_to=None,
                observed_at=base_obs + timedelta(seconds=i),
                payload={
                    "instrument_id": iid,
                    "listing_id": None,
                    "namespace": "OPENFIGI",
                    "identifier_value": figi,
                    "mic": None,
                },
            )
        )
    return events


def run_benchmark(n_instruments: int = 100, n_lookups: int = 200) -> Dict[str, Any]:
    base_cfg = load_config(PROJECT_ROOT / "config/default.json")
    db_path = PROJECT_ROOT / ".runtime/benchmark_security_master.db"
    if db_path.exists():
        db_path.unlink()

    mem_cfg = PlatformConfig(
        config_version=base_cfg.config_version,
        schema_version=base_cfg.schema_version,
        environment=base_cfg.environment,
        storage=StorageConfig(backend="memory", sqlite_path=":memory:"),
        licensing=base_cfg.licensing,
        conflict_resolution=base_cfg.conflict_resolution,
        quality=base_cfg.quality,
        reproducibility=base_cfg.reproducibility,
        http=base_cfg.http,
    )
    sqlite_cfg = PlatformConfig(
        config_version=base_cfg.config_version,
        schema_version=base_cfg.schema_version,
        environment=base_cfg.environment,
        storage=StorageConfig(backend="sqlite", sqlite_path=str(db_path)),
        licensing=base_cfg.licensing,
        conflict_resolution=base_cfg.conflict_resolution,
        quality=base_cfg.quality,
        reproducibility=base_cfg.reproducibility,
        http=base_cfg.http,
    )

    events = _generate_synthetic_universe(n_instruments)
    query_valid = datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
    query_obs = datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc)

    # 1. Pure domain + memory store ingest & lookup
    mem_svc = SecurityMasterService(mem_cfg)
    t0 = time.perf_counter()
    for ev in events:
        mem_svc.ingest_event(ev)
    mem_ingest_s = time.perf_counter() - t0

    lookup_latencies_ms: List[float] = []
    for k in range(n_lookups):
        idx = k % n_instruments
        q0 = time.perf_counter()
        mem_svc.lookup_as_of(
            IdentifierNamespace.MIC_TICKER,
            f"XNYS:B{idx:04d}",
            query_valid,
            query_obs,
        )
        lookup_latencies_ms.append((time.perf_counter() - q0) * 1000.0)
    mem_svc.close()

    # 2. Durable SQLite store ingest + full journal replay
    sql_svc = SecurityMasterService(sqlite_cfg)
    t1 = time.perf_counter()
    for ev in events:
        sql_svc.ingest_event(ev)
    sqlite_ingest_s = time.perf_counter() - t1

    t2 = time.perf_counter()
    replay_info = sql_svc.replay_from_journal()
    sqlite_replay_s = time.perf_counter() - t2
    sql_svc.close()

    sorted_lats = sorted(lookup_latencies_ms)
    p95_idx = min(len(sorted_lats) - 1, int(0.95 * len(sorted_lats)))

    result = {
        "report_type": "benchmark_results",
        "data_provenance": "synthetic_generated_benchmark_universe",
        "topology": "single_node_local_prototype",
        "runtime": {
            "python_version": platform.python_version(),
            "platform": platform.platform(),
            "deterministic_seed": base_cfg.reproducibility.deterministic_seed,
        },
        "workload": {
            "n_instruments": n_instruments,
            "total_events": len(events),
            "n_as_of_lookups": n_lookups,
        },
        "measured_observations": {
            "memory_ingest_seconds": round(mem_ingest_s, 4),
            "memory_ingest_events_per_sec": round(len(events) / max(mem_ingest_s, 1e-9), 2),
            "sqlite_durable_ingest_seconds": round(sqlite_ingest_s, 4),
            "sqlite_durable_ingest_events_per_sec": round(len(events) / max(sqlite_ingest_s, 1e-9), 2),
            "sqlite_journal_replay_seconds": round(sqlite_replay_s, 4),
            "sqlite_replay_events_per_sec": round(len(events) / max(sqlite_replay_s, 1e-9), 2),
            "lookup_latency_ms_median": round(statistics.median(lookup_latencies_ms), 4),
            "lookup_latency_ms_p95": round(sorted_lats[p95_idx], 4),
            "lookup_latency_ms_max": round(max(lookup_latencies_ms), 4),
            "replay_snapshot_hash": replay_info["snapshot_hash"],
        },
        "not_run": [
            "PostgreSQL 16 live multi-connection concurrency benchmark (no local PostgreSQL server in offline prototype environment)",
            "Multi-node distributed read replica failover benchmark (outside single-node prototype scope)",
        ],
    }

    report_path = PROJECT_ROOT / "reports/benchmark_results.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return result


def main() -> int:
    res = run_benchmark()
    print(json.dumps(res["measured_observations"], indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
