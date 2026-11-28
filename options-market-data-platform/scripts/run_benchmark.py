#!/usr/bin/env python3
"""Deterministic synthetic benchmark measuring ingest, IV derivation, query, and replay throughput.

Writes measured local single-node results to reports/benchmark_results.json.
"""

from __future__ import annotations

import json
import platform as py_platform
import random
import sys
import tempfile
import time
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from omdp.config import StorageConfig, load_config
from omdp.domain.contracts import (
    CanonicalOptionID,
    ContractMetadata,
    ExpiryCalendarEntry,
    OptionQuote,
    OptionRight,
    RateCurvePoint,
    UnderlyingQuote,
)
from omdp.domain.pricing import bs_price_and_greeks
from omdp.service.platform import OptionsMarketDataPlatform


def run_benchmark(num_snapshots: int = 20, strikes_per_expiry: int = 25) -> dict:
    cfg = load_config(ROOT / "config" / "default_config.json")
    rng = random.Random(cfg.random_seed)

    base_time = datetime(2026, 6, 19, 14, 0, 0, tzinfo=timezone.utc)
    expiries = [date(2026, 9, 18), date(2026, 12, 18)]
    strikes = [Decimal(str(80 + 2 * i)) for i in range(strikes_per_expiry)]

    contracts = []
    for exp in expiries:
        for k in strikes:
            for right in (OptionRight.CALL, OptionRight.PUT):
                cid = CanonicalOptionID("BENCH", exp, right, k)
                contracts.append(
                    ContractMetadata(
                        option_id=cid.canonical,
                        contract_version=1,
                        underlying_symbol="BENCH",
                        expiry_date=exp,
                        option_right=right,
                        strike=k,
                        strike_scale_decimals=2,
                        contract_multiplier=100,
                        exercise_style="EUROPEAN",
                        currency="USD",
                        source_id="synthetic_bench",
                        schema_version="1.0.0",
                        ingestion_time=base_time - timedelta(hours=1),
                    )
                )

    calendars = [
        ExpiryCalendarEntry(
            calendar_id="US_OPT_BENCH",
            calendar_version=1,
            expiry_date=exp,
            expiry_timestamp_utc=datetime(exp.year, exp.month, exp.day, 20, 0, 0, tzinfo=timezone.utc),
            is_trading_day=True,
            settlement_type="PM",
            source_id="synthetic_bench",
            schema_version="1.0.0",
        )
        for exp in expiries
    ]

    rates = [
        RateCurvePoint(
            curve_id="USD_OIS_BENCH",
            curve_version=1,
            as_of_time=base_time - timedelta(minutes=30),
            tenor_days=90,
            risk_free_rate=0.045,
            dividend_yield=0.01,
            source_id="synthetic_bench",
            schema_version="1.0.0",
        ),
        RateCurvePoint(
            curve_id="USD_OIS_BENCH",
            curve_version=1,
            as_of_time=base_time - timedelta(minutes=30),
            tenor_days=365,
            risk_free_rate=0.048,
            dividend_yield=0.01,
            source_id="synthetic_bench",
            schema_version="1.0.0",
        ),
    ]

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "bench.sqlite3"
        bench_cfg = replace(cfg, storage=StorageConfig(backend="sqlite", sqlite_path=str(db_path)))
        platform = OptionsMarketDataPlatform(config=bench_cfg)
        try:
            platform.register_contracts(contracts)
            platform.register_calendars(calendars)
            platform.register_rate_curves(rates)

            # Pre-generate deterministic synthetic snapshots (outside timed ingest loop)
            batches = []
            seq = 1
            for s_idx in range(num_snapshots):
                snap_time = base_time + timedelta(minutes=s_idx)
                spot = 100.0 + 0.25 * (s_idx - num_snapshots / 2.0)
                uq = UnderlyingQuote(
                    underlying_symbol="BENCH",
                    quote_version=1,
                    event_time=snap_time - timedelta(seconds=1),
                    ingestion_time=snap_time,
                    bid=Decimal(f"{spot - 0.02:.2f}"),
                    ask=Decimal(f"{spot + 0.02:.2f}"),
                    last_price=Decimal(f"{spot:.2f}"),
                    source_id="synthetic_bench",
                    source_sequence=seq,
                    schema_version="1.0.0",
                )
                seq += 1
                quotes = []
                snap_id = f"BENCH-{snap_time.strftime('%Y%m%dT%H%M%SZ')}"
                for c in contracts:
                    exp_ts = datetime(c.expiry_date.year, c.expiry_date.month, c.expiry_date.day, 20, 0, 0, tzinfo=timezone.utc)
                    tte = (exp_ts - snap_time).total_seconds() / (365.0 * 86400.0)
                    # Mild deterministic vol smile
                    moneyness_offset = (float(c.strike) - spot) / spot
                    vol = max(0.10, 0.22 + 0.15 * moneyness_offset * moneyness_offset - 0.05 * moneyness_offset + rng.uniform(-0.002, 0.002))
                    px = bs_price_and_greeks(spot, float(c.strike), tte, 0.0465, 0.01, vol, c.option_right).price
                    half_spread = max(0.01, round(px * 0.005, 4))
                    bid = max(0.01, px - half_spread)
                    ask = px + half_spread
                    quotes.append(
                        OptionQuote(
                            snapshot_id=snap_id,
                            option_id=c.option_id,
                            quote_version=1,
                            underlying_symbol="BENCH",
                            reported_strike=c.strike,
                            event_time=snap_time - timedelta(seconds=1),
                            ingestion_time=snap_time + timedelta(seconds=1),
                            bid=Decimal(f"{bid:.4f}"),
                            ask=Decimal(f"{ask:.4f}"),
                            bid_size=50,
                            ask_size=50,
                            source_id="synthetic_bench",
                            source_sequence=seq,
                            schema_version="1.0.0",
                        )
                    )
                    seq += 1
                batches.append((snap_id, snap_time, uq, quotes))

            # Time ingestion + validation + IV derivation + SQLite persistence
            t0 = time.perf_counter()
            for snap_id, snap_time, uq, quotes in batches:
                platform.ingest_underlying_quotes([uq])
                platform.ingest_chain_snapshot(
                    snapshot_id=snap_id,
                    underlying_symbol="BENCH",
                    snapshot_time=snap_time,
                    ingestion_time=snap_time + timedelta(seconds=2),
                    source_id="synthetic_bench",
                    schema_version="1.0.0",
                    quotes=quotes,
                )
            ingest_elapsed = time.perf_counter() - t0

            # Time historical range query
            t1 = time.perf_counter()
            queried = platform.query_historical_chains(underlying_symbol="BENCH")
            query_elapsed = time.perf_counter() - t1

            # Time deterministic replay of all snapshots
            t2 = time.perf_counter()
            replays = platform.replay_range(underlying_symbol="BENCH")
            replay_elapsed = time.perf_counter() - t2

            total_quotes = len(batches) * len(contracts)
            all_replays_identical = all(r.is_identical for r in replays)
            db_size_bytes = db_path.stat().st_size

            return {
                "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "environment": {
                    "topology": "single_node_local_sqlite",
                    "python_version": py_platform.python_version(),
                    "system": py_platform.system(),
                    "machine": py_platform.machine(),
                },
                "workload": {
                    "synthetic_fixture": True,
                    "random_seed": cfg.random_seed,
                    "num_snapshots": num_snapshots,
                    "contracts_per_snapshot": len(contracts),
                    "total_quotes": total_quotes,
                },
                "measured_metrics": {
                    "ingest_and_derive_seconds": round(ingest_elapsed, 4),
                    "ingest_quotes_per_second": round(total_quotes / ingest_elapsed, 2),
                    "historical_query_seconds": round(query_elapsed, 4),
                    "snapshots_returned": len(queried),
                    "replay_all_seconds": round(replay_elapsed, 4),
                    "replay_quotes_per_second": round(total_quotes / replay_elapsed, 2),
                    "all_replays_identical": all_replays_identical,
                    "sqlite_db_size_bytes": db_size_bytes,
                },
                "counters": platform.counters.to_dict(),
                "config_hash": cfg.config_hash(),
            }
        finally:
            platform.close()


def main() -> int:
    results = run_benchmark()
    out_path = ROOT / "reports" / "benchmark_results.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(results, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    m = results["measured_metrics"]
    print(
        f"Benchmark complete: {results['workload']['total_quotes']} quotes across "
        f"{results['workload']['num_snapshots']} snapshots in {m['ingest_and_derive_seconds']}s "
        f"({m['ingest_quotes_per_second']} quotes/s), replay_identical={m['all_replays_identical']}"
    )
    return 0 if m["all_replays_identical"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
