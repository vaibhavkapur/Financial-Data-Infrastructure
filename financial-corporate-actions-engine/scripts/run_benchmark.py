#!/usr/bin/env python3
"""Deterministic synthetic benchmark measuring ingestion, PIT query, and replay latency."""

from __future__ import annotations

import json
import platform
import random
import statistics
import sys
import time
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import List

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from corp_actions.config import EngineConfig
from corp_actions.contracts import (
    ActionType,
    CorporateActionEvent,
    PositionSnapshot,
    RawPriceBar,
    RevisionType,
)
from corp_actions.engine import CorporateActionsEngine


def _generate_synthetic_series(
    security_id: str,
    symbol: str,
    num_days: int,
    seed: int,
) -> tuple:
    rng = random.Random(seed)
    start = date(2020, 1, 2)
    bars: List[RawPriceBar] = []
    actions: List[CorporateActionEvent] = []
    price = Decimal("100.00")
    cur = start
    day_idx = 0
    action_seq = 0

    while len(bars) < num_days:
        if cur.weekday() < 5:
            # Periodic split every 250 trading days, quarterly dividend every 63 trading days
            if day_idx > 0 and day_idx % 250 == 0:
                action_seq += 1
                price = (price / Decimal("2")).quantize(Decimal("0.01"))
                et = datetime(cur.year, cur.month, cur.day, 12, 0, tzinfo=timezone.utc) - timedelta(days=2)
                actions.append(
                    CorporateActionEvent(
                        action_id=f"CA-{security_id}-S{action_seq}",
                        version=1,
                        revision_type=RevisionType.NEW,
                        action_type=ActionType.SPLIT,
                        security_id=security_id,
                        symbol=symbol,
                        announcement_date=cur - timedelta(days=5),
                        ex_date=cur,
                        ratio_numerator=Decimal("2"),
                        ratio_denominator=Decimal("1"),
                        event_time=et,
                        ingestion_time=et + timedelta(minutes=5),
                        availability_time=et + timedelta(minutes=10),
                        source="synthetic-bench",
                        lineage_id=f"BENCH-{security_id}-{action_seq}",
                    )
                )
            elif day_idx > 0 and day_idx % 63 == 0:
                action_seq += 1
                div = (price * Decimal("0.005")).quantize(Decimal("0.01"))
                if div < Decimal("0.01"):
                    div = Decimal("0.01")
                price = max(Decimal("1.00"), price - div)
                et = datetime(cur.year, cur.month, cur.day, 12, 0, tzinfo=timezone.utc) - timedelta(days=2)
                actions.append(
                    CorporateActionEvent(
                        action_id=f"CA-{security_id}-D{action_seq}",
                        version=1,
                        revision_type=RevisionType.NEW,
                        action_type=ActionType.CASH_DIVIDEND,
                        security_id=security_id,
                        symbol=symbol,
                        announcement_date=cur - timedelta(days=5),
                        ex_date=cur,
                        pay_date=cur + timedelta(days=5),
                        cash_amount=div,
                        currency="USD",
                        event_time=et,
                        ingestion_time=et + timedelta(minutes=5),
                        availability_time=et + timedelta(minutes=10),
                        source="synthetic-bench",
                        lineage_id=f"BENCH-{security_id}-{action_seq}",
                    )
                )
            else:
                shock = Decimal(str(round(1.0 + rng.uniform(-0.015, 0.016), 4)))
                price = max(Decimal("5.00"), (price * shock).quantize(Decimal("0.01")))

            et_bar = datetime(cur.year, cur.month, cur.day, 21, 0, tzinfo=timezone.utc)
            bars.append(
                RawPriceBar(
                    security_id=security_id,
                    symbol=symbol,
                    trade_date=cur,
                    open=price,
                    high=price,
                    low=price,
                    close=price,
                    volume=Decimal("100000"),
                    currency="USD",
                    event_time=et_bar,
                    ingestion_time=et_bar + timedelta(minutes=2),
                    source="synthetic-bench",
                )
            )
            day_idx += 1
        cur += timedelta(days=1)

    pos = PositionSnapshot(
        account_id="ACCT-BENCH",
        security_id=security_id,
        symbol=symbol,
        as_of_date=bars[0].trade_date,
        quantity=Decimal("100"),
        cost_basis_per_share=bars[0].close,
        currency="USD",
        cash_balance=Decimal("1000.00"),
        event_time=bars[0].event_time,
        ingestion_time=bars[0].ingestion_time,
        source="synthetic-bench",
    )
    return bars, pos, actions


def run_benchmark() -> dict:
    config = EngineConfig.from_file(PROJECT_ROOT / "config" / "engine_config.json")
    num_securities = 10
    days_per_security = 1000  # ~4 years of daily bars per security = 10,000 bars total

    all_bars = []
    all_positions = []
    all_actions = []
    for i in range(num_securities):
        bars, pos, actions = _generate_synthetic_series(
            f"SEC-BENCH-{i:02d}", f"BNC{i:02d}", days_per_security, seed=config.random_seed + i
        )
        all_bars.extend(bars)
        all_positions.append(pos)
        all_actions.extend(actions)

    engine = CorporateActionsEngine(config=config)

    t0 = time.perf_counter()
    engine.ingest_prices(all_bars)
    engine.ingest_positions(all_positions)
    engine.ingest_actions(all_actions)
    ingest_sec = time.perf_counter() - t0

    as_of = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    query_latencies_ms: List[float] = []
    all_reconciled = True
    max_discrepancy = Decimal("0")

    for i in range(num_securities):
        sec_id = f"SEC-BENCH-{i:02d}"
        tq0 = time.perf_counter()
        snap = engine.query_as_of(sec_id, as_of_time=as_of, account_id="ACCT-BENCH")
        query_latencies_ms.append((time.perf_counter() - tq0) * 1000.0)
        assert snap.reconciliation is not None
        if not snap.reconciliation.reconciled:
            all_reconciled = False
        if snap.reconciliation.return_discrepancy > max_discrepancy:
            max_discrepancy = snap.reconciliation.return_discrepancy

    engine.close()

    results = {
        "report_type": "synthetic_benchmark_results",
        "data_provenance": "synthetic_generated_seed_42",
        "runtime_metadata": {
            "python_version": platform.python_version(),
            "platform": platform.platform(),
            "machine": platform.machine(),
            "config_version": config.config_version,
            "random_seed": config.random_seed,
            "storage_backend": config.storage_backend,
        },
        "workload": {
            "num_securities": num_securities,
            "days_per_security": days_per_security,
            "total_price_bars": len(all_bars),
            "total_corporate_actions": len(all_actions),
        },
        "measured_metrics": {
            "ingest_seconds": round(ingest_sec, 4),
            "ingest_bars_per_second": round(len(all_bars) / ingest_sec, 1) if ingest_sec > 0 else 0.0,
            "pit_query_mean_ms": round(statistics.mean(query_latencies_ms), 2),
            "pit_query_median_ms": round(statistics.median(query_latencies_ms), 2),
            "pit_query_max_ms": round(max(query_latencies_ms), 2),
            "all_series_reconciled": all_reconciled,
            "max_return_discrepancy": format(max_discrepancy, "f"),
        },
    }

    out_path = PROJECT_ROOT / "reports" / "benchmark_results.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(results, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    print("=== Synthetic Benchmark Results ===")
    print(json.dumps(results["measured_metrics"], indent=2))
    print(f"Wrote {out_path}")
    return results


if __name__ == "__main__":
    run_benchmark()
