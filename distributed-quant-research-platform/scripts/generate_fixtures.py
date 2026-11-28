#!/usr/bin/env python3
"""Generate deterministic synthetic fixtures, hand-calculated oracle, invalid fixtures, and configs.

All fixtures are 100% synthetic (generated via fixed seeds) with zero real market data,
customer records, credentials, or proprietary inputs.
"""

from __future__ import annotations

import csv
from datetime import datetime, timedelta, timezone
import json
import math
from pathlib import Path
import random
import sys
from typing import Dict, List

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from dqrp.contracts import (
    SCHEMA_VERSION,
    CostModel,
    DatasetManifest,
    DatasetPartition,
    ExperimentSpec,
    MarketBar,
    ResourceBudget,
)
from dqrp.hashing import sha256_file

FIXTURES_DIR = PROJECT_ROOT / "data" / "fixtures"
CONFIG_DIR = PROJECT_ROOT / "config"


def _write_jsonl_bars(path: Path, bars: List[MarketBar]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for bar in bars:
            fh.write(json.dumps(bar.to_dict(), sort_keys=True) + "\n")


def _write_csv_bars(path: Path, bars: List[MarketBar]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "schema_version",
        "symbol",
        "event_time",
        "availability_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "source",
    ]
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for bar in bars:
            writer.writerow(bar.to_dict())


def _build_partition(base_dir: Path, partition_id: str, rel_path: str, bars: List[MarketBar]) -> DatasetPartition:
    fpath = base_dir / rel_path
    return DatasetPartition(
        partition_id=partition_id,
        relative_path=rel_path,
        row_count=len(bars),
        content_sha256=sha256_file(fpath),
        min_event_time=min(b.event_time for b in bars),
        max_event_time=max(b.event_time for b in bars),
        max_availability_time=max(b.availability_time for b in bars),
        size_bytes=fpath.stat().st_size,
    )


def generate_oracle_fixture() -> DatasetManifest:
    """Create 6-bar synthetic oracle dataset paired with closed-form hand-calculated metrics."""
    oracle_dir = FIXTURES_DIR / "oracle"
    oracle_dir.mkdir(parents=True, exist_ok=True)
    closes = [100.0, 102.0, 104.0, 106.0, 98.0, 102.0]
    start = datetime(2025, 1, 2, 21, 0, 0, tzinfo=timezone.utc)
    bars: List[MarketBar] = []
    for i, c in enumerate(closes):
        ev = start + timedelta(days=i)
        av = ev + timedelta(seconds=30)
        bars.append(
            MarketBar(
                symbol="ORACLE",
                event_time=ev.strftime("%Y-%m-%dT%H:%M:%SZ"),
                availability_time=av.strftime("%Y-%m-%dT%H:%M:%SZ"),
                open=c,
                high=c + 1.0,
                low=c - 1.0,
                close=c,
                volume=10000.0,
                source="synthetic-oracle-v1",
            )
        )
    rel = "oracle_bars.jsonl"
    _write_jsonl_bars(oracle_dir / rel, bars)
    part = _build_partition(oracle_dir, "part-000", rel, bars)
    manifest = DatasetManifest(
        schema_version=SCHEMA_VERSION,
        dataset_id="synthetic-oracle-bars",
        version="1.0.0",
        format="jsonl",
        source_name="synthetic-oracle-generator",
        source_lineage="scripts/generate_fixtures.py:generate_oracle_fixture",
        created_at="2025-01-10T00:00:00Z",
        expected_row_count=len(bars),
        symbols=("ORACLE",),
        partitions=(part,),
    )
    (oracle_dir / "manifest.json").write_text(
        json.dumps(manifest.to_dict(), indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )

    # Independent closed-form analytical calculation for fast_window=2, slow_window=3, fee_bps=10.0
    r0 = 0.0
    r1 = 0.0
    r2 = (106.0 / 104.0 - 1.0) - 0.001  # enter long at bar 2, close->close return - 10bps fee
    r3 = 98.0 / 106.0 - 1.0             # stay long at bar 3
    r4 = -0.001                         # exit to flat at bar 4, pay 10bps fee
    rets = [r0, r1, r2, r3, r4]
    eq = 10000.0
    peak = eq
    max_dd = 0.0
    for r in rets:
        eq *= 1.0 + r
        if eq > peak:
            peak = eq
        max_dd = max(max_dd, (peak - eq) / peak)
    mean_r = sum(rets) / len(rets)
    std_r = math.sqrt(sum((r - mean_r) ** 2 for r in rets) / len(rets))
    sharpe = (mean_r / std_r) * math.sqrt(252.0)
    total_ret = eq / 10000.0 - 1.0
    ann_ret = (eq / 10000.0) ** (252.0 / 5.0) - 1.0
    ann_vol = std_r * math.sqrt(252.0)

    oracle_expected = {
        "description": "Independent analytical hand-calculated oracle for sma_crossover(fast=2, slow=3, fee_bps=10)",
        "tolerance": 1e-8,
        "dataset_sha256": manifest.manifest_sha256,
        "strategy_name": "sma_crossover",
        "params": {
            "symbol": "ORACLE",
            "fast_window": 2,
            "slow_window": 3,
            "fee_bps": 10.0,
            "slippage_bps": 0.0,
            "initial_capital": 10000.0,
            "allow_short": False,
            "bootstrap_draws": 0,
        },
        "expected_metrics": {
            "total_return": round(total_ret, 10),
            "annualized_return": round(ann_ret, 10),
            "annualized_volatility": round(ann_vol, 10),
            "sharpe_ratio": round(sharpe, 10),
            "max_drawdown": round(max_dd, 10),
            "win_rate": round(1.0 / 3.0, 10),
            "trade_count": 2,
            "final_equity": round(eq, 8),
            "turnover": 2.0,
            "bars_evaluated": 6,
        },
    }
    (oracle_dir / "expected_oracle.json").write_text(
        json.dumps(oracle_expected, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return manifest


def generate_multi_symbol_dataset() -> DatasetManifest:
    """Create a 2-partition synthetic 252-day dataset for SYM_A and SYM_B."""
    ds_dir = FIXTURES_DIR / "equities_synthetic_v1"
    ds_dir.mkdir(parents=True, exist_ok=True)
    rng = random.Random(20250101)
    start = datetime(2024, 1, 2, 21, 0, 0, tzinfo=timezone.utc)

    symbols_cfg = [
        ("SYM_A", 100.0, 0.00045, 0.014, 15),
        ("SYM_B", 50.0, 0.00020, 0.018, 45),
    ]
    partitions: List[DatasetPartition] = []
    total_rows = 0

    for idx, (sym, s0, drift, vol, lag_s) in enumerate(symbols_cfg):
        bars: List[MarketBar] = []
        price = s0
        for day in range(252):
            ev = start + timedelta(days=day)
            av = ev + timedelta(seconds=lag_s)
            shock = rng.gauss(drift, vol)
            o = round(price, 4)
            c = round(max(1.0, price * math.exp(shock)), 4)
            spread = abs(rng.gauss(0.0, vol * 0.5)) * max(o, c)
            h = round(max(o, c) + spread, 4)
            l = round(max(0.5, min(o, c) - spread), 4)
            vol_sh = round(50000.0 + abs(rng.gauss(0.0, 15000.0)), 2)
            bars.append(
                MarketBar(
                    symbol=sym,
                    event_time=ev.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    availability_time=av.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    open=o,
                    high=h,
                    low=l,
                    close=c,
                    volume=vol_sh,
                    source="synthetic-gbm-v1",
                )
            )
            price = c
        rel = f"part-{idx:03d}-{sym.lower()}.jsonl"
        _write_jsonl_bars(ds_dir / rel, bars)
        partitions.append(_build_partition(ds_dir, f"part-{idx:03d}", rel, bars))
        total_rows += len(bars)

    # Also write a CSV mirror dataset to validate CSV source adapter
    csv_dir = FIXTURES_DIR / "equities_synthetic_csv_v1"
    csv_dir.mkdir(parents=True, exist_ok=True)
    # Reuse first 60 bars of SYM_A from jsonl
    sym_a_bars = [
        MarketBar.from_dict(json.loads(line))
        for line in (ds_dir / "part-000-sym_a.jsonl").read_text(encoding="utf-8").splitlines()[:60]
    ]
    _write_csv_bars(csv_dir / "sym_a_60d.csv", sym_a_bars)
    csv_part = _build_partition(csv_dir, "part-000", "sym_a_60d.csv", sym_a_bars)
    csv_manifest = DatasetManifest(
        schema_version=SCHEMA_VERSION,
        dataset_id="synthetic-equities-csv",
        version="1.0.0",
        format="csv",
        source_name="synthetic-gbm-v1",
        source_lineage="scripts/generate_fixtures.py:generate_multi_symbol_dataset:csv",
        created_at="2025-01-10T00:00:00Z",
        expected_row_count=len(sym_a_bars),
        symbols=("SYM_A",),
        partitions=(csv_part,),
    )
    (csv_dir / "manifest.json").write_text(
        json.dumps(csv_manifest.to_dict(), indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )

    manifest = DatasetManifest(
        schema_version=SCHEMA_VERSION,
        dataset_id="synthetic-equities-daily",
        version="1.0.0",
        format="jsonl",
        source_name="synthetic-gbm-v1",
        source_lineage="scripts/generate_fixtures.py:generate_multi_symbol_dataset:seed=20250101",
        created_at="2025-01-10T00:00:00Z",
        expected_row_count=total_rows,
        symbols=("SYM_A", "SYM_B"),
        partitions=tuple(partitions),
    )
    (ds_dir / "manifest.json").write_text(
        json.dumps(manifest.to_dict(), indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return manifest


def generate_invalid_and_boundary_fixtures() -> None:
    """Generate explicit invalid and boundary fixtures referenced by contract tests."""
    inv_dir = FIXTURES_DIR / "invalid"
    inv_dir.mkdir(parents=True, exist_ok=True)

    # 1. Invalid manifest: unsupported schema version
    (inv_dir / "manifest_bad_schema.json").write_text(
        json.dumps(
            {
                "schema_version": "dqrp.v99",
                "dataset_id": "bad-schema",
                "version": "1.0.0",
                "format": "jsonl",
                "source_name": "synthetic",
                "source_lineage": "test",
                "created_at": "2025-01-01T00:00:00Z",
                "expected_row_count": 1,
                "symbols": ["SYM_A"],
                "partitions": [],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    # 2. Invalid bar fixture: point-in-time violation (availability_time < event_time)
    bad_bar_path = inv_dir / "pit_violation_bars.jsonl"
    bad_bar = {
        "schema_version": SCHEMA_VERSION,
        "symbol": "BAD",
        "event_time": "2025-01-02T21:00:00Z",
        "availability_time": "2025-01-02T20:59:00Z",
        "open": 100.0,
        "high": 101.0,
        "low": 99.0,
        "close": 100.5,
        "volume": 1000.0,
        "source": "synthetic-invalid",
    }
    bad_bar_path.write_text(json.dumps(bad_bar) + "\n", encoding="utf-8")
    bad_part = {
        "partition_id": "part-000",
        "relative_path": "pit_violation_bars.jsonl",
        "row_count": 1,
        "content_sha256": sha256_file(bad_bar_path),
        "min_event_time": "2025-01-02T21:00:00Z",
        "max_event_time": "2025-01-02T21:00:00Z",
        "max_availability_time": "2025-01-02T21:00:00Z",
        "size_bytes": bad_bar_path.stat().st_size,
    }
    (inv_dir / "manifest_pit_violation.json").write_text(
        json.dumps(
            {
                "schema_version": SCHEMA_VERSION,
                "dataset_id": "bad-pit",
                "version": "1.0.0",
                "format": "jsonl",
                "source_name": "synthetic",
                "source_lineage": "invalid-fixture",
                "created_at": "2025-01-03T00:00:00Z",
                "expected_row_count": 1,
                "symbols": ["BAD"],
                "partitions": [bad_part],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    # 3. Invalid spec: unsupported strategy
    (inv_dir / "spec_unsupported_strategy.json").write_text(
        json.dumps(
            {
                "schema_version": SCHEMA_VERSION,
                "experiment_id": "exp-invalid-strategy",
                "name": "Invalid Strategy Spec",
                "dataset_manifest_path": "../oracle/manifest.json",
                "strategy_name": "quantum_magic_alpha",
                "base_seed": 42,
                "parameter_grid": {"symbol": ["ORACLE"]},
                "budget": {"max_trials": 1, "max_concurrency": 1},
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    # 4. Boundary fixture: minimal 2-bar flat-price dataset (zero volatility -> Sharpe = 0.0)
    bnd_dir = FIXTURES_DIR / "boundary"
    bnd_dir.mkdir(parents=True, exist_ok=True)
    flat_bars = [
        MarketBar(
            symbol="FLAT",
            event_time="2025-01-02T21:00:00Z",
            availability_time="2025-01-02T21:00:00Z",
            open=100.0,
            high=100.0,
            low=100.0,
            close=100.0,
            volume=0.0,
            source="synthetic-boundary",
        ),
        MarketBar(
            symbol="FLAT",
            event_time="2025-01-03T21:00:00Z",
            availability_time="2025-01-03T21:00:00Z",
            open=100.0,
            high=100.0,
            low=100.0,
            close=100.0,
            volume=0.0,
            source="synthetic-boundary",
        ),
    ]
    _write_jsonl_bars(bnd_dir / "flat_2bars.jsonl", flat_bars)
    bpart = _build_partition(bnd_dir, "part-000", "flat_2bars.jsonl", flat_bars)
    bmanifest = DatasetManifest(
        schema_version=SCHEMA_VERSION,
        dataset_id="synthetic-boundary-flat",
        version="1.0.0",
        format="jsonl",
        source_name="synthetic-boundary",
        source_lineage="scripts/generate_fixtures.py:boundary",
        created_at="2025-01-10T00:00:00Z",
        expected_row_count=2,
        symbols=("FLAT",),
        partitions=(bpart,),
    )
    (bnd_dir / "manifest.json").write_text(
        json.dumps(bmanifest.to_dict(), indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def generate_configs(oracle_manifest: DatasetManifest, eq_manifest: DatasetManifest) -> None:
    """Write canonical versioned experiment configs in config/."""
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)

    oracle_spec = ExperimentSpec(
        schema_version=SCHEMA_VERSION,
        experiment_id="exp-oracle-sma",
        name="Reference Oracle SMA Crossover",
        description="Deterministic single-trial reference experiment matching hand-calculated expected_oracle.json.",
        dataset_manifest_path="../data/fixtures/oracle/manifest.json",
        expected_dataset_sha256=oracle_manifest.manifest_sha256,
        strategy_name="sma_crossover",
        base_seed=42,
        budget=ResourceBudget(
            max_trials=4,
            max_concurrency=1,
            max_cpu_seconds=30.0,
            max_wall_seconds=30.0,
            max_memory_mb=1024.0,
            max_estimated_cost_usd=0.50,
        ),
        explicit_trials=(
            {
                "symbol": "ORACLE",
                "fast_window": 2,
                "slow_window": 3,
                "fee_bps": 10.0,
                "slippage_bps": 0.0,
                "initial_capital": 10000.0,
                "allow_short": False,
                "bootstrap_draws": 0,
            },
        ),
        cost_model=CostModel(),
    )
    (CONFIG_DIR / "oracle_experiment.json").write_text(
        json.dumps(oracle_spec.to_dict(), indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )

    demo_spec = ExperimentSpec(
        schema_version=SCHEMA_VERSION,
        experiment_id="exp-demo-momentum-grid",
        name="Demo Momentum Z-Score Parameter Sweep",
        description="Reproducible 16-trial momentum sweep across SYM_A and SYM_B with seeded bootstrap CIs.",
        dataset_manifest_path="../data/fixtures/equities_synthetic_v1/manifest.json",
        expected_dataset_sha256=eq_manifest.manifest_sha256,
        strategy_name="momentum_zscore",
        base_seed=1337,
        budget=ResourceBudget(
            max_trials=32,
            max_concurrency=4,
            max_cpu_seconds=120.0,
            max_wall_seconds=120.0,
            max_memory_mb=2048.0,
            max_estimated_cost_usd=1.00,
        ),
        parameter_grid={
            "symbol": ["SYM_A", "SYM_B"],
            "lookback_window": [5, 10, 20, 40],
            "entry_z": [0.75, 1.25],
            "exit_z": [0.25],
            "fee_bps": [5.0],
            "bootstrap_draws": [300],
        },
        cost_model=CostModel(),
    )
    (CONFIG_DIR / "demo_experiment.json").write_text(
        json.dumps(demo_spec.to_dict(), indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )

    bench_spec = ExperimentSpec(
        schema_version=SCHEMA_VERSION,
        experiment_id="exp-scaling-benchmark",
        name="Scaling Benchmark Sweep (24 CPU-bound bootstrap trials)",
        description="Synthetic CPU-bound bootstrap sweep designed to measure real multi-core distributed speedup.",
        dataset_manifest_path="../data/fixtures/equities_synthetic_v1/manifest.json",
        expected_dataset_sha256=eq_manifest.manifest_sha256,
        strategy_name="momentum_zscore",
        base_seed=2025,
        budget=ResourceBudget(
            max_trials=48,
            max_concurrency=8,
            max_cpu_seconds=300.0,
            max_wall_seconds=300.0,
            max_memory_mb=4096.0,
            max_estimated_cost_usd=2.00,
        ),
        parameter_grid={
            "symbol": ["SYM_A", "SYM_B"],
            "lookback_window": [5, 10, 15, 20, 30, 40],
            "entry_z": [0.75, 1.25],
            "exit_z": [0.25],
            "fee_bps": [5.0],
            "bootstrap_draws": [2500],
        },
        cost_model=CostModel(),
    )
    (CONFIG_DIR / "benchmark_experiment.json").write_text(
        json.dumps(bench_spec.to_dict(), indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def main() -> int:
    oracle_manifest = generate_oracle_fixture()
    eq_manifest = generate_multi_symbol_dataset()
    generate_invalid_and_boundary_fixtures()
    generate_configs(oracle_manifest, eq_manifest)
    summary: Dict[str, str] = {
        "oracle_dataset_sha256": oracle_manifest.manifest_sha256,
        "equities_dataset_sha256": eq_manifest.manifest_sha256,
        "fixtures_dir": str(FIXTURES_DIR),
        "config_dir": str(CONFIG_DIR),
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
