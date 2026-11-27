"""Integration tests for snapshots, backfill, full replay recovery, and retention."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from financial_data_lake import AccessContext, FinancialDataLake
from financial_data_lake.query import compute_rows_digest


def test_snapshot_pinning_and_replay_reproduction(
    populated_lake: FinancialDataLake,
    research_ctx: AccessContext,
    eng_ctx: AccessContext,
    repo_root: Path,
    tmp_path: Path,
) -> None:
    as_of = datetime(2024, 6, 7, 23, 59, 59, tzinfo=UTC)
    snap = populated_lake.create_snapshot(
        name="snap-repro-test",
        datasets=["prices_eod", "fundamentals", "corporate_actions"],
        as_of_ts=as_of,
        access=research_ctx,
    )
    assert snap.license_ids["prices_eod"] == "SYNTH-INTERNAL-003"
    assert snap.license_ids["fundamentals"] == "SYNTH-INTERNAL-005"

    baseline = {
        ds: populated_lake.query_snapshot(snap.name, ds, research_ctx)
        for ds in snap.datasets
    }

    # Replay raw artifacts into a brand new recovery lake and verify identical row digests
    recovery_lake = FinancialDataLake(
        lake_root=tmp_path / "recovery_lake",
        config_path=repo_root / "config" / "lake_config.json",
        entitlements_path=repo_root / "config" / "entitlements.json",
        schemas_path=repo_root / "config" / "schemas" / "registry.json",
    )
    for ds in snap.datasets:
        populated_lake.recovery.replay_dataset_from_raw(
            dataset_id=ds,
            target_ingest=recovery_lake.ingest,
            access=eng_ctx,
        )
        replayed_rows = recovery_lake.query_as_of(ds, snap.as_of_ts, research_ctx)
        assert compute_rows_digest(replayed_rows) == compute_rows_digest(baseline[ds])


def test_idempotent_backfill_and_retention_evaluation(
    lake: FinancialDataLake,
    eng_ctx: AccessContext,
    research_ctx: AccessContext,
) -> None:
    batch_1 = [
        {
            "instrument_id": "INST-AAPL",
            "symbol": "AAPL",
            "trade_date": "2024-05-01",
            "open": 170.0,
            "high": 172.0,
            "low": 169.0,
            "close": 171.0,
            "volume": 500000.0,
            "currency": "USD",
            "unit": "share",
            "event_ts": "2024-05-01T20:00:00Z",
            "available_at": "2024-05-01T20:15:00Z",
            "revision_number": 1,
            "correction_reason": None,
        }
    ]
    batch_2 = [
        batch_1[0],  # overlapping duplicate across backfill windows
        {
            "instrument_id": "INST-AAPL",
            "symbol": "AAPL",
            "trade_date": "2024-05-02",
            "open": 171.0,
            "high": 174.0,
            "low": 170.5,
            "close": 173.0,
            "volume": 600000.0,
            "currency": "USD",
            "unit": "share",
            "event_ts": "2024-05-02T20:00:00Z",
            "available_at": "2024-05-02T20:15:00Z",
            "revision_number": 1,
            "correction_reason": None,
        },
    ]

    results = lake.recovery.backfill_batches(
        dataset_id="prices_eod",
        batches=[("backfill_may01", batch_1), ("backfill_may02", batch_2)],
        provenance="historical-backfill-202405",
        access=eng_ctx,
    )
    assert results[0].curated_rows == 1
    assert results[1].curated_rows == 1
    assert results[1].deduplicated_rows == 1

    rows = lake.query_as_of("prices_eod", datetime(2024, 5, 3, 0, 0, 0, tzinfo=UTC), research_ctx)
    assert len(rows) == 2

    # Retention check far in the future proposes cold-tier immutable archival without destructive rewrite
    ret = lake.recovery.evaluate_retention_policy(datetime(2035, 1, 1, 0, 0, 0, tzinfo=UTC))
    assert ret["destructive_rewrite_allowed"] is False
    assert ret["archival_mode"] == "cold_tier_immutable"
    assert len(ret["cold_archive_candidates"]) > 0
