"""Required failure-path and edge-case scenario tests from plan.md Section 5.

Covers all 7 mandatory scenarios:
1. Duplicate ticks
2. Late correction
3. Split/dividend
4. Schema change
5. Corrupt partition
6. Future revision
7. Unauthorized dataset
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from financial_data_lake import (
    AccessContext,
    CorruptPartitionError,
    EntitlementError,
    FieldSpec,
    FinancialDataLake,
    SchemaEvolutionError,
)
from financial_data_lake.query import compute_rows_digest


def test_scenario_1_duplicate_ticks(
    lake: FinancialDataLake,
    repo_root: Path,
    eng_ctx: AccessContext,
    research_ctx: AccessContext,
) -> None:
    """Duplicate ticks within a batch AND across re-ingested batches must never duplicate rows."""
    res1 = lake.ingest_all_fixtures(repo_root / "data" / "fixtures", eng_ctx)
    ticks_res = [r for r in res1 if r.dataset_id == "market_ticks"]
    # ticks.jsonl had 1 intra-batch duplicate; clickhouse_ticks.jsonl had 1 intra-batch duplicate
    assert sum(r.deduplicated_rows for r in ticks_res) == 2

    # Re-ingest identical fixtures -> completely idempotent
    res2 = lake.ingest_all_fixtures(repo_root / "data" / "fixtures", eng_ctx)
    assert all(r.skipped_idempotent for r in res2)

    # Also test overlapping batch with different raw bytes containing an already-ingested tick + 1 new tick
    overlap_batch = [
        {
            "tick_id": "TCK-20240603-0001",
            "instrument_id": "INST-AAPL",
            "symbol": "AAPL",
            "event_ts": "2024-06-03T13:30:01.100000Z",
            "available_at": "2024-06-03T13:30:01.250000Z",
            "event_date": "2024-06-03",
            "price": 199.80,
            "size": 100.0,
            "currency": "USD",
            "venue": "XNAS",
            "side": "BUY",
            "revision_number": 1,
            "correction_of": None,
            "source_system": "ts08-clickhouse",
        },
        {
            "tick_id": "TCK-20240603-9999",
            "instrument_id": "INST-AAPL",
            "symbol": "AAPL",
            "event_ts": "2024-06-03T15:00:00.000000Z",
            "available_at": "2024-06-03T15:00:00.100000Z",
            "event_date": "2024-06-03",
            "price": 200.25,
            "size": 50.0,
            "currency": "USD",
            "venue": "XNAS",
            "side": "BUY",
            "revision_number": 1,
            "correction_of": None,
            "source_system": "ts08-clickhouse",
        },
    ]
    overlap_res = lake.ingest.ingest_records(
        dataset_id="market_ticks",
        records=overlap_batch,
        source_name="overlap_ticks.jsonl",
        provenance="scenario-duplicate-ticks",
        access=eng_ctx,
    )
    assert overlap_res.deduplicated_rows == 1
    assert overlap_res.curated_rows == 1

    # Query all revisions and verify TCK-20240603-0002 & TCK-20240603-0001 appear only once
    all_rows = lake.query_as_of(
        "market_ticks",
        datetime(2024, 6, 5, 0, 0, 0, tzinfo=UTC),
        research_ctx,
        include_all_revisions=True,
    )
    tck2 = [r for r in all_rows if r["tick_id"] == "TCK-20240603-0002"]
    tck1 = [r for r in all_rows if r["tick_id"] == "TCK-20240603-0001"]
    assert len(tck2) == 1
    assert len(tck1) == 1


def test_scenario_2_late_correction(
    populated_lake: FinancialDataLake,
    research_ctx: AccessContext,
) -> None:
    """Late corrections must be versioned immutably; as-of queries reflect state before and after."""
    # TCK-20240603-0003: rev 1 (200.50) available at 14:30:05.12Z; rev 2 (200.10) available at 14:45:00Z
    before = populated_lake.query_as_of(
        "market_ticks",
        datetime(2024, 6, 3, 14, 40, 0, tzinfo=UTC),
        research_ctx,
        filters={"tick_id": "TCK-20240603-0003"},
    )
    assert len(before) == 1
    assert before[0]["revision_number"] == 1
    assert before[0]["price"] == pytest.approx(200.50)

    after = populated_lake.query_as_of(
        "market_ticks",
        datetime(2024, 6, 3, 14, 45, 0, tzinfo=UTC),
        research_ctx,
        filters={"tick_id": "TCK-20240603-0003"},
    )
    assert len(after) == 1
    assert after[0]["revision_number"] == 2
    assert after[0]["price"] == pytest.approx(200.10)

    # Full audit history still retains both versions (no destructive rewrite)
    both = populated_lake.query_as_of(
        "market_ticks",
        datetime(2024, 6, 3, 15, 0, 0, tzinfo=UTC),
        research_ctx,
        include_all_revisions=True,
        filters={"tick_id": "TCK-20240603-0003"},
    )
    assert [r["revision_number"] for r in both] == [1, 2]

    # Same check on EOD close revision for MSFT on 2024-06-04 (rev 1=401.0 -> rev 2=400.0 at 2024-06-05T12:00Z)
    msft_before = populated_lake.query_as_of(
        "prices_eod",
        datetime(2024, 6, 5, 11, 59, 0, tzinfo=UTC),
        research_ctx,
        filters={"instrument_id": "INST-MSFT", "trade_date": "2024-06-04"},
    )
    msft_after = populated_lake.query_as_of(
        "prices_eod",
        datetime(2024, 6, 5, 12, 0, 0, tzinfo=UTC),
        research_ctx,
        filters={"instrument_id": "INST-MSFT", "trade_date": "2024-06-04"},
    )
    assert msft_before[0]["close"] == pytest.approx(401.0)
    assert msft_after[0]["close"] == pytest.approx(400.0)


def test_scenario_3_split_and_dividend(
    populated_lake: FinancialDataLake,
    research_ctx: AccessContext,
) -> None:
    """Split and cash dividend adjustments apply accurately AND only when known as-of query time."""
    # AAPL 2-for-1 split: ex_date=2024-06-05, available_at=2024-06-04T22:00:00Z
    before_split_avail = populated_lake.query_adjusted_prices_as_of(
        datetime(2024, 6, 4, 21, 0, 0, tzinfo=UTC),
        research_ctx,
        symbol="AAPL",
    )
    # No lookahead: 2024-06-04 close remains 200.0 and volume 1_200_000
    assert before_split_avail[-1]["trade_date"] == "2024-06-04"
    assert before_split_avail[-1]["adj_close"] == pytest.approx(200.0)
    assert before_split_avail[-1]["adj_volume"] == pytest.approx(1_200_000.0)
    assert before_split_avail[-1]["applied_corporate_actions"] == []

    after_split_avail = populated_lake.query_adjusted_prices_as_of(
        datetime(2024, 6, 6, 23, 59, 59, tzinfo=UTC),
        research_ctx,
        symbol="AAPL",
    )
    by_date_aapl = {r["trade_date"]: r for r in after_split_avail}
    # Pre-split bars (06-03, 06-04) halved in price, doubled in volume; raw close preserved!
    assert by_date_aapl["2024-06-04"]["close"] == pytest.approx(200.0)
    assert by_date_aapl["2024-06-04"]["adj_close"] == pytest.approx(100.0)
    assert by_date_aapl["2024-06-04"]["adj_volume"] == pytest.approx(2_400_000.0)
    # Post-split bars unadjusted (factor = 1.0)
    assert by_date_aapl["2024-06-05"]["adj_close"] == pytest.approx(101.0)
    assert by_date_aapl["2024-06-06"]["adj_close"] == pytest.approx(102.0)

    # MSFT $2.00 cash dividend: ex_date=2024-06-06, available_at=2024-06-05T22:00:00Z
    # Prev close on 2024-06-05 is 400.0 -> factor = (400 - 2) / 400 = 0.995
    after_div_avail = populated_lake.query_adjusted_prices_as_of(
        datetime(2024, 6, 6, 23, 59, 59, tzinfo=UTC),
        research_ctx,
        symbol="MSFT",
    )
    by_date_msft = {r["trade_date"]: r for r in after_div_avail}
    assert by_date_msft["2024-06-05"]["adj_close"] == pytest.approx(398.0)
    assert by_date_msft["2024-06-06"]["adj_close"] == pytest.approx(398.0)


def test_scenario_4_schema_change(
    populated_lake: FinancialDataLake,
    eng_ctx: AccessContext,
    research_ctx: AccessContext,
) -> None:
    """Backward-compatible schema evolution works across old+new partitions; breaking change rejected."""
    v1_fields = list(populated_lake.schema_registry.get_schema("market_ticks").fields)

    # 1. Breaking change (dropping a column) is rejected before touching storage
    with pytest.raises(SchemaEvolutionError):
        populated_lake.evolve_schema("market_ticks", v1_fields[:-1])

    # 2. Compatible additive evolution (v1 -> v2 adding nullable venue_mic) succeeds
    v2 = populated_lake.evolve_schema(
        "market_ticks",
        v1_fields + [FieldSpec(name="venue_mic", type="string", nullable=True, default=None)],
    )
    assert v2.version == 2

    # Ingest a new tick using v2 column
    v2_tick = [
        {
            "tick_id": "TCK-20240605-V2",
            "instrument_id": "INST-AAPL",
            "symbol": "AAPL",
            "event_ts": "2024-06-05T14:00:00Z",
            "available_at": "2024-06-05T14:00:01Z",
            "event_date": "2024-06-05",
            "price": 100.50,
            "size": 200.0,
            "currency": "USD",
            "venue": "XNAS",
            "side": "BUY",
            "revision_number": 1,
            "correction_of": None,
            "source_system": "ts08-clickhouse",
            "venue_mic": "XNGS",
        }
    ]
    res = populated_lake.ingest.ingest_records(
        dataset_id="market_ticks",
        records=v2_tick,
        source_name="ticks_v2.jsonl",
        provenance="scenario-schema-evolution",
        access=eng_ctx,
    )
    assert res.curated_partitions[0].schema_version == 2

    # Query seamlessly across v1 and v2 partitions: v1 rows get venue_mic=None, v2 row gets "XNGS"
    rows = populated_lake.query_as_of(
        "market_ticks",
        datetime(2024, 6, 5, 23, 59, 59, tzinfo=UTC),
        research_ctx,
    )
    by_id = {r["tick_id"]: r for r in rows}
    assert by_id["TCK-20240603-0001"]["venue_mic"] is None
    assert by_id["TCK-20240605-V2"]["venue_mic"] == "XNGS"


def test_scenario_5_corrupt_partition(
    populated_lake: FinancialDataLake,
    research_ctx: AccessContext,
    recovery_ctx: AccessContext,
) -> None:
    """Corrupting a Parquet partition is detected fail-closed, quarantined, and recovered cleanly."""
    as_of = datetime(2024, 6, 7, 23, 59, 59, tzinfo=UTC)
    baseline_rows = populated_lake.query_as_of("prices_eod", as_of, research_ctx)
    baseline_digest = compute_rows_digest(baseline_rows)

    # Corrupt one curated Parquet partition on disk
    cur_parts = populated_lake.catalog.list_partitions(dataset_id="prices_eod", layer="curated", status="ACTIVE")
    victim = cur_parts[0]
    victim_file = populated_lake.storage.resolve_path(victim.object_path)
    victim_file.write_bytes(b"BITFLIP_CORRUPTION_PAYLOAD")

    # Fail-closed on query
    with pytest.raises(CorruptPartitionError):
        populated_lake.query_as_of("prices_eod", as_of, research_ctx)

    # Integrity scan detects the exact corrupt partition
    report_before = populated_lake.verify_integrity("prices_eod")
    assert victim.partition_id in report_before.corrupt_partition_ids

    # Quarantine & recover from immutable raw landing
    rec = populated_lake.recover_partition(victim.partition_id, recovery_ctx)
    assert populated_lake.storage.exists(rec.quarantine_path)
    assert populated_lake.catalog.get_partition(victim.partition_id).status == "QUARANTINED"

    # Integrity is restored and query reproduces exact baseline row digest
    report_after = populated_lake.verify_integrity("prices_eod")
    assert report_after.corrupt_partition_ids == []

    recovered_rows = populated_lake.query_as_of("prices_eod", as_of, research_ctx)
    assert compute_rows_digest(recovered_rows) == baseline_digest


def test_scenario_6_future_revision(
    populated_lake: FinancialDataLake,
    research_ctx: AccessContext,
) -> None:
    """Future restatements must be strictly excluded from earlier as-of queries and snapshots."""
    # AAPL 2024Q1 revenue restatement (rev 2 = 90.12B) has available_at = 2024-06-10T21:00:00Z
    snap = populated_lake.create_snapshot(
        name="pre-restatement-snap",
        datasets=["fundamentals"],
        as_of_ts=datetime(2024, 6, 7, 23, 59, 59, tzinfo=UTC),
        access=research_ctx,
    )

    # As-of query on June 7 sees ONLY rev 1 (90.753B, eps 1.53)
    pre_rows = populated_lake.query_snapshot(
        snap.name,
        "fundamentals",
        research_ctx,
        filters={"instrument_id": "INST-AAPL"},
    )
    pre_by_metric = {r["metric_name"]: r for r in pre_rows}
    assert pre_by_metric["revenue"]["revision_number"] == 1
    assert pre_by_metric["revenue"]["metric_value"] == pytest.approx(90753000000.0)
    assert pre_by_metric["eps_diluted"]["metric_value"] == pytest.approx(1.53)

    # As-of query 1 second BEFORE restatement still excludes rev 2
    just_before = populated_lake.query_as_of(
        "fundamentals",
        datetime(2024, 6, 10, 20, 59, 59, tzinfo=UTC),
        research_ctx,
        filters={"instrument_id": "INST-AAPL", "metric_name": "revenue"},
    )
    assert just_before[0]["revision_number"] == 1

    # As-of query AT restatement timestamp sees rev 2 (90.12B, is_restatement=True)
    at_restatement = populated_lake.query_as_of(
        "fundamentals",
        datetime(2024, 6, 10, 21, 0, 0, tzinfo=UTC),
        research_ctx,
        filters={"instrument_id": "INST-AAPL", "metric_name": "revenue"},
    )
    assert at_restatement[0]["revision_number"] == 2
    assert at_restatement[0]["metric_value"] == pytest.approx(90120000000.0)
    assert at_restatement[0]["is_restatement"] is True


def test_scenario_7_unauthorized_dataset(
    populated_lake: FinancialDataLake,
) -> None:
    """Unauthorized role, expired license, or redistribution request must fail closed and log audit."""
    as_of = datetime(2024, 6, 7, 23, 59, 59, tzinfo=UTC)

    # 1. Unauthorized role requesting restricted alt_sentiment (#19 dataset)
    external_user = AccessContext(
        principal="partner-bob",
        role="external_partner",
        purpose="research",
        requested_at=datetime(2025, 1, 15, 12, 0, 0, tzinfo=UTC),
    )
    with pytest.raises(EntitlementError):
        populated_lake.query_as_of("alt_sentiment", as_of, external_user)

    # 2. Redistribution attempt by otherwise authorized researcher
    redistribute_ctx = AccessContext(
        principal="quant-alice",
        role="quant_research",
        purpose="redistribution",
        requested_at=datetime(2025, 1, 15, 12, 0, 0, tzinfo=UTC),
    )
    with pytest.raises(EntitlementError, match="Redistribution"):
        populated_lake.query_as_of("prices_eod", as_of, redistribute_ctx)

    # 3. Expired dataset license
    expired_ctx = AccessContext(
        principal="quant-alice",
        role="quant_research",
        purpose="research",
        requested_at=datetime(2025, 1, 15, 12, 0, 0, tzinfo=UTC),
    )
    with pytest.raises(EntitlementError, match="not active"):
        populated_lake.query_as_of("legacy_expired_feed", as_of, expired_ctx)

    # Verify all 3 denials were durably recorded in audit_log
    audit = populated_lake.catalog.get_audit_log()
    denies = [e for e in audit if e["decision"] == "DENY"]
    assert len(denies) >= 3
    assert {e["dataset_id"] for e in denies} >= {"alt_sentiment", "prices_eod", "legacy_expired_feed"}
