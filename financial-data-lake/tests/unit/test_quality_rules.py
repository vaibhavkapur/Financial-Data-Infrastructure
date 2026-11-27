"""Unit tests for the deterministic data quality rules engine."""

from __future__ import annotations

from datetime import UTC, datetime

from financial_data_lake.config import QualityConfig
from financial_data_lake.quality import QualityEngine


def test_quality_engine_passes_valid_batch() -> None:
    engine = QualityEngine(QualityConfig())
    records = [
        {
            "tick_id": "T1",
            "instrument_id": "INST-AAPL",
            "price": 100.0,
            "size": 10.0,
            "event_ts": datetime(2024, 6, 3, 13, 30, 0, tzinfo=UTC),
            "available_at": datetime(2024, 6, 3, 13, 30, 1, tzinfo=UTC),
            "revision_number": 1,
        }
    ]
    report = engine.evaluate(
        dataset_id="market_ticks",
        layer="normalized",
        ingestion_run_id="run-1",
        records=records,
        required_non_null_fields=["tick_id", "instrument_id", "price"],
        dedupe_key=["instrument_id", "tick_id", "revision_number"],
    )
    assert report.passed
    assert all(c.passed for c in report.checks)


def test_quality_engine_catches_revision_monotonicity_violation() -> None:
    engine = QualityEngine(QualityConfig())
    records = [
        {
            "tick_id": "T1",
            "instrument_id": "INST-AAPL",
            "price": 100.0,
            "size": 10.0,
            "event_ts": datetime(2024, 6, 3, 13, 30, 0, tzinfo=UTC),
            "available_at": datetime(2024, 6, 3, 14, 0, 0, tzinfo=UTC),
            "revision_number": 1,
        },
        {
            "tick_id": "T1",
            "instrument_id": "INST-AAPL",
            "price": 101.0,
            "size": 10.0,
            "event_ts": datetime(2024, 6, 3, 13, 30, 0, tzinfo=UTC),
            # Revision 2 claims earlier availability than Revision 1!
            "available_at": datetime(2024, 6, 3, 13, 45, 0, tzinfo=UTC),
            "revision_number": 2,
        },
    ]
    report = engine.evaluate(
        dataset_id="market_ticks",
        layer="normalized",
        ingestion_run_id="run-2",
        records=records,
        required_non_null_fields=["tick_id", "instrument_id", "price"],
        dedupe_key=["instrument_id", "tick_id", "revision_number"],
    )
    assert not report.passed
    rev_check = next(c for c in report.checks if c.rule_name == "revision_availability_monotonicity")
    assert not rev_check.passed
    assert rev_check.affected_rows == 1
