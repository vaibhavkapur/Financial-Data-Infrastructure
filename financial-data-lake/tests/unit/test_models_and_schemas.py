"""Unit tests for domain contracts, configuration guardrails, and schema evolution."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from financial_data_lake import FieldSpec, SchemaEvolutionError, SchemaRegistry
from financial_data_lake.config import ExecutionControlsConfig, RetentionPolicyConfig
from financial_data_lake.models import CorporateActionRecord, PriceBarRecord, TickRecord


def test_bitemporal_rejects_naive_timestamps() -> None:
    with pytest.raises(ValidationError, match="timezone-aware UTC"):
        TickRecord(
            tick_id="T1",
            instrument_id="INST-AAPL",
            symbol="AAPL",
            event_ts=datetime(2024, 6, 3, 13, 30, 0),  # naive!
            available_at=datetime(2024, 6, 3, 13, 30, 1, tzinfo=UTC),
            event_date="2024-06-03",
            price=100.0,
            size=10.0,
            currency="USD",
            venue="XNAS",
            side="BUY",
            source_system="test",
        )


def test_bitemporal_rejects_available_before_event() -> None:
    with pytest.raises(ValidationError, match="cannot precede"):
        TickRecord(
            tick_id="T1",
            instrument_id="INST-AAPL",
            symbol="AAPL",
            event_ts=datetime(2024, 6, 3, 13, 30, 5, tzinfo=UTC),
            available_at=datetime(2024, 6, 3, 13, 30, 1, tzinfo=UTC),
            event_date="2024-06-03",
            price=100.0,
            size=10.0,
            currency="USD",
            venue="XNAS",
            side="BUY",
            source_system="test",
        )


def test_tick_revision_requires_correction_of() -> None:
    with pytest.raises(ValidationError, match="correction_of"):
        TickRecord(
            tick_id="T1",
            instrument_id="INST-AAPL",
            symbol="AAPL",
            event_ts=datetime(2024, 6, 3, 13, 30, 0, tzinfo=UTC),
            available_at=datetime(2024, 6, 3, 13, 31, 0, tzinfo=UTC),
            event_date="2024-06-03",
            price=100.0,
            size=10.0,
            currency="USD",
            venue="XNAS",
            side="BUY",
            revision_number=2,
            correction_of=None,
            source_system="test",
        )


def test_price_bar_ohlc_invariants() -> None:
    with pytest.raises(ValidationError, match="high must be"):
        PriceBarRecord(
            instrument_id="INST-AAPL",
            symbol="AAPL",
            trade_date="2024-06-03",
            open=100.0,
            high=99.0,
            low=98.0,
            close=99.5,
            volume=1000.0,
            currency="USD",
            unit="share",
            event_ts=datetime(2024, 6, 3, 20, 0, 0, tzinfo=UTC),
            available_at=datetime(2024, 6, 3, 20, 15, 0, tzinfo=UTC),
        )


def test_corporate_action_requires_type_specific_fields() -> None:
    with pytest.raises(ValidationError, match="split_numerator"):
        CorporateActionRecord(
            action_id="CA-1",
            instrument_id="INST-AAPL",
            symbol="AAPL",
            action_type="SPLIT",
            ex_date="2024-06-05",
            record_date="2024-06-04",
            currency="USD",
            event_ts=datetime(2024, 6, 4, 21, 0, 0, tzinfo=UTC),
            available_at=datetime(2024, 6, 4, 22, 0, 0, tzinfo=UTC),
        )


def test_config_forbids_destructive_rewrite_execution_and_redistribution() -> None:
    with pytest.raises(ValidationError, match="Destructive history rewrite"):
        RetentionPolicyConfig(allow_destructive_history_rewrite=True)
    with pytest.raises(ValidationError, match="Order execution must remain disabled"):
        ExecutionControlsConfig(order_execution_enabled=True)
    with pytest.raises(ValidationError, match="Redistribution"):
        ExecutionControlsConfig(allow_redistribution=True)


def test_schema_evolution_compatible_and_breaking(repo_root: Path) -> None:
    reg = SchemaRegistry.load(repo_root / "config" / "schemas" / "registry.json")
    v1 = list(reg.get_schema("market_ticks").fields)

    # Compatible: add nullable column + widen revision_number int32 -> int64
    widened = [
        FieldSpec(name=f.name, type="int64" if f.name == "revision_number" else f.type, nullable=f.nullable)
        for f in v1
    ] + [FieldSpec(name="venue_mic", type="string", nullable=True, default=None)]
    v2 = reg.register_evolution("market_ticks", widened)
    assert v2.version == 2

    # Breaking: drop column
    with pytest.raises(SchemaEvolutionError, match="drop existing field"):
        reg.validate_evolution("market_ticks", widened[:-2])

    # Breaking: tighten nullable -> non-nullable
    tightened = [
        FieldSpec(name=f.name, type=f.type, nullable=False if f.name == "correction_of" else f.nullable)
        for f in widened
    ]
    with pytest.raises(SchemaEvolutionError, match="tighten nullable"):
        reg.validate_evolution("market_ticks", tightened)

    # Breaking: narrow float64 -> int64
    narrowed = [
        FieldSpec(name=f.name, type="int64" if f.name == "price" else f.type, nullable=f.nullable)
        for f in widened
    ]
    with pytest.raises(SchemaEvolutionError, match="incompatible type change"):
        reg.validate_evolution("market_ticks", narrowed)

    # Breaking: new non-nullable column without default
    with pytest.raises(SchemaEvolutionError, match="must provide a default"):
        reg.validate_evolution(
            "market_ticks",
            widened + [FieldSpec(name="mandatory_tag", type="string", nullable=False, default=None)],
        )
