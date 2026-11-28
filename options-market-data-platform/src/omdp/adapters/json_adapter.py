"""JSON source adapter for option chain bundles and fixtures."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from omdp.domain.contracts import (
    CanonicalOptionID,
    ContractMetadata,
    ContractValidationError,
    ExpiryCalendarEntry,
    OptionQuote,
    OptionRight,
    RateCurvePoint,
    UnderlyingQuote,
    parse_decimal,
    parse_utc_timestamp,
)


@dataclass(frozen=True)
class SnapshotBatchInput:
    snapshot_id: str
    underlying_symbol: str
    snapshot_time: datetime
    ingestion_time: datetime
    source_id: str
    schema_version: str
    quotes: Tuple[OptionQuote, ...]


@dataclass(frozen=True)
class FixtureBundle:
    bundle_name: str
    schema_version: str
    contracts: Tuple[ContractMetadata, ...]
    calendars: Tuple[ExpiryCalendarEntry, ...]
    rate_curves: Tuple[RateCurvePoint, ...]
    underlying_quotes: Tuple[UnderlyingQuote, ...]
    snapshots: Tuple[SnapshotBatchInput, ...]
    expected_oracle: Dict[str, Any]


def _parse_date(val: str) -> date:
    try:
        return date.fromisoformat(val)
    except (TypeError, ValueError) as exc:
        raise ContractValidationError(f"Invalid ISO date '{val}': {exc}") from exc


def parse_contract(raw: Dict[str, Any]) -> ContractMetadata:
    exp = _parse_date(raw["expiry_date"])
    right = OptionRight(str(raw["option_right"]).upper())
    strike = parse_decimal(raw["strike"], "strike")
    option_id = raw.get("option_id") or CanonicalOptionID(
        underlying_symbol=str(raw["underlying_symbol"]),
        expiry_date=exp,
        option_right=right,
        strike=strike,
    ).canonical
    return ContractMetadata(
        option_id=option_id,
        contract_version=int(raw.get("contract_version", 1)),
        underlying_symbol=str(raw["underlying_symbol"]),
        expiry_date=exp,
        option_right=right,
        strike=strike,
        strike_scale_decimals=int(raw.get("strike_scale_decimals", 2)),
        contract_multiplier=int(raw.get("contract_multiplier", 100)),
        exercise_style=str(raw.get("exercise_style", "EUROPEAN")),
        currency=str(raw.get("currency", "USD")),
        source_id=str(raw["source_id"]),
        schema_version=str(raw.get("schema_version", "1.0.0")),
        ingestion_time=parse_utc_timestamp(raw["ingestion_time"]),
        occ_symbol=raw.get("occ_symbol"),
    )


def parse_calendar_entry(raw: Dict[str, Any]) -> ExpiryCalendarEntry:
    return ExpiryCalendarEntry(
        calendar_id=str(raw["calendar_id"]),
        calendar_version=int(raw.get("calendar_version", 1)),
        expiry_date=_parse_date(raw["expiry_date"]),
        expiry_timestamp_utc=parse_utc_timestamp(raw["expiry_timestamp_utc"]),
        is_trading_day=bool(raw.get("is_trading_day", True)),
        settlement_type=str(raw.get("settlement_type", "PM")),
        source_id=str(raw["source_id"]),
        schema_version=str(raw.get("schema_version", "1.0.0")),
    )


def parse_rate_point(raw: Dict[str, Any]) -> RateCurvePoint:
    return RateCurvePoint(
        curve_id=str(raw["curve_id"]),
        curve_version=int(raw.get("curve_version", 1)),
        as_of_time=parse_utc_timestamp(raw["as_of_time"]),
        tenor_days=int(raw["tenor_days"]),
        risk_free_rate=float(raw["risk_free_rate"]),
        dividend_yield=float(raw.get("dividend_yield", 0.0)),
        source_id=str(raw["source_id"]),
        schema_version=str(raw.get("schema_version", "1.0.0")),
    )


def parse_underlying_quote(raw: Dict[str, Any]) -> UnderlyingQuote:
    last_raw = raw.get("last_price")
    return UnderlyingQuote(
        underlying_symbol=str(raw["underlying_symbol"]),
        quote_version=int(raw.get("quote_version", 1)),
        event_time=parse_utc_timestamp(raw["event_time"]),
        ingestion_time=parse_utc_timestamp(raw["ingestion_time"]),
        bid=parse_decimal(raw["bid"], "bid"),
        ask=parse_decimal(raw["ask"], "ask"),
        last_price=parse_decimal(last_raw, "last_price") if last_raw is not None else None,
        source_id=str(raw["source_id"]),
        source_sequence=int(raw.get("source_sequence", 0)),
        schema_version=str(raw.get("schema_version", "1.0.0")),
    )


def parse_option_quote(raw: Dict[str, Any], default_snapshot_id: Optional[str] = None) -> OptionQuote:
    snap_id = str(raw.get("snapshot_id") or default_snapshot_id or "")
    return OptionQuote(
        snapshot_id=snap_id,
        option_id=str(raw["option_id"]),
        quote_version=int(raw.get("quote_version", 1)),
        underlying_symbol=str(raw["underlying_symbol"]),
        reported_strike=parse_decimal(raw["reported_strike"], "reported_strike"),
        event_time=parse_utc_timestamp(raw["event_time"]),
        ingestion_time=parse_utc_timestamp(raw["ingestion_time"]),
        bid=parse_decimal(raw["bid"], "bid"),
        ask=parse_decimal(raw["ask"], "ask"),
        bid_size=int(raw.get("bid_size", 0)),
        ask_size=int(raw.get("ask_size", 0)),
        source_id=str(raw["source_id"]),
        source_sequence=int(raw.get("source_sequence", 0)),
        schema_version=str(raw.get("schema_version", "1.0.0")),
    )


def load_fixture_bundle(path: str | Path) -> FixtureBundle:
    """Load a versioned JSON fixture/feed bundle into typed canonical domain contracts."""
    file_path = Path(path)
    if not file_path.exists():
        raise FileNotFoundError(f"Fixture file not found: {file_path}")
    raw = json.loads(file_path.read_text(encoding="utf-8"))

    contracts = tuple(parse_contract(c) for c in raw.get("contracts", []))
    calendars = tuple(parse_calendar_entry(e) for e in raw.get("calendars", []))
    rate_curves = tuple(parse_rate_point(r) for r in raw.get("rate_curves", []))
    underlying_quotes = tuple(parse_underlying_quote(u) for u in raw.get("underlying_quotes", []))

    snapshots: List[SnapshotBatchInput] = []
    for snap_raw in raw.get("snapshots", []):
        snap_id = str(snap_raw["snapshot_id"])
        quotes = tuple(parse_option_quote(q, default_snapshot_id=snap_id) for q in snap_raw.get("quotes", []))
        snapshots.append(
            SnapshotBatchInput(
                snapshot_id=snap_id,
                underlying_symbol=str(snap_raw["underlying_symbol"]),
                snapshot_time=parse_utc_timestamp(snap_raw["snapshot_time"]),
                ingestion_time=parse_utc_timestamp(snap_raw["ingestion_time"]),
                source_id=str(snap_raw["source_id"]),
                schema_version=str(snap_raw.get("schema_version", "1.0.0")),
                quotes=quotes,
            )
        )

    return FixtureBundle(
        bundle_name=str(raw.get("bundle_name", file_path.stem)),
        schema_version=str(raw.get("schema_version", "1.0.0")),
        contracts=contracts,
        calendars=calendars,
        rate_curves=rate_curves,
        underlying_quotes=underlying_quotes,
        snapshots=tuple(snapshots),
        expected_oracle=dict(raw.get("expected_oracle", {})),
    )
