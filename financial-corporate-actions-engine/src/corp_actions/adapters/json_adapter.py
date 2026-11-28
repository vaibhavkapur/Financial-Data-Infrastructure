"""JSON source adapter converting external records into canonical domain contracts."""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from corp_actions.contracts import (
    SCHEMA_VERSION,
    ActionType,
    CorporateActionEvent,
    FxRate,
    PositionSnapshot,
    RawPriceBar,
    RevisionType,
)
from corp_actions.errors import ValidationError


def _parse_date(raw: Optional[str], field: str) -> Optional[date]:
    if raw is None:
        return None
    try:
        return date.fromisoformat(str(raw))
    except ValueError as exc:
        raise ValidationError(f"Invalid ISO date for {field}: {raw!r}") from exc


def _parse_dt(raw: Any, field: str) -> datetime:
    if raw is None:
        raise ValidationError(f"Missing required timestamp field: {field}")
    try:
        s = str(raw).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            raise ValidationError(f"Timestamp {field} must include timezone offset: {raw!r}")
        return dt.astimezone(timezone.utc)
    except ValidationError:
        raise
    except ValueError as exc:
        raise ValidationError(f"Invalid ISO datetime for {field}: {raw!r}") from exc


def _parse_decimal(raw: Any, field: str, *, required: bool = True) -> Optional[Decimal]:
    if raw is None:
        if required:
            raise ValidationError(f"Missing required Decimal field: {field}")
        return None
    try:
        return Decimal(str(raw))
    except Exception as exc:
        raise ValidationError(f"Invalid Decimal for {field}: {raw!r}") from exc


def parse_price_dict(d: Dict[str, Any]) -> RawPriceBar:
    try:
        td = _parse_date(d.get("trade_date"), "trade_date")
        assert td is not None
        return RawPriceBar(
            security_id=str(d["security_id"]),
            symbol=str(d["symbol"]),
            trade_date=td,
            open=_parse_decimal(d.get("open"), "open") or Decimal("0"),
            high=_parse_decimal(d.get("high"), "high") or Decimal("0"),
            low=_parse_decimal(d.get("low"), "low") or Decimal("0"),
            close=_parse_decimal(d.get("close"), "close") or Decimal("0"),
            volume=_parse_decimal(d.get("volume"), "volume") or Decimal("0"),
            currency=str(d["currency"]),
            event_time=_parse_dt(d.get("event_time"), "event_time"),
            ingestion_time=_parse_dt(d.get("ingestion_time"), "ingestion_time"),
            source=str(d["source"]),
            schema_version=str(d.get("schema_version", SCHEMA_VERSION)),
        )
    except KeyError as exc:
        raise ValidationError(f"Missing required RawPriceBar key: {exc}") from exc


def parse_position_dict(d: Dict[str, Any]) -> PositionSnapshot:
    try:
        ad = _parse_date(d.get("as_of_date"), "as_of_date")
        assert ad is not None
        return PositionSnapshot(
            account_id=str(d["account_id"]),
            security_id=str(d["security_id"]),
            symbol=str(d["symbol"]),
            as_of_date=ad,
            quantity=_parse_decimal(d.get("quantity"), "quantity") or Decimal("0"),
            cost_basis_per_share=_parse_decimal(d.get("cost_basis_per_share"), "cost_basis_per_share") or Decimal("0"),
            currency=str(d["currency"]),
            cash_balance=_parse_decimal(d.get("cash_balance"), "cash_balance") or Decimal("0"),
            event_time=_parse_dt(d.get("event_time"), "event_time"),
            ingestion_time=_parse_dt(d.get("ingestion_time"), "ingestion_time"),
            source=str(d["source"]),
            schema_version=str(d.get("schema_version", SCHEMA_VERSION)),
        )
    except KeyError as exc:
        raise ValidationError(f"Missing required PositionSnapshot key: {exc}") from exc


def parse_fx_dict(d: Dict[str, Any]) -> FxRate:
    try:
        rd = _parse_date(d.get("rate_date"), "rate_date")
        assert rd is not None
        return FxRate(
            base_currency=str(d["base_currency"]),
            quote_currency=str(d["quote_currency"]),
            rate_date=rd,
            rate=_parse_decimal(d.get("rate"), "rate") or Decimal("0"),
            event_time=_parse_dt(d.get("event_time"), "event_time"),
            ingestion_time=_parse_dt(d.get("ingestion_time"), "ingestion_time"),
            source=str(d["source"]),
            schema_version=str(d.get("schema_version", SCHEMA_VERSION)),
        )
    except KeyError as exc:
        raise ValidationError(f"Missing required FxRate key: {exc}") from exc


def parse_action_dict(d: Dict[str, Any]) -> CorporateActionEvent:
    try:
        ann = _parse_date(d.get("announcement_date"), "announcement_date")
        ex_d = _parse_date(d.get("ex_date"), "ex_date")
        if ann is None or ex_d is None:
            raise ValidationError("announcement_date and ex_date are required")
        rev_type = RevisionType(str(d["revision_type"]))
        act_type = ActionType(str(d["action_type"]))
        return CorporateActionEvent(
            action_id=str(d["action_id"]),
            version=int(d["version"]),
            revision_type=rev_type,
            action_type=act_type,
            security_id=str(d["security_id"]),
            symbol=str(d["symbol"]),
            announcement_date=ann,
            ex_date=ex_d,
            record_date=_parse_date(d.get("record_date"), "record_date"),
            pay_date=_parse_date(d.get("pay_date"), "pay_date"),
            ratio_numerator=_parse_decimal(d.get("ratio_numerator"), "ratio_numerator", required=False),
            ratio_denominator=_parse_decimal(d.get("ratio_denominator"), "ratio_denominator", required=False),
            cash_amount=_parse_decimal(d.get("cash_amount"), "cash_amount", required=False),
            currency=str(d["currency"]) if d.get("currency") is not None else None,
            tax_withholding_rate=_parse_decimal(d.get("tax_withholding_rate", "0"), "tax_withholding_rate") or Decimal("0"),
            new_symbol=str(d["new_symbol"]) if d.get("new_symbol") is not None else None,
            dividend_basis=str(d["dividend_basis"]) if d.get("dividend_basis") is not None else None,
            event_time=_parse_dt(d.get("event_time"), "event_time"),
            ingestion_time=_parse_dt(d.get("ingestion_time"), "ingestion_time"),
            availability_time=_parse_dt(d.get("availability_time"), "availability_time"),
            source=str(d["source"]),
            lineage_id=str(d["lineage_id"]),
            schema_version=str(d.get("schema_version", SCHEMA_VERSION)),
            metadata=dict(d.get("metadata") or {}),
        )
    except (KeyError, ValueError) as exc:
        raise ValidationError(f"Invalid CorporateActionEvent record: {exc}") from exc


def _load_json_array(path: Union[str, Path]) -> List[Dict[str, Any]]:
    p = Path(path)
    if not p.exists():
        raise ValidationError(f"Fixture file not found: {p}", details={"path": str(p)})
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValidationError(f"Malformed JSON in {p}: {exc}") from exc
    if not isinstance(data, list):
        raise ValidationError(f"Expected JSON array in {p}")
    return data


def load_prices_json(path: Union[str, Path]) -> List[RawPriceBar]:
    return [parse_price_dict(row) for row in _load_json_array(path)]


def load_positions_json(path: Union[str, Path]) -> List[PositionSnapshot]:
    return [parse_position_dict(row) for row in _load_json_array(path)]


def load_fx_rates_json(path: Union[str, Path]) -> List[FxRate]:
    return [parse_fx_dict(row) for row in _load_json_array(path)]


def load_actions_json(path: Union[str, Path]) -> List[CorporateActionEvent]:
    return [parse_action_dict(row) for row in _load_json_array(path)]


def load_fixture_bundle(
    fixtures_dir: Union[str, Path],
) -> Tuple[List[RawPriceBar], List[PositionSnapshot], List[FxRate], List[CorporateActionEvent]]:
    base = Path(fixtures_dir)
    return (
        load_prices_json(base / "raw_prices.json"),
        load_positions_json(base / "positions.json"),
        load_fx_rates_json(base / "fx_rates.json"),
        load_actions_json(base / "corporate_actions.json"),
    )
