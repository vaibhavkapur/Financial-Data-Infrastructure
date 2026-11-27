"""ClickHouse tick schema contract & adapter (TS-08 08-market-data-platform integration boundary).

Implements a clean, documented contract adapter rather than copying external repository code,
normalizing ClickHouse MergeTree JSONEachRow tick exports into canonical UTC TickRecord rows.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from financial_data_lake.errors import SchemaValidationError
from financial_data_lake.models import TickRecord

CLICKHOUSE_SIDE_MAP: dict[str, Literal["BUY", "SELL", "UNKNOWN"]] = {
    "B": "BUY",
    "BUY": "BUY",
    "S": "SELL",
    "SELL": "SELL",
    "U": "UNKNOWN",
    "UNKNOWN": "UNKNOWN",
}


class ClickHouseTickRow(BaseModel):
    """Contract mirroring ClickHouse MergeTree `market_ticks_raw` JSONEachRow export."""

    model_config = ConfigDict(frozen=True)

    seq_id: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    ts_event_ns: int = Field(gt=0)
    ts_ingest_ns: int = Field(gt=0)
    px: float = Field(gt=0.0)
    qty: float = Field(gt=0.0)
    ccy: str = Field(min_length=3, max_length=3)
    venue_code: str = Field(min_length=1)
    aggressor_side: str = Field(min_length=1)
    revision_number: int = Field(default=1, ge=1)
    correction_of: str | None = None


class ClickHouseTickAdapter:
    """Translates ClickHouse tick rows into canonical TickRecord models with explicit instrument IDs."""

    def __init__(
        self,
        symbol_to_instrument_id: dict[str, str],
        *,
        source_system: str = "ts08-clickhouse",
    ) -> None:
        self.symbol_to_instrument_id = dict(symbol_to_instrument_id)
        self.source_system = source_system

    def adapt_row(self, raw: dict[str, Any]) -> TickRecord:
        try:
            row = ClickHouseTickRow.model_validate(raw)
        except Exception as exc:
            raise SchemaValidationError(
                f"ClickHouse tick row failed contract validation: {exc}",
                context={"raw_row": raw},
            ) from exc

        instrument_id = self.symbol_to_instrument_id.get(row.symbol)
        if instrument_id is None:
            raise SchemaValidationError(
                f"Unknown symbol '{row.symbol}' in ClickHouse tick stream; register reference instrument first.",
                context={"symbol": row.symbol, "seq_id": row.seq_id},
            )

        side = CLICKHOUSE_SIDE_MAP.get(row.aggressor_side.upper())
        if side is None:
            raise SchemaValidationError(
                f"Unrecognized ClickHouse aggressor_side '{row.aggressor_side}'.",
                context={"seq_id": row.seq_id, "aggressor_side": row.aggressor_side},
            )

        event_ts = datetime.fromtimestamp(row.ts_event_ns / 1_000_000_000, tz=UTC)
        available_at = datetime.fromtimestamp(row.ts_ingest_ns / 1_000_000_000, tz=UTC)
        if available_at < event_ts:
            raise SchemaValidationError(
                f"ClickHouse ts_ingest_ns ({row.ts_ingest_ns}) precedes ts_event_ns ({row.ts_event_ns}).",
                context={"seq_id": row.seq_id},
            )

        return TickRecord(
            tick_id=row.seq_id,
            instrument_id=instrument_id,
            symbol=row.symbol,
            event_ts=event_ts,
            available_at=available_at,
            event_date=event_ts.strftime("%Y-%m-%d"),
            price=row.px,
            size=row.qty,
            currency=row.ccy.upper(),
            venue=row.venue_code,
            side=side,
            revision_number=row.revision_number,
            correction_of=row.correction_of,
            source_system=self.source_system,
        )

    def adapt_jsonl_bytes(self, payload: bytes) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for idx, line in enumerate(payload.decode("utf-8").splitlines(), start=1):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                raw = json.loads(stripped)
            except json.JSONDecodeError as exc:
                raise SchemaValidationError(
                    f"Malformed JSON on ClickHouse line {idx}: {exc}",
                    context={"line": idx},
                ) from exc
            adapted = self.adapt_row(raw)
            records.append(adapted.model_dump(mode="python"))
        return records

    def adapt_jsonl_file(self, path: Path | str) -> list[dict[str, Any]]:
        return self.adapt_jsonl_bytes(Path(path).read_bytes())
