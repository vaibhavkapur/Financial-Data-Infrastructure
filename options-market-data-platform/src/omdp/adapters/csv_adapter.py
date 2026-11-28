"""CSV source adapter for option and underlying quote feeds."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Tuple

from omdp.domain.contracts import (
    OptionQuote,
    UnderlyingQuote,
    parse_decimal,
    parse_utc_timestamp,
)


def load_option_quotes_csv(path: str | Path) -> Tuple[OptionQuote, ...]:
    """Read option quotes from a canonical CSV file."""
    file_path = Path(path)
    quotes = []
    with file_path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            quotes.append(
                OptionQuote(
                    snapshot_id=row["snapshot_id"],
                    option_id=row["option_id"],
                    quote_version=int(row.get("quote_version") or 1),
                    underlying_symbol=row["underlying_symbol"],
                    reported_strike=parse_decimal(row["reported_strike"], "reported_strike"),
                    event_time=parse_utc_timestamp(row["event_time"]),
                    ingestion_time=parse_utc_timestamp(row["ingestion_time"]),
                    bid=parse_decimal(row["bid"], "bid"),
                    ask=parse_decimal(row["ask"], "ask"),
                    bid_size=int(row.get("bid_size") or 0),
                    ask_size=int(row.get("ask_size") or 0),
                    source_id=row["source_id"],
                    source_sequence=int(row.get("source_sequence") or 0),
                    schema_version=row.get("schema_version") or "1.0.0",
                )
            )
    return tuple(quotes)


def load_underlying_quotes_csv(path: str | Path) -> Tuple[UnderlyingQuote, ...]:
    """Read underlying quotes from a canonical CSV file."""
    file_path = Path(path)
    quotes = []
    with file_path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            last_raw = row.get("last_price")
            quotes.append(
                UnderlyingQuote(
                    underlying_symbol=row["underlying_symbol"],
                    quote_version=int(row.get("quote_version") or 1),
                    event_time=parse_utc_timestamp(row["event_time"]),
                    ingestion_time=parse_utc_timestamp(row["ingestion_time"]),
                    bid=parse_decimal(row["bid"], "bid"),
                    ask=parse_decimal(row["ask"], "ask"),
                    last_price=parse_decimal(last_raw, "last_price") if last_raw else None,
                    source_id=row["source_id"],
                    source_sequence=int(row.get("source_sequence") or 0),
                    schema_version=row.get("schema_version") or "1.0.0",
                )
            )
    return tuple(quotes)
