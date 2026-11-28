"""Pure domain quote validation and chain snapshot consistency checks.

Isolated completely from I/O and storage. Evaluates:
- Schema version support
- Expired options (against expiry calendar or default UTC expiry close)
- Strike / decimal precision mismatch against ContractMetadata
- Crossed, locked, and negative/zero ask quotes
- Quote staleness (snapshot_time - event_time) and ingestion delay (ingestion_time - event_time)
- Chain snapshot structural consistency (mixed underlying, duplicate quote version, snapshot_id mismatch)
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, List, Mapping, Optional, Sequence, Tuple

from omdp.config import PlatformConfig
from omdp.domain.contracts import (
    CanonicalOptionID,
    ChainSnapshot,
    ContractMetadata,
    DataQualityFlag,
    ExpiryCalendarEntry,
    OptionQuote,
    SnapshotConsistencyError,
    UnsupportedInputError,
    ValidatedOptionQuote,
    ValidationStatus,
    decimal_places,
)


def resolve_expiry_timestamp(
    contract: ContractMetadata,
    calendar_by_date: Mapping[str, ExpiryCalendarEntry],
    config: PlatformConfig,
) -> Tuple[datetime, Optional[ExpiryCalendarEntry]]:
    """Return the exact UTC expiry timestamp for a contract, using calendar entry when available."""
    key = contract.expiry_date.isoformat()
    if key in calendar_by_date:
        entry = calendar_by_date[key]
        return entry.expiry_timestamp_utc, entry
    fallback = datetime(
        contract.expiry_date.year,
        contract.expiry_date.month,
        contract.expiry_date.day,
        config.pricing.default_expiry_utc_hour,
        config.pricing.default_expiry_utc_minute,
        0,
        tzinfo=timezone.utc,
    )
    return fallback, None


def validate_option_quote(
    quote: OptionQuote,
    snapshot_time: datetime,
    contract: Optional[ContractMetadata],
    calendar_by_date: Mapping[str, ExpiryCalendarEntry],
    config: PlatformConfig,
) -> ValidatedOptionQuote:
    """Validate a single OptionQuote deterministically without any side effects."""
    if quote.schema_version not in config.validation.supported_schema_versions:
        raise UnsupportedInputError(
            f"Unsupported option quote schema_version '{quote.schema_version}'. "
            f"Supported: {list(config.validation.supported_schema_versions)}"
        )

    flags: List[DataQualityFlag] = []
    reasons: List[str] = []
    rejected = False

    staleness_seconds = (snapshot_time - quote.event_time).total_seconds()
    ingestion_delay_seconds = (quote.ingestion_time - quote.event_time).total_seconds()

    # 1. Future-dated quote relative to snapshot_time
    if staleness_seconds < -config.validation.max_future_clock_skew_seconds:
        flags.append(DataQualityFlag.FUTURE_DATED_QUOTE)
        reasons.append(
            f"Quote event_time exceeds snapshot_time by {-staleness_seconds:.2f}s "
            f"(limit {config.validation.max_future_clock_skew_seconds}s)"
        )
        rejected = True

    # 2. Contract existence and contract-level checks
    if contract is None:
        flags.append(DataQualityFlag.MISSING_CONTRACT)
        reasons.append(f"No contract metadata registered for {quote.option_id}")
        rejected = True
    else:
        # 2a. Unsupported exercise style
        if contract.exercise_style not in config.validation.supported_exercise_styles:
            flags.append(DataQualityFlag.UNSUPPORTED_EXERCISE_STYLE)
            reasons.append(
                f"Exercise style '{contract.exercise_style}' is not in supported styles "
                f"{list(config.validation.supported_exercise_styles)}"
            )
            rejected = True

        # 2b. Strike / decimal precision mismatch
        if quote.reported_strike <= Decimal("0"):
            flags.append(DataQualityFlag.STRIKE_DECIMAL_MISMATCH)
            reasons.append(f"Non-positive reported_strike={quote.reported_strike}")
            rejected = True
        else:
            reported_dp = decimal_places(quote.reported_strike)
            if reported_dp > contract.strike_scale_decimals:
                flags.append(DataQualityFlag.STRIKE_DECIMAL_MISMATCH)
                reasons.append(
                    f"Reported strike {quote.reported_strike} has {reported_dp} decimal places, "
                    f"exceeding contract strike_scale_decimals={contract.strike_scale_decimals}"
                )
                rejected = True
            elif quote.reported_strike != contract.strike:
                flags.append(DataQualityFlag.STRIKE_DECIMAL_MISMATCH)
                reasons.append(
                    f"Reported strike {quote.reported_strike} does not match contract strike {contract.strike}"
                )
                rejected = True

        # 2c. Expiry check (check against both quote event_time and snapshot_time)
        expiry_ts, _ = resolve_expiry_timestamp(contract, calendar_by_date, config)
        if quote.event_time >= expiry_ts or snapshot_time >= expiry_ts:
            flags.append(DataQualityFlag.EXPIRED_OPTION)
            reasons.append(
                f"Option expired at {expiry_ts.isoformat()} (event_time={quote.event_time.isoformat()}, "
                f"snapshot_time={snapshot_time.isoformat()})"
            )
            rejected = True

    # 3. Price validity and crossed/locked checks
    if quote.bid < Decimal("0") or quote.ask <= Decimal("0"):
        flags.append(DataQualityFlag.INVALID_PRICE)
        reasons.append(f"Invalid quote price: bid={quote.bid}, ask={quote.ask}")
        rejected = True
    elif quote.bid > quote.ask:
        flags.append(DataQualityFlag.CROSSED_QUOTE)
        reasons.append(f"Crossed quote: bid={quote.bid} > ask={quote.ask}")
        rejected = True
    elif quote.bid == quote.ask and quote.bid > Decimal("0"):
        flags.append(DataQualityFlag.LOCKED_QUOTE)
        reasons.append(f"Locked quote: bid == ask == {quote.bid}")
        if not config.validation.allow_locked_quotes:
            rejected = True

    # 4. Size validity
    if quote.bid_size < 0 or quote.ask_size < 0:
        flags.append(DataQualityFlag.INVALID_SIZE)
        reasons.append(f"Negative quote size: bid_size={quote.bid_size}, ask_size={quote.ask_size}")
        rejected = True

    # 5. Staleness check (snapshot_time - event_time)
    if staleness_seconds > config.validation.max_quote_staleness_seconds:
        flags.append(DataQualityFlag.STALE_QUOTE)
        reasons.append(
            f"Quote is stale by {staleness_seconds:.2f}s "
            f"(max {config.validation.max_quote_staleness_seconds}s)"
        )
        if config.validation.exclude_stale_quotes_from_iv:
            rejected = True

    # 6. Ingestion delay check (ingestion_time - event_time)
    if ingestion_delay_seconds > config.validation.max_ingestion_delay_seconds:
        flags.append(DataQualityFlag.DELAYED_QUOTE)
        reasons.append(
            f"Quote ingestion delayed by {ingestion_delay_seconds:.2f}s "
            f"(max {config.validation.max_ingestion_delay_seconds}s)"
        )

    if rejected:
        status = ValidationStatus.REJECTED
    elif flags:
        status = ValidationStatus.FLAGGED
    else:
        status = ValidationStatus.VALID

    return ValidatedOptionQuote(
        quote=quote,
        status=status,
        flags=tuple(flags),
        reasons=tuple(reasons),
        ingestion_delay_seconds=ingestion_delay_seconds,
        staleness_seconds=staleness_seconds,
    )


def validate_and_build_chain_snapshot(
    snapshot_id: str,
    underlying_symbol: str,
    snapshot_time: datetime,
    ingestion_time: datetime,
    source_id: str,
    schema_version: str,
    quotes: Sequence[OptionQuote],
    contracts_by_id: Mapping[str, ContractMetadata],
    calendar_by_date: Mapping[str, ExpiryCalendarEntry],
    config: PlatformConfig,
) -> ChainSnapshot:
    """Validate structural snapshot consistency and every quote, returning an immutable ChainSnapshot.

    Raises SnapshotConsistencyError on structural violations:
    - Empty snapshot batch
    - Quote snapshot_id mismatch
    - Mixed underlying symbols in the same chain snapshot
    - Duplicate (option_id, quote_version) in the same snapshot
    - Option ID embedded underlying mismatch with quote.underlying_symbol
    """
    if schema_version not in config.validation.supported_schema_versions:
        raise UnsupportedInputError(
            f"Unsupported snapshot schema_version '{schema_version}'. "
            f"Supported: {list(config.validation.supported_schema_versions)}"
        )

    target_symbol = underlying_symbol.strip().upper()
    if not quotes:
        raise SnapshotConsistencyError(
            f"Snapshot '{snapshot_id}' contains zero quotes",
            code="EMPTY_SNAPSHOT",
        )

    seen_keys: Dict[Tuple[str, int], int] = {}
    validated: List[ValidatedOptionQuote] = []

    for idx, q in enumerate(quotes):
        if q.snapshot_id != snapshot_id:
            raise SnapshotConsistencyError(
                f"Quote at index {idx} has snapshot_id='{q.snapshot_id}', expected '{snapshot_id}'",
                code="SNAPSHOT_ID_MISMATCH",
            )
        if q.underlying_symbol != target_symbol:
            raise SnapshotConsistencyError(
                f"Quote {q.option_id} has underlying_symbol='{q.underlying_symbol}', "
                f"expected '{target_symbol}' for snapshot '{snapshot_id}'",
                code="MIXED_UNDERLYING_SNAPSHOT",
            )
        parsed_id = CanonicalOptionID.parse(q.option_id)
        if parsed_id.underlying_symbol != target_symbol:
            raise SnapshotConsistencyError(
                f"Canonical option_id '{q.option_id}' embeds underlying '{parsed_id.underlying_symbol}', "
                f"mismatching snapshot underlying '{target_symbol}'",
                code="OPTION_ID_UNDERLYING_MISMATCH",
            )
        key = (q.option_id, q.quote_version)
        if key in seen_keys:
            raise SnapshotConsistencyError(
                f"Duplicate (option_id, quote_version)={key} in snapshot '{snapshot_id}' "
                f"(indices {seen_keys[key]} and {idx})",
                code="DUPLICATE_QUOTE_VERSION",
            )
        seen_keys[key] = idx

        contract = contracts_by_id.get(q.option_id)
        validated.append(
            validate_option_quote(
                quote=q,
                snapshot_time=snapshot_time,
                contract=contract,
                calendar_by_date=calendar_by_date,
                config=config,
            )
        )

    return ChainSnapshot(
        snapshot_id=snapshot_id,
        underlying_symbol=target_symbol,
        snapshot_time=snapshot_time,
        ingestion_time=ingestion_time,
        source_id=source_id,
        schema_version=schema_version,
        config_hash=config.config_hash(),
        quotes=tuple(validated),
    )
