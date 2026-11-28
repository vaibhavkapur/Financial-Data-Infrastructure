"""Pure domain data-quality reporting for chain snapshots and derived features."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from typing import Optional, Sequence

from omdp.domain.contracts import (
    ChainSnapshot,
    DerivedOptionFeature,
    IVStatus,
    QualityReport,
    ValidationStatus,
)


def build_quality_report(
    snapshot: ChainSnapshot,
    features: Sequence[DerivedOptionFeature],
    generated_at: Optional[datetime] = None,
) -> QualityReport:
    """Compute completeness, freshness, and diagnostic breakdown for a snapshot."""
    ts = generated_at if generated_at is not None else datetime.now(timezone.utc)
    total = snapshot.quote_count
    valid = snapshot.valid_quote_count
    flagged = snapshot.flagged_quote_count
    rejected = snapshot.rejected_quote_count

    flag_counter: Counter[str] = Counter()
    max_staleness = 0.0
    max_delay = 0.0

    for vq in snapshot.quotes:
        if vq.staleness_seconds > max_staleness:
            max_staleness = vq.staleness_seconds
        if vq.ingestion_delay_seconds > max_delay:
            max_delay = vq.ingestion_delay_seconds
        for flag in vq.flags:
            flag_counter[flag.value] += 1

    # Also incorporate feature-level flags (e.g., MISSING_UNDERLYING, STALE_UNDERLYING, MISSING_RATE_CURVE)
    iv_counter: Counter[str] = Counter()
    computed_iv = 0
    for feat in features:
        iv_counter[feat.iv_status.value] += 1
        if feat.iv_status == IVStatus.COMPUTED:
            computed_iv += 1
        for flag in feat.quality_flags:
            if flag.value not in ("EXPIRED_OPTION", "STRIKE_DECIMAL_MISMATCH", "CROSSED_QUOTE",
                                  "LOCKED_QUOTE", "INVALID_PRICE", "INVALID_SIZE", "STALE_QUOTE",
                                  "DELAYED_QUOTE", "FUTURE_DATED_QUOTE", "MISSING_CONTRACT",
                                  "UNSUPPORTED_EXERCISE_STYLE"):
                flag_counter[flag.value] += 1

    usable_quotes = valid + flagged
    completeness_ratio = (usable_quotes / float(total)) if total > 0 else 0.0
    feature_total = len(features)
    iv_coverage_ratio = (computed_iv / float(feature_total)) if feature_total > 0 else 0.0

    return QualityReport(
        snapshot_id=snapshot.snapshot_id,
        underlying_symbol=snapshot.underlying_symbol,
        snapshot_time=snapshot.snapshot_time,
        generated_at=ts,
        total_quotes=total,
        valid_quotes=valid,
        flagged_quotes=flagged,
        rejected_quotes=rejected,
        computed_iv_count=computed_iv,
        completeness_ratio=completeness_ratio,
        iv_coverage_ratio=iv_coverage_ratio,
        max_quote_staleness_seconds=max_staleness,
        max_ingestion_delay_seconds=max_delay,
        flag_counts=dict(flag_counter),
        iv_status_counts=dict(iv_counter),
        snapshot_hash=snapshot.snapshot_hash,
    )
