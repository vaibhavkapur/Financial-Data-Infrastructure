"""Data quality validation rules for normalized and curated layers."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from financial_data_lake.config import QualityConfig
from financial_data_lake.models import QualityCheckResult, QualityReport


class QualityEngine:
    """Evaluates deterministic quality checks on batches of canonical records."""

    def __init__(self, config: QualityConfig) -> None:
        self.config = config

    def evaluate(
        self,
        *,
        dataset_id: str,
        layer: str,
        ingestion_run_id: str,
        records: list[dict[str, Any]],
        required_non_null_fields: list[str],
        dedupe_key: list[str],
        duplicates_removed: int = 0,
    ) -> QualityReport:
        checks: list[QualityCheckResult] = []
        total = len(records)

        # 1. Non-empty batch check
        checks.append(
            QualityCheckResult(
                rule_name="non_empty_batch",
                severity="CRITICAL",
                passed=total > 0,
                message="Batch contains at least one record." if total > 0 else "Batch is empty.",
                affected_rows=0 if total > 0 else 1,
            )
        )

        # 2. Required non-null fields check
        null_violations = 0
        null_fields: dict[str, int] = {}
        for row in records:
            for field in required_non_null_fields:
                if row.get(field) is None:
                    null_violations += 1
                    null_fields[field] = null_fields.get(field, 0) + 1
        null_ratio = (null_violations / (total * max(len(required_non_null_fields), 1))) if total else 0.0
        checks.append(
            QualityCheckResult(
                rule_name="required_fields_non_null",
                severity="CRITICAL",
                passed=null_ratio <= self.config.max_null_ratio,
                message=(
                    "All required fields populated."
                    if null_violations == 0
                    else f"Found {null_violations} null values in required fields."
                ),
                affected_rows=null_violations,
                details={"null_counts": null_fields},
            )
        )

        # 3. Bitemporal timestamp ordering (available_at >= event_ts) & UTC timezone check
        ts_violations = 0
        for row in records:
            ev = row.get("event_ts")
            av = row.get("available_at")
            if not isinstance(ev, datetime) or not isinstance(av, datetime):
                ts_violations += 1
                continue
            if self.config.require_utc_timestamps and (ev.tzinfo is None or av.tzinfo is None):
                ts_violations += 1
                continue
            if self.config.require_availability_ge_event and av < ev:
                ts_violations += 1
        checks.append(
            QualityCheckResult(
                rule_name="bitemporal_utc_ordering",
                severity="CRITICAL",
                passed=ts_violations == 0,
                message=(
                    "All timestamps are UTC with available_at >= event_ts."
                    if ts_violations == 0
                    else f"{ts_violations} rows violated UTC or bitemporal ordering."
                ),
                affected_rows=ts_violations,
            )
        )

        # 4. Post-dedupe uniqueness check on dedupe_key
        seen: set[tuple[Any, ...]] = set()
        dup_remaining = 0
        for row in records:
            key = tuple(row.get(k) for k in dedupe_key)
            if key in seen:
                dup_remaining += 1
            seen.add(key)
        checks.append(
            QualityCheckResult(
                rule_name="primary_key_uniqueness",
                severity="CRITICAL",
                passed=dup_remaining == 0,
                message=(
                    f"All {total} records unique on {dedupe_key} ({duplicates_removed} input duplicates removed)."
                    if dup_remaining == 0
                    else f"{dup_remaining} duplicate keys remain after deduplication."
                ),
                affected_rows=dup_remaining,
                details={"input_duplicates_removed": duplicates_removed},
            )
        )

        # 5. Domain-specific checks (positive prices / OHLC / corporate action ratios)
        domain_violations = 0
        for row in records:
            if dataset_id == "market_ticks":
                if row.get("price", 0) <= 0 or row.get("size", 0) <= 0:
                    domain_violations += 1
            elif dataset_id == "prices_eod":
                o, h, l_val, c = row["open"], row["high"], row["low"], row["close"]
                if min(o, h, l_val, c) <= 0 or h < max(o, c, l_val) or l_val > min(o, c, h):
                    domain_violations += 1
            elif dataset_id == "corporate_actions":
                if row["action_type"] == "SPLIT" and (
                    (row.get("split_numerator") or 0) <= 0 or (row.get("split_denominator") or 0) <= 0
                ):
                    domain_violations += 1
                if row["action_type"] == "CASH_DIVIDEND" and (row.get("cash_amount") or 0) <= 0:
                    domain_violations += 1
        checks.append(
            QualityCheckResult(
                rule_name="domain_invariants",
                severity="CRITICAL",
                passed=domain_violations == 0,
                message=(
                    "All dataset domain invariants satisfied."
                    if domain_violations == 0
                    else f"{domain_violations} rows failed domain invariants."
                ),
                affected_rows=domain_violations,
            )
        )

        # 6. Revision monotonicity (if multiple revisions of same entity exist, higher revision must have >= available_at)
        rev_violations = 0
        if dataset_id in {"market_ticks", "prices_eod", "fundamentals"}:
            entity_cols = [k for k in dedupe_key if k != "revision_number"]
            grouped: dict[tuple[Any, ...], list[tuple[int, datetime]]] = {}
            for row in records:
                ekey = tuple(row.get(k) for k in entity_cols)
                grouped.setdefault(ekey, []).append((int(row["revision_number"]), row["available_at"]))
            for revs in grouped.values():
                revs.sort(key=lambda x: x[0])
                for i in range(1, len(revs)):
                    if revs[i][1] < revs[i - 1][1]:
                        rev_violations += 1
        checks.append(
            QualityCheckResult(
                rule_name="revision_availability_monotonicity",
                severity="CRITICAL",
                passed=rev_violations == 0,
                message=(
                    "Revisions have monotonically non-decreasing availability timestamps."
                    if rev_violations == 0
                    else f"{rev_violations} revision monotonicity violations detected."
                ),
                affected_rows=rev_violations,
            )
        )

        passed = all(c.passed for c in checks if c.severity == "CRITICAL")
        return QualityReport(
            dataset_id=dataset_id,
            layer=layer,  # type: ignore[arg-type]
            ingestion_run_id=ingestion_run_id,
            checked_at=datetime.now(UTC),
            total_rows=total,
            passed=passed,
            checks=checks,
        )
