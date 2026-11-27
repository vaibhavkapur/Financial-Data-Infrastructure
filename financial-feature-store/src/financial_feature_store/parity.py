"""Training/serving parity verification and skew detection harness."""

from __future__ import annotations

import math
from collections.abc import Callable
from datetime import datetime
from typing import Any

import polars as pl

from financial_feature_store.adapters.online_backends import InMemoryOnlineBackend, OnlineBackend
from financial_feature_store.contracts import (
    FeatureRef,
    OnlineFeatureVector,
    ParityDiffDetail,
    ParityReport,
    StalePolicy,
    ensure_utc,
)
from financial_feature_store.offline import OfflineFeatureStore
from financial_feature_store.online import OnlineFeatureStore
from financial_feature_store.registry import FeatureRegistry


def _values_match(a: Any, b: Any, *, atol: float, rtol: float) -> tuple[bool, float | None]:
    if a is None and b is None:
        return True, 0.0
    if a is None or b is None:
        return False, None
    if isinstance(a, bool) or isinstance(b, bool):
        return bool(a) == bool(b), 0.0 if bool(a) == bool(b) else 1.0
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        af, bf = float(a), float(b)
        diff = abs(af - bf)
        return math.isclose(af, bf, rel_tol=rtol, abs_tol=atol), diff
    return a == b, 0.0 if a == b else None


class ParityVerifier:
    """Verifies exact training (offline as-of) vs serving (online replay) parity."""

    def __init__(
        self,
        offline_store: OfflineFeatureStore,
        registry: FeatureRegistry,
        *,
        atol: float = 1e-9,
        rtol: float = 1e-9,
    ) -> None:
        self.offline_store = offline_store
        self.registry = registry
        self.atol = atol
        self.rtol = rtol

    def compare_vectors(
        self,
        offline_joined_df: pl.DataFrame,
        online_vectors: dict[tuple[str, datetime], OnlineFeatureVector],
        feature_refs: list[FeatureRef | str],
    ) -> ParityReport:
        """Compare offline point-in-time joined rows against captured online feature vectors."""
        resolved = [self.registry.resolve(r) for r in feature_refs]
        total = 0
        matched = 0
        null_mismatches = 0
        version_mismatches = 0
        max_abs_diff = 0.0
        diffs: list[ParityDiffDetail] = []

        for row in offline_joined_df.to_dicts():
            ent = str(row["entity_id"])
            as_of = ensure_utc(row["as_of_time"], "as_of_time")
            vec = online_vectors.get((ent, as_of))

            for feat in resolved:
                total += 1
                off_val = row.get(feat.name)
                off_vid = row.get(f"{feat.name}__version_id")

                if vec is None or feat.name not in vec.features:
                    diffs.append(
                        ParityDiffDetail(
                            entity_id=ent,
                            as_of_time=as_of,
                            feature_name=feat.name,
                            offline_value=off_val,
                            online_value=None,
                            offline_version_id=off_vid,
                            online_version_id=None,
                            abs_diff=None,
                            reason="missing_online_vector",
                        )
                    )
                    continue

                on_feat = vec.features[feat.name]
                on_val = on_feat.value
                on_vid = on_feat.version_id if on_feat.event_time is not None else None

                if off_vid is not None and on_vid is not None and off_vid != on_vid:
                    version_mismatches += 1
                    diffs.append(
                        ParityDiffDetail(
                            entity_id=ent,
                            as_of_time=as_of,
                            feature_name=feat.name,
                            offline_value=off_val,
                            online_value=on_val,
                            offline_version_id=off_vid,
                            online_version_id=on_vid,
                            abs_diff=None,
                            reason="version_id_mismatch",
                        )
                    )
                    continue

                is_eq, abs_diff = _values_match(off_val, on_val, atol=self.atol, rtol=self.rtol)
                if abs_diff is not None and abs_diff > max_abs_diff:
                    max_abs_diff = abs_diff

                if is_eq:
                    matched += 1
                else:
                    if (off_val is None) != (on_val is None):
                        null_mismatches += 1
                        reason = "null_mismatch"
                    else:
                        reason = "value_skew"
                    diffs.append(
                        ParityDiffDetail(
                            entity_id=ent,
                            as_of_time=as_of,
                            feature_name=feat.name,
                            offline_value=off_val,
                            online_value=on_val,
                            offline_version_id=off_vid,
                            online_version_id=on_vid,
                            abs_diff=abs_diff,
                            reason=reason,
                        )
                    )

        mismatched = total - matched
        return ParityReport(
            passed=(mismatched == 0),
            total_comparisons=total,
            matched_count=matched,
            mismatched_count=mismatched,
            null_mismatches=null_mismatches,
            version_mismatches=version_mismatches,
            max_abs_diff=max_abs_diff,
            atol=self.atol,
            rtol=self.rtol,
            diffs=tuple(diffs[:50]),
        )

    def verify_replay_parity(
        self,
        spine_df: pl.DataFrame,
        feature_refs: list[FeatureRef | str],
        raw_sources_by_feature: dict[str, pl.DataFrame],
        *,
        backend_factory: Callable[[], OnlineBackend] = InMemoryOnlineBackend,
        enforce_ttl: bool = False,
        allow_non_active: bool = False,
    ) -> ParityReport:
        """Replay chronological events into an online store and verify exact offline/online parity."""
        offline_df, _audit = self.offline_store.point_in_time_join(
            spine_df,
            feature_refs,
            enforce_ttl=enforce_ttl,
            include_metadata=True,
        )

        online_store = OnlineFeatureStore(backend_factory(), self.registry)
        resolved = [self.registry.resolve(r) for r in feature_refs]
        stale_policy = StalePolicy.FALLBACK_NULL if enforce_ttl else StalePolicy.WARN

        # Chronologically replay each unique as_of_time in the spine
        spine_rows = offline_df.select(["entity_id", "as_of_time"]).to_dicts()
        entities_by_time: dict[datetime, list[str]] = {}
        for row in spine_rows:
            ts = ensure_utc(row["as_of_time"], "as_of_time")
            entities_by_time.setdefault(ts, []).append(str(row["entity_id"]))

        captured: dict[tuple[str, datetime], OnlineFeatureVector] = {}
        for as_of in sorted(entities_by_time):
            for feat in resolved:
                raw_df = raw_sources_by_feature[feat.name]
                online_store.materialize_and_sync_from_raw(
                    feat,
                    raw_df,
                    as_of_time=as_of,
                    allow_non_active=allow_non_active,
                )
            for ent in entities_by_time[as_of]:
                captured[(ent, as_of)] = online_store.get_online_features(
                    ent,
                    feature_refs,
                    now=as_of,
                    stale_policy_override=stale_policy,
                )

        return self.compare_vectors(offline_df, captured, feature_refs)
