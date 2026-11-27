"""Low-latency online feature serving with freshness/TTL policies, version checks, and metrics."""

from __future__ import annotations

import time
import uuid
from datetime import UTC, datetime
from typing import Any

import polars as pl

from financial_feature_store.adapters.online_backends import OnlineBackend
from financial_feature_store.contracts import (
    FeatureDefinition,
    FeatureRecord,
    FeatureRef,
    FreshnessStatus,
    LifecycleStage,
    NullPolicy,
    OnlineFeatureValue,
    OnlineFeatureVector,
    StalePolicy,
    ensure_utc,
)
from financial_feature_store.errors import (
    LeakageViolationError,
    NullPolicyViolationError,
    QualityGateError,
    StaleFeatureError,
    VersionMismatchError,
)
from financial_feature_store.quality import LineageTracker
from financial_feature_store.registry import FeatureRegistry
from financial_feature_store.transforms import compute_causal_feature_frame


class OnlineFeatureStore:
    """Online feature serving layer exposing explicit freshness, TTL enforcement, and metrics."""

    def __init__(
        self,
        backend: OnlineBackend,
        registry: FeatureRegistry,
        *,
        lineage: LineageTracker | None = None,
    ) -> None:
        self.backend = backend
        self.registry = registry
        self.lineage = lineage or LineageTracker()
        self._metrics: dict[str, Any] = {
            "requests_total": 0,
            "feature_lookups_total": 0,
            "hits": 0,
            "misses": 0,
            "stale_count": 0,
            "version_mismatch_count": 0,
            "leakage_blocked_count": 0,
            "latencies_ms": [],
        }

    def sync_feature_frame(
        self,
        feature_ref: FeatureRef | FeatureDefinition | str,
        feature_df: pl.DataFrame,
        *,
        as_of_time: datetime | None = None,
        allow_non_active: bool = False,
    ) -> int:
        """Promote the latest causal feature values per entity into the online backend."""
        feat = (
            feature_ref
            if isinstance(feature_ref, FeatureDefinition)
            else self.registry.resolve(feature_ref)
        )
        if not allow_non_active and feat.stage != LifecycleStage.ACTIVE:
            raise QualityGateError(
                f"Cannot sync '{feat.name}@{feat.version}' to online store: stage is '{feat.stage.value}', expected 'active'",
                details={
                    "feature_name": feat.name,
                    "version": feat.version,
                    "stage": feat.stage.value,
                },
            )

        working = feature_df
        if as_of_time is not None:
            cutoff = ensure_utc(as_of_time, "as_of_time")
            working = working.filter(
                (pl.col("availability_time") <= cutoff) & (pl.col("event_time") <= cutoff)
            )

        if working.height == 0:
            return 0

        # Validate version_id before writing to online serving
        stored_vids = set(working["version_id"].unique().to_list())
        if stored_vids != {feat.version_id}:
            self._metrics["version_mismatch_count"] += 1
            raise VersionMismatchError(
                f"Refusing to sync mismatched version_id into online store for '{feat.name}@{feat.version}'",
                details={
                    "expected_version_id": feat.version_id,
                    "incoming_version_ids": sorted(stored_vids),
                },
            )

        # Pick strictly latest (event_time, availability_time, materialized_at) per entity
        latest_df = (
            working.sort(["entity_id", "event_time", "availability_time", "materialized_at"])
            .group_by("entity_id", maintain_order=True)
            .tail(1)
        )

        records = [FeatureRecord.model_validate(row) for row in latest_df.to_dicts()]
        written = self.backend.put_batch(records)

        online_node = f"online_store:{feat.name}@{feat.version}"
        self.lineage.record_feature_definition(feat)
        self.lineage.record_node(
            online_node,
            "online_serving_store",
            {
                "backend": self.backend.health().get("backend", "unknown"),
                "records_synced": written,
            },
        )
        self.lineage.record_edge(f"feature:{feat.version_id}", online_node, "synced_to_online")
        return written

    def materialize_and_sync_from_raw(
        self,
        feature_ref: FeatureRef | FeatureDefinition | str,
        raw_df: pl.DataFrame,
        *,
        as_of_time: datetime | None = None,
        run_id: str | None = None,
        materialized_at: datetime | None = None,
        allow_non_active: bool = False,
    ) -> int:
        """Compute features using the shared causal transform engine and sync to online store."""
        feat = (
            feature_ref
            if isinstance(feature_ref, FeatureDefinition)
            else self.registry.resolve(feature_ref)
        )
        filtered = raw_df
        if as_of_time is not None:
            cutoff = ensure_utc(as_of_time, "as_of_time")
            filtered = raw_df.filter(
                (pl.col("availability_time") <= cutoff) & (pl.col("event_time") <= cutoff)
            )
        if filtered.height == 0:
            return 0
        computed = compute_causal_feature_frame(
            filtered,
            feat,
            run_id=run_id or f"online-{uuid.uuid4().hex[:10]}",
            materialized_at=materialized_at or as_of_time or datetime.now(UTC),
        )
        return self.sync_feature_frame(
            feat,
            computed,
            as_of_time=as_of_time,
            allow_non_active=allow_non_active,
        )

    def get_online_features(
        self,
        entity_id: str,
        feature_refs: list[FeatureRef | str],
        *,
        now: datetime | None = None,
        stale_policy_override: StalePolicy | None = None,
    ) -> OnlineFeatureVector:
        """Fetch latest online features with explicit freshness, TTL enforcement, and version pinning."""
        t0 = time.perf_counter()
        served_at = ensure_utc(now or datetime.now(UTC), "now")
        self._metrics["requests_total"] += 1

        results: dict[str, OnlineFeatureValue] = {}
        stale_names: list[str] = []
        missing_names: list[str] = []

        for ref in feature_refs:
            self._metrics["feature_lookups_total"] += 1
            feat = self.registry.resolve(ref)
            rec = self.backend.get(entity_id, feat.name, feat.version)

            if rec is None:
                self._metrics["misses"] += 1
                missing_names.append(feat.name)
                if feat.null_policy == NullPolicy.REJECT:
                    raise NullPolicyViolationError(
                        f"Online feature '{feat.name}@{feat.version}' missing for '{entity_id}' under REJECT policy",
                        details={"entity_id": entity_id, "feature_name": feat.name, "version": feat.version},
                    )
                fallback_val = feat.default_value if feat.null_policy == NullPolicy.DEFAULT_VALUE else None
                results[feat.name] = OnlineFeatureValue(
                    entity_id=entity_id,
                    feature_name=feat.name,
                    feature_version=feat.version,
                    version_id=feat.version_id,
                    value=fallback_val,
                    event_time=None,
                    availability_time=None,
                    served_at=served_at,
                    age_seconds=None,
                    availability_age_seconds=None,
                    ttl_seconds=feat.ttl_seconds,
                    freshness_status=FreshnessStatus.MISSING,
                    is_stale=True,
                    unit=feat.unit,
                    warnings=("feature_missing_in_online_store",),
                )
                continue

            # Version integrity check: cached record MUST match registered version_id
            if rec.version_id != feat.version_id:
                self._metrics["version_mismatch_count"] += 1
                raise VersionMismatchError(
                    f"Online cache version_id mismatch for '{feat.name}@{feat.version}'",
                    details={
                        "entity_id": entity_id,
                        "expected_version_id": feat.version_id,
                        "cached_version_id": rec.version_id,
                    },
                )

            # Leakage guard: online store must never serve a future event or unavailable record
            if rec.event_time > served_at or rec.availability_time > served_at:
                self._metrics["leakage_blocked_count"] += 1
                raise LeakageViolationError(
                    f"Online record for '{feat.name}' is in the future relative to served_at",
                    details={
                        "entity_id": entity_id,
                        "served_at": served_at.isoformat(),
                        "event_time": rec.event_time.isoformat(),
                        "availability_time": rec.availability_time.isoformat(),
                    },
                )

            self._metrics["hits"] += 1
            age_sec = (served_at - rec.event_time).total_seconds()
            avail_age_sec = (served_at - rec.availability_time).total_seconds()
            is_stale = age_sec > feat.ttl_seconds
            warnings: list[str] = []
            served_val = rec.value
            status = FreshnessStatus.FRESH

            if is_stale:
                self._metrics["stale_count"] += 1
                stale_names.append(feat.name)
                status = FreshnessStatus.STALE
                policy = stale_policy_override or feat.stale_policy
                if policy == StalePolicy.REJECT:
                    raise StaleFeatureError(
                        f"Online feature '{feat.name}@{feat.version}' is stale (age={age_sec:.2f}s > ttl={feat.ttl_seconds:.2f}s)",
                        details={
                            "entity_id": entity_id,
                            "feature_name": feat.name,
                            "version": feat.version,
                            "age_seconds": age_sec,
                            "ttl_seconds": feat.ttl_seconds,
                            "event_time": rec.event_time.isoformat(),
                            "served_at": served_at.isoformat(),
                        },
                    )
                if policy == StalePolicy.FALLBACK_NULL:
                    served_val = None
                    warnings.append(
                        f"stale_fallback_null: age={age_sec:.2f}s exceeded ttl={feat.ttl_seconds:.2f}s"
                    )
                else:
                    warnings.append(
                        f"stale_warn: age={age_sec:.2f}s exceeded ttl={feat.ttl_seconds:.2f}s"
                    )

            results[feat.name] = OnlineFeatureValue(
                entity_id=entity_id,
                feature_name=feat.name,
                feature_version=feat.version,
                version_id=rec.version_id,
                value=served_val,
                event_time=rec.event_time,
                availability_time=rec.availability_time,
                served_at=served_at,
                age_seconds=age_sec,
                availability_age_seconds=avail_age_sec,
                ttl_seconds=feat.ttl_seconds,
                freshness_status=status,
                is_stale=is_stale,
                unit=feat.unit,
                warnings=tuple(warnings),
            )

        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        self._metrics["latencies_ms"].append(elapsed_ms)

        return OnlineFeatureVector(
            entity_id=entity_id,
            requested_at=served_at,
            features=results,
            all_fresh=(len(stale_names) == 0 and len(missing_names) == 0),
            stale_features=tuple(stale_names),
            missing_features=tuple(missing_names),
        )

    def metrics(self) -> dict[str, Any]:
        latencies: list[float] = list(self._metrics["latencies_ms"])
        latencies_sorted = sorted(latencies)
        n = len(latencies_sorted)

        def _pct(p: float) -> float:
            if n == 0:
                return 0.0
            idx = min(n - 1, int(p * n))
            return round(latencies_sorted[idx], 4)

        return {
            "requests_total": self._metrics["requests_total"],
            "feature_lookups_total": self._metrics["feature_lookups_total"],
            "hits": self._metrics["hits"],
            "misses": self._metrics["misses"],
            "stale_count": self._metrics["stale_count"],
            "version_mismatch_count": self._metrics["version_mismatch_count"],
            "leakage_blocked_count": self._metrics["leakage_blocked_count"],
            "latency_ms_p50": _pct(0.50),
            "latency_ms_p95": _pct(0.95),
            "latency_ms_p99": _pct(0.99),
            "backend_health": self.backend.health(),
        }
