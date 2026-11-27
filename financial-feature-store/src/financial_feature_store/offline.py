"""Offline materialization, point-in-time as-of joins with leakage barriers, backfill, and dataset export."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl

from financial_feature_store.adapters.sources import compute_file_sha256
from financial_feature_store.contracts import (
    BackfillReport,
    FeatureDefinition,
    FeatureRef,
    MaterializationManifest,
    NullPolicy,
    TrainingDatasetManifest,
    ensure_utc,
)
from financial_feature_store.errors import (
    BackfillError,
    ExportError,
    LeakageViolationError,
    NullPolicyViolationError,
    TimestampContractError,
    VersionMismatchError,
)
from financial_feature_store.quality import LineageTracker
from financial_feature_store.registry import FeatureRegistry
from financial_feature_store.transforms import compute_causal_feature_frame


class OfflineFeatureStore:
    """Versioned Parquet offline store enforcing strict point-in-time causality."""

    def __init__(
        self,
        root_dir: str | Path,
        registry: FeatureRegistry,
        *,
        lineage: LineageTracker | None = None,
    ) -> None:
        self.root_dir = Path(root_dir)
        self.root_dir.mkdir(parents=True, exist_ok=True)
        self.registry = registry
        self.lineage = lineage or LineageTracker()

    def _feature_dir(self, feature_name: str, feature_version: str) -> Path:
        return self.root_dir / feature_name / feature_version

    def materialize(
        self,
        feature_ref: FeatureRef | FeatureDefinition | str,
        raw_df: pl.DataFrame,
        *,
        run_id: str | None = None,
        materialized_at: datetime | None = None,
        as_of_cutoff: datetime | None = None,
    ) -> tuple[pl.DataFrame, MaterializationManifest]:
        """Compute and persist immutable versioned feature records to Parquet + manifest."""
        feat = (
            feature_ref
            if isinstance(feature_ref, FeatureDefinition)
            else self.registry.resolve(feature_ref)
        )
        effective_run_id = run_id or f"run-{uuid.uuid4().hex[:12]}"
        mat_ts = ensure_utc(materialized_at or datetime.now(UTC), "materialized_at")

        working_raw = raw_df
        if as_of_cutoff is not None:
            cutoff = ensure_utc(as_of_cutoff, "as_of_cutoff")
            working_raw = working_raw.filter(
                (pl.col("availability_time") <= cutoff) & (pl.col("event_time") <= cutoff)
            )

        computed = compute_causal_feature_frame(
            working_raw,
            feat,
            run_id=effective_run_id,
            materialized_at=mat_ts,
        )

        # Idempotent deduplication on (entity_id, event_time, availability_time, version_id)
        deduped = computed.unique(
            subset=["entity_id", "event_time", "availability_time", "version_id"],
            keep="last",
            maintain_order=True,
        ).sort(["entity_id", "event_time", "availability_time"])

        out_dir = self._feature_dir(feat.name, feat.version)
        out_dir.mkdir(parents=True, exist_ok=True)
        pq_path = out_dir / f"{effective_run_id}.parquet"
        deduped.write_parquet(pq_path)

        manifest = self._build_manifest(
            feat=feat,
            df=deduped,
            run_id=effective_run_id,
            pq_path=pq_path,
            mat_ts=mat_ts,
        )
        manifest_path = out_dir / f"{effective_run_id}.manifest.json"
        manifest_path.write_text(manifest.model_dump_json(indent=2), encoding="utf-8")

        self.lineage.record_feature_definition(feat)
        mat_node = f"offline_run:{feat.name}@{feat.version}:{effective_run_id}"
        self.lineage.record_node(
            mat_node,
            "offline_materialization",
            {
                "run_id": effective_run_id,
                "parquet_path": str(pq_path),
                "sha256_checksum": manifest.sha256_checksum,
                "row_count": manifest.row_count,
            },
        )
        self.lineage.record_edge(f"feature:{feat.version_id}", mat_node, "materialized_to")

        return deduped, manifest

    def _build_manifest(
        self,
        *,
        feat: FeatureDefinition,
        df: pl.DataFrame,
        run_id: str,
        pq_path: Path,
        mat_ts: datetime,
        is_backfill: bool = False,
        window_start: datetime | None = None,
        window_end: datetime | None = None,
    ) -> MaterializationManifest:
        min_ev = df["event_time"].min() if df.height > 0 else None
        max_ev = df["event_time"].max() if df.height > 0 else None
        min_av = df["availability_time"].min() if df.height > 0 else None
        max_av = df["availability_time"].max() if df.height > 0 else None
        return MaterializationManifest(
            run_id=run_id,
            feature_name=feat.name,
            feature_version=feat.version,
            version_id=feat.version_id,
            transform_hash=feat.transform.transform_hash,
            source_id=feat.source.source_id,
            source_version=feat.source.source_version,
            source_schema_hash=feat.source.schema_hash,
            parquet_path=str(pq_path),
            sha256_checksum=compute_file_sha256(pq_path),
            row_count=df.height,
            entity_count=int(df["entity_id"].n_unique()) if df.height > 0 else 0,
            min_event_time=ensure_utc(min_ev) if isinstance(min_ev, datetime) else None,
            max_event_time=ensure_utc(max_ev) if isinstance(max_ev, datetime) else None,
            min_availability_time=ensure_utc(min_av) if isinstance(min_av, datetime) else None,
            max_availability_time=ensure_utc(max_av) if isinstance(max_av, datetime) else None,
            materialized_at=mat_ts,
            is_backfill=is_backfill,
            backfill_window_start=window_start,
            backfill_window_end=window_end,
        )

    def load_materialized(self, feature_def: FeatureDefinition) -> pl.DataFrame:
        """Load and deduplicate all materialized partitions for a feature version, verifying integrity."""
        fdir = self._feature_dir(feature_def.name, feature_def.version)
        if not fdir.exists():
            raise FileNotFoundError(
                f"No offline materialization found for '{feature_def.name}@{feature_def.version}'"
            )
        parquet_files = sorted(fdir.glob("*.parquet"))
        if not parquet_files:
            raise FileNotFoundError(
                f"No Parquet partitions found in {fdir}"
            )

        frames: list[pl.DataFrame] = []
        for pq in parquet_files:
            manifest_file = pq.with_suffix(".manifest.json")
            if manifest_file.exists():
                manifest = MaterializationManifest.model_validate_json(
                    manifest_file.read_text(encoding="utf-8")
                )
                actual_sha = compute_file_sha256(pq)
                if actual_sha != manifest.sha256_checksum:
                    raise VersionMismatchError(
                        f"Parquet checksum mismatch for {pq.name}",
                        details={"expected": manifest.sha256_checksum, "actual": actual_sha},
                    )
            df = pl.read_parquet(pq)
            if df.height > 0:
                stored_vids = set(df["version_id"].unique().to_list())
                if stored_vids != {feature_def.version_id}:
                    raise VersionMismatchError(
                        f"Materialized data version_id mismatch for '{feature_def.name}@{feature_def.version}'",
                        details={
                            "expected_version_id": feature_def.version_id,
                            "stored_version_ids": sorted(stored_vids),
                        },
                    )
            frames.append(df)

        combined = pl.concat(frames, how="vertical")
        # Sort by materialized_at ascending so keep="last" preserves latest materialization per bi-temporal key
        combined = (
            combined.sort(["entity_id", "event_time", "availability_time", "materialized_at"])
            .unique(
                subset=["entity_id", "event_time", "availability_time", "version_id"],
                keep="last",
                maintain_order=True,
            )
            .sort(["entity_id", "event_time", "availability_time"])
        )
        return combined

    def point_in_time_join(
        self,
        spine_df: pl.DataFrame,
        feature_refs: list[FeatureRef | str],
        *,
        max_allowed_as_of: datetime | None = None,
        enforce_ttl: bool = False,
        include_metadata: bool = True,
    ) -> tuple[pl.DataFrame, dict[str, Any]]:
        """Execute strict point-in-time as-of joins with causal leakage barriers.

        For each (entity_id, as_of_time) row in spine_df and each requested feature:
        - Only feature rows with availability_time <= as_of_time AND event_time <= as_of_time are eligible.
        - Future records (event_time > as_of_time) and late-arriving records not yet available
          (availability_time > as_of_time) are strictly excluded.
        - Among eligible rows, selects max (event_time, availability_time, materialized_at).
        - Never forward-fills from future rows.
        """
        if "entity_id" not in spine_df.columns or "as_of_time" not in spine_df.columns:
            raise TimestampContractError(
                "Spine dataframe must contain 'entity_id' and 'as_of_time' columns",
                details={"columns": spine_df.columns},
            )
        as_of_dtype = spine_df.schema["as_of_time"]
        if not isinstance(as_of_dtype, pl.Datetime) or as_of_dtype.time_zone is None:
            raise TimestampContractError(
                "Spine 'as_of_time' must be timezone-aware UTC Datetime",
                details={"dtype": str(as_of_dtype)},
            )
        if spine_df["as_of_time"].null_count() > 0:
            raise TimestampContractError("Spine 'as_of_time' contains null timestamps")

        norm_spine = spine_df.with_columns(
            pl.col("as_of_time").dt.convert_time_zone("UTC").dt.cast_time_unit("us")
        )
        if max_allowed_as_of is not None:
            cutoff = ensure_utc(max_allowed_as_of, "max_allowed_as_of")
            future_queries = norm_spine.filter(pl.col("as_of_time") > cutoff)
            if future_queries.height > 0:
                raise LeakageViolationError(
                    "Spine contains future as_of_time queries beyond max_allowed_as_of",
                    details={
                        "future_query_count": future_queries.height,
                        "max_allowed_as_of": cutoff.isoformat(),
                    },
                )

        spine_rows = norm_spine.to_dicts()
        result_df = norm_spine.clone()
        audit: dict[str, Any] = {
            "spine_rows": norm_spine.height,
            "leakage_violations": 0,
            "features": {},
        }

        for ref in feature_refs:
            feat = self.registry.resolve(ref)
            mat_df = self.load_materialized(feat)

            # Group candidate rows by entity_id for deterministic point-in-time matching
            by_entity: dict[str, list[dict[str, Any]]] = {}
            for row in mat_df.to_dicts():
                by_entity.setdefault(str(row["entity_id"]), []).append(row)

            matched_vals: list[Any] = []
            matched_ev: list[datetime | None] = []
            matched_av: list[datetime | None] = []
            matched_vids: list[str | None] = []
            matched_ages: list[float | None] = []
            matched_stale: list[bool] = []

            matched_count = 0
            excluded_future = 0
            excluded_late = 0

            for srow in spine_rows:
                ent = str(srow["entity_id"])
                as_of = ensure_utc(srow["as_of_time"], "as_of_time")
                candidates = by_entity.get(ent, [])

                best: dict[str, Any] | None = None
                best_key: tuple[datetime, datetime, datetime] | None = None
                for cand in candidates:
                    c_ev = ensure_utc(cand["event_time"])
                    c_av = ensure_utc(cand["availability_time"])
                    if c_ev > as_of:
                        excluded_future += 1
                        continue
                    if c_av > as_of:
                        excluded_late += 1
                        continue
                    c_mat = ensure_utc(cand["materialized_at"])
                    cand_key = (c_ev, c_av, c_mat)
                    if best_key is None or cand_key > best_key:
                        best_key = cand_key
                        best = cand

                if best is None:
                    val: Any = None
                    if feat.null_policy == NullPolicy.REJECT:
                        raise NullPolicyViolationError(
                            f"Missing point-in-time value for '{feat.name}' under REJECT null policy",
                            details={"entity_id": ent, "as_of_time": as_of.isoformat()},
                        )
                    if feat.null_policy == NullPolicy.DEFAULT_VALUE:
                        val = feat.default_value
                    matched_vals.append(val)
                    matched_ev.append(None)
                    matched_av.append(None)
                    matched_vids.append(None)
                    matched_ages.append(None)
                    matched_stale.append(True)
                    continue

                b_ev = ensure_utc(best["event_time"])
                b_av = ensure_utc(best["availability_time"])
                # Explicit post-match leakage barrier assertion
                if b_ev > as_of or b_av > as_of:
                    audit["leakage_violations"] += 1
                    raise LeakageViolationError(
                        "Post-join leakage barrier triggered",
                        details={
                            "entity_id": ent,
                            "as_of_time": as_of.isoformat(),
                            "matched_event_time": b_ev.isoformat(),
                            "matched_availability_time": b_av.isoformat(),
                        },
                    )

                age_sec = (as_of - b_ev).total_seconds()
                is_stale = age_sec > feat.ttl_seconds
                val = best["value"]
                if enforce_ttl and is_stale:
                    val = None

                matched_count += 1
                matched_vals.append(val)
                matched_ev.append(b_ev)
                matched_av.append(b_av)
                matched_vids.append(str(best["version_id"]))
                matched_ages.append(age_sec)
                matched_stale.append(is_stale)

            new_cols: list[pl.Series] = [pl.Series(feat.name, matched_vals)]
            if include_metadata:
                new_cols.extend(
                    [
                        pl.Series(f"{feat.name}__event_time", matched_ev, dtype=pl.Datetime("us", "UTC")),
                        pl.Series(
                            f"{feat.name}__availability_time", matched_av, dtype=pl.Datetime("us", "UTC")
                        ),
                        pl.Series(f"{feat.name}__version_id", matched_vids, dtype=pl.String()),
                        pl.Series(f"{feat.name}__age_seconds", matched_ages, dtype=pl.Float64()),
                        pl.Series(f"{feat.name}__is_stale", matched_stale, dtype=pl.Boolean()),
                    ]
                )
            result_df = result_df.with_columns(new_cols)
            audit["features"][feat.name] = {
                "version": feat.version,
                "version_id": feat.version_id,
                "matched_rows": matched_count,
                "null_rows": int(result_df[feat.name].null_count()),
                "excluded_future_candidates": excluded_future,
                "excluded_late_candidates": excluded_late,
            }

        return result_df, audit

    def backfill(
        self,
        feature_ref: FeatureRef | FeatureDefinition | str,
        raw_df: pl.DataFrame,
        *,
        window_start: datetime,
        window_end: datetime,
        run_id: str | None = None,
        materialized_at: datetime | None = None,
    ) -> BackfillReport:
        """Idempotently backfill a historical window while preserving causal warmup & availability times."""
        w_start = ensure_utc(window_start, "window_start")
        w_end = ensure_utc(window_end, "window_end")
        if w_end <= w_start:
            raise BackfillError(
                "Backfill window_end must be strictly after window_start",
                details={"window_start": w_start.isoformat(), "window_end": w_end.isoformat()},
            )

        feat = (
            feature_ref
            if isinstance(feature_ref, FeatureDefinition)
            else self.registry.resolve(feature_ref)
        )
        effective_run_id = run_id or f"backfill-{uuid.uuid4().hex[:10]}"
        mat_ts = ensure_utc(materialized_at or datetime.now(UTC), "materialized_at")

        # Include causal warmup buffer prior to window_start so rolling windows are exact at window_start
        warmup_margin = max(feat.transform.lookback_seconds, 3600.0)
        warmup_start = w_start - timedelta(seconds=warmup_margin)
        window_raw = raw_df.filter(
            (pl.col("event_time") >= warmup_start) & (pl.col("event_time") <= w_end)
        )

        computed = compute_causal_feature_frame(
            window_raw,
            feat,
            run_id=effective_run_id,
            materialized_at=mat_ts,
        )
        # Slice back to requested backfill window
        sliced = computed.filter(
            (pl.col("event_time") >= w_start) & (pl.col("event_time") <= w_end)
        )

        before_dedup = sliced.height
        existing_count = 0
        try:
            existing_df = self.load_materialized(feat)
            existing_count = existing_df.height
            # Count how many incoming keys already exist verbatim
            incoming_keys = sliced.select(["entity_id", "event_time", "availability_time", "version_id"])
            overlap = incoming_keys.join(
                existing_df.select(["entity_id", "event_time", "availability_time", "version_id"]),
                on=["entity_id", "event_time", "availability_time", "version_id"],
                how="inner",
            ).height
        except FileNotFoundError:
            overlap = 0

        deduped = sliced.unique(
            subset=["entity_id", "event_time", "availability_time", "version_id"],
            keep="last",
            maintain_order=True,
        ).sort(["entity_id", "event_time", "availability_time"])

        out_dir = self._feature_dir(feat.name, feat.version)
        out_dir.mkdir(parents=True, exist_ok=True)
        pq_path = out_dir / f"{effective_run_id}.parquet"
        deduped.write_parquet(pq_path)

        manifest = self._build_manifest(
            feat=feat,
            df=deduped,
            run_id=effective_run_id,
            pq_path=pq_path,
            mat_ts=mat_ts,
            is_backfill=True,
            window_start=w_start,
            window_end=w_end,
        )
        manifest_path = out_dir / f"{effective_run_id}.manifest.json"
        manifest_path.write_text(manifest.model_dump_json(indent=2), encoding="utf-8")

        self.lineage.record_feature_definition(feat)
        bf_node = f"backfill:{feat.name}@{feat.version}:{effective_run_id}"
        self.lineage.record_node(
            bf_node,
            "backfill_run",
            {
                "run_id": effective_run_id,
                "window_start": w_start.isoformat(),
                "window_end": w_end.isoformat(),
                "rows_written": deduped.height,
                "existing_rows_before": existing_count,
            },
        )
        self.lineage.record_edge(f"feature:{feat.version_id}", bf_node, "backfilled_to")

        return BackfillReport(
            run_id=effective_run_id,
            feature_name=feat.name,
            feature_version=feat.version,
            version_id=feat.version_id,
            window_start=w_start,
            window_end=w_end,
            rows_written=deduped.height,
            rows_deduplicated=(before_dedup - deduped.height) + overlap,
            manifest=manifest,
        )

    def export_training_dataset(
        self,
        spine_df: pl.DataFrame,
        feature_refs: list[FeatureRef | str],
        *,
        export_dir: str | Path,
        export_name: str,
        train_ratio: float = 0.7,
        val_ratio: float = 0.15,
        seed: int | None = None,
    ) -> tuple[pl.DataFrame, TrainingDatasetManifest]:
        """Export a point-in-time reproducible training dataset with chronological splits & checksums."""
        if not (0.0 < train_ratio < 1.0) or not (0.0 <= val_ratio < 1.0) or (train_ratio + val_ratio >= 1.0):
            raise ExportError(
                "Invalid chronological split ratios; require train_ratio > 0, val_ratio >= 0, train + val < 1",
                details={"train_ratio": train_ratio, "val_ratio": val_ratio},
            )

        joined, audit = self.point_in_time_join(spine_df, feature_refs, include_metadata=True)
        if audit["leakage_violations"] != 0:
            raise LeakageViolationError("Refusing to export training dataset with leakage violations")

        # Sort chronologically by as_of_time (then entity_id) for deterministic chronological splits
        ordered = joined.sort(["as_of_time", "entity_id"])
        unique_times = sorted(ordered["as_of_time"].unique().to_list())
        if len(unique_times) < 3:
            raise ExportError(
                "At least 3 distinct as_of_time timestamps are required for chronological train/val/test splits",
                details={"distinct_timestamps": len(unique_times)},
            )

        train_cut_idx = max(1, int(len(unique_times) * train_ratio))
        val_cut_idx = max(train_cut_idx + 1, int(len(unique_times) * (train_ratio + val_ratio)))
        if val_cut_idx >= len(unique_times):
            val_cut_idx = len(unique_times) - 1

        train_end_ts = ensure_utc(unique_times[train_cut_idx - 1])
        val_end_ts = ensure_utc(unique_times[val_cut_idx - 1])

        ordered = ordered.with_columns(
            pl.when(pl.col("as_of_time") <= train_end_ts)
            .then(pl.lit("train"))
            .when(pl.col("as_of_time") <= val_end_ts)
            .then(pl.lit("val"))
            .otherwise(pl.lit("test"))
            .alias("split")
        )

        # Verify zero temporal overlap between splits
        split_bounds: dict[str, dict[str, Any]] = {}
        for split_name in ("train", "val", "test"):
            sub = ordered.filter(pl.col("split") == split_name)
            split_bounds[split_name] = {
                "row_count": sub.height,
                "min_as_of_time": ensure_utc(sub["as_of_time"].min()).isoformat() if sub.height > 0 else None,
                "max_as_of_time": ensure_utc(sub["as_of_time"].max()).isoformat() if sub.height > 0 else None,
            }

        out_dir = Path(export_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        pq_path = out_dir / f"{export_name}.parquet"
        ordered.write_parquet(pq_path)

        resolved_feats = [self.registry.resolve(r) for r in feature_refs]
        feature_versions = {f.name: f.version_id for f in resolved_feats}
        feature_transform_hashes = {f.name: f.transform.transform_hash for f in resolved_feats}
        source_provenance = {
            f.name: {
                "source_id": f.source.source_id,
                "source_version": f.source.source_version,
                "schema_hash": f.source.schema_hash,
                "entitlement": f.source.entitlement,
                "license": f.source.license,
            }
            for f in resolved_feats
        }
        null_counts = {f.name: int(ordered[f.name].null_count()) for f in resolved_feats}

        manifest = TrainingDatasetManifest(
            export_name=export_name,
            exported_at=datetime.now(UTC),
            parquet_path=str(pq_path),
            sha256_checksum=compute_file_sha256(pq_path),
            row_count=ordered.height,
            entity_count=int(ordered["entity_id"].n_unique()) if ordered.height > 0 else 0,
            feature_versions=feature_versions,
            feature_transform_hashes=feature_transform_hashes,
            source_provenance=source_provenance,
            min_as_of_time=ensure_utc(ordered["as_of_time"].min()) if ordered.height > 0 else None,
            max_as_of_time=ensure_utc(ordered["as_of_time"].max()) if ordered.height > 0 else None,
            split_boundaries=split_bounds,
            null_counts=null_counts,
            zero_leakage_verified=True,
            seed=seed,
        )

        manifest_path = out_dir / f"{export_name}.manifest.json"
        manifest_path.write_text(
            json.dumps(manifest.model_dump(mode="json"), indent=2, sort_keys=True),
            encoding="utf-8",
        )

        exp_node = f"training_export:{export_name}"
        self.lineage.record_node(
            exp_node,
            "training_dataset_export",
            {
                "parquet_path": str(pq_path),
                "sha256_checksum": manifest.sha256_checksum,
                "row_count": manifest.row_count,
            },
        )
        for f in resolved_feats:
            self.lineage.record_edge(f"feature:{f.version_id}", exp_node, "exported_to_training")

        return ordered, manifest
