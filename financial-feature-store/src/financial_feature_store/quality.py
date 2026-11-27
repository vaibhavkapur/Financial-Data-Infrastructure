"""Data quality evaluation (promotion gates) and feature lineage tracking."""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

import polars as pl

from financial_feature_store.contracts import (
    FeatureDefinition,
    LineageEdge,
    LineageGraph,
    QualityCheckResult,
    QualityReport,
    ValueType,
)


class QualityEvaluator:
    """Evaluates materialized feature frames against registered quality rules."""

    def evaluate(
        self,
        feature_df: pl.DataFrame,
        feature_def: FeatureDefinition,
        *,
        run_id: str,
    ) -> QualityReport:
        rules = feature_def.quality_rules
        row_count = feature_df.height
        null_count = int(feature_df["value"].null_count()) if row_count > 0 else 0
        null_ratio = (null_count / row_count) if row_count > 0 else 1.0

        late_count = int(feature_df.filter(pl.col("is_late_arrival")).height) if row_count > 0 else 0
        max_late_sec = (
            float(feature_df["late_arrival_seconds"].max())  # type: ignore[arg-type]
            if row_count > 0
            else 0.0
        )

        checks: list[QualityCheckResult] = []

        # 1. Minimum row count
        checks.append(
            QualityCheckResult(
                check_name="min_row_count",
                passed=row_count >= rules.min_row_count,
                observed=row_count,
                threshold=rules.min_row_count,
                message=f"Row count {row_count} >= {rules.min_row_count}",
            )
        )

        # 2. Version integrity
        distinct_vids = feature_df["version_id"].unique().to_list() if row_count > 0 else []
        version_ok = row_count > 0 and distinct_vids == [feature_def.version_id]
        checks.append(
            QualityCheckResult(
                check_name="version_id_integrity",
                passed=version_ok,
                observed=distinct_vids,
                threshold=[feature_def.version_id],
                message="All rows match feature definition version_id",
            )
        )

        # 3. Causality invariant: availability_time >= event_time
        causality_violations = (
            int(feature_df.filter(pl.col("availability_time") < pl.col("event_time")).height)
            if row_count > 0
            else 0
        )
        checks.append(
            QualityCheckResult(
                check_name="bi_temporal_causality",
                passed=causality_violations == 0,
                observed=causality_violations,
                threshold=0,
                message="availability_time >= event_time for all rows",
            )
        )

        # 4. Null ratio threshold
        checks.append(
            QualityCheckResult(
                check_name="max_null_ratio",
                passed=null_ratio <= rules.max_null_ratio,
                observed=round(null_ratio, 6),
                threshold=rules.max_null_ratio,
                message=f"Null ratio {null_ratio:.4f} <= {rules.max_null_ratio:.4f}",
            )
        )

        # 5. Late arrival SLA
        checks.append(
            QualityCheckResult(
                check_name="max_late_arrival_seconds",
                passed=max_late_sec <= rules.max_late_arrival_seconds,
                observed=round(max_late_sec, 6),
                threshold=rules.max_late_arrival_seconds,
                message=f"Max late arrival {max_late_sec:.3f}s <= {rules.max_late_arrival_seconds:.3f}s",
            )
        )

        # 6. Numeric NaN/Inf and bounds checks
        if feature_def.value_type in (ValueType.FLOAT64, ValueType.INT64) and row_count > 0:
            non_null_vals = [v for v in feature_df["value"].to_list() if v is not None]
            if feature_def.value_type == ValueType.FLOAT64 and not rules.allow_nan_or_inf:
                bad_float = sum(1 for v in non_null_vals if math.isnan(float(v)) or math.isinf(float(v)))
                checks.append(
                    QualityCheckResult(
                        check_name="no_nan_or_inf",
                        passed=bad_float == 0,
                        observed=bad_float,
                        threshold=0,
                        message="No NaN or Inf values present",
                    )
                )
            finite_vals = [
                float(v)
                for v in non_null_vals
                if not (math.isnan(float(v)) or math.isinf(float(v)))
            ]
            if rules.min_value is not None and finite_vals:
                obs_min = min(finite_vals)
                checks.append(
                    QualityCheckResult(
                        check_name="min_value_bound",
                        passed=obs_min >= rules.min_value,
                        observed=obs_min,
                        threshold=rules.min_value,
                        message=f"Min value {obs_min} >= {rules.min_value}",
                    )
                )
            if rules.max_value is not None and finite_vals:
                obs_max = max(finite_vals)
                checks.append(
                    QualityCheckResult(
                        check_name="max_value_bound",
                        passed=obs_max <= rules.max_value,
                        observed=obs_max,
                        threshold=rules.max_value,
                        message=f"Max value {obs_max} <= {rules.max_value}",
                    )
                )

        passed = all(c.passed for c in checks)
        return QualityReport(
            feature_name=feature_def.name,
            feature_version=feature_def.version,
            version_id=feature_def.version_id,
            run_id=run_id,
            passed=passed,
            row_count=row_count,
            null_count=null_count,
            null_ratio=null_ratio,
            late_arrival_count=late_count,
            max_late_arrival_seconds=max_late_sec,
            checks=tuple(checks),
        )


class LineageTracker:
    """Directed acyclic provenance graph across sources, feature versions, runs, online sync, and exports."""

    def __init__(self) -> None:
        self._nodes: dict[str, dict[str, Any]] = {}
        self._edges: list[LineageEdge] = []

    def record_node(self, node_id: str, node_type: str, metadata: dict[str, Any] | None = None) -> None:
        self._nodes[node_id] = {
            "node_type": node_type,
            **(metadata or {}),
        }

    def record_edge(
        self,
        source_node: str,
        target_node: str,
        relation: str,
        metadata: dict[str, Any] | None = None,
    ) -> LineageEdge:
        edge = LineageEdge(
            source_node=source_node,
            target_node=target_node,
            relation=relation,
            metadata=metadata or {},
        )
        self._edges.append(edge)
        return edge

    def record_feature_definition(self, feature_def: FeatureDefinition) -> None:
        src_node = f"source:{feature_def.source.source_id}@{feature_def.source.source_version}"
        feat_node = f"feature:{feature_def.version_id}"
        self.record_node(
            src_node,
            "source_dataset",
            {
                "source_id": feature_def.source.source_id,
                "dataset_name": feature_def.source.dataset_name,
                "source_version": feature_def.source.source_version,
                "schema_hash": feature_def.source.schema_hash,
                "entitlement": feature_def.source.entitlement,
                "license": feature_def.source.license,
            },
        )
        self.record_node(
            feat_node,
            "feature_definition",
            {
                "feature_name": feature_def.name,
                "feature_version": feature_def.version,
                "version_id": feature_def.version_id,
                "transform_name": feature_def.transform.transform_name,
                "transform_hash": feature_def.transform.transform_hash,
                "owner": feature_def.owner,
            },
        )
        self.record_edge(src_node, feat_node, "derives_feature")

    def upstream_of(self, node_id: str) -> list[str]:
        visited: set[str] = set()
        stack = [node_id]
        while stack:
            cur = stack.pop()
            for edge in self._edges:
                if edge.target_node == cur and edge.source_node not in visited:
                    visited.add(edge.source_node)
                    stack.append(edge.source_node)
        return sorted(visited)

    def downstream_of(self, node_id: str) -> list[str]:
        visited: set[str] = set()
        stack = [node_id]
        while stack:
            cur = stack.pop()
            for edge in self._edges:
                if edge.source_node == cur and edge.target_node not in visited:
                    visited.add(edge.target_node)
                    stack.append(edge.target_node)
        return sorted(visited)

    def snapshot(self) -> LineageGraph:
        return LineageGraph(nodes=dict(self._nodes), edges=tuple(self._edges))

    def save(self, path: str | Path) -> Path:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            json.dumps(self.snapshot().model_dump(mode="json"), indent=2, sort_keys=True),
            encoding="utf-8",
        )
        return target
