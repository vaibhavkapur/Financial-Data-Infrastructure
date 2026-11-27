"""Versioned Feature Definition Registry with schema evolution and proliferation controls."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from financial_feature_store.contracts import (
    FeatureDefinition,
    FeatureRef,
    GovernanceConfig,
    LifecycleStage,
    QualityReport,
    canonical_sha256,
    parse_semver,
)
from financial_feature_store.errors import (
    FeatureProliferationError,
    ImmutableVersionError,
    QualityGateError,
    RegistryError,
    SchemaCompatibilityError,
    VersionMismatchError,
)


class FeatureRegistry:
    """Canonical registry owning feature definitions, version contracts, and promotion gates."""

    def __init__(self, governance: GovernanceConfig | None = None) -> None:
        self.governance = governance or GovernanceConfig()
        # (name, version) -> FeatureDefinition
        self._definitions: dict[tuple[str, str], FeatureDefinition] = {}
        # name -> active semver string
        self._active_versions: dict[str, str] = {}
        # (name, version) -> last quality report used for promotion evidence
        self._promotion_evidence: dict[tuple[str, str], QualityReport] = {}

    def register(self, definition: FeatureDefinition) -> FeatureDefinition:
        """Register a feature version with immutability, compatibility, and proliferation checks."""
        if self.governance.require_owner and not definition.owner.strip():
            raise RegistryError("Feature owner is required by governance policy")
        if self.governance.require_source_provenance and not definition.source.provenance_uri.strip():
            raise RegistryError("Source provenance URI is required by governance policy")

        key = (definition.name, definition.version)
        if key in self._definitions:
            existing = self._definitions[key]
            if existing.version_id == definition.version_id:
                return existing
            raise ImmutableVersionError(
                f"Feature '{definition.name}' version '{definition.version}' is immutable and already registered with a different definition",
                details={
                    "feature_name": definition.name,
                    "version": definition.version,
                    "existing_version_id": existing.version_id,
                    "attempted_version_id": definition.version_id,
                },
            )

        # Check schema evolution compatibility against prior versions of the same feature
        prior_versions = self.list_versions(definition.name)
        if prior_versions:
            latest_prior = prior_versions[-1]
            self._validate_schema_evolution(latest_prior, definition)
        else:
            # Enforce anti-proliferation guard when introducing a brand new feature name
            self._check_proliferation_budget(definition)

        self._definitions[key] = definition
        if definition.stage == LifecycleStage.ACTIVE:
            raise QualityGateError(
                "Features cannot be registered directly as ACTIVE; register as DRAFT/STAGING and promote with a passing QualityReport",
                details={"feature_name": definition.name, "version": definition.version},
            )
        return definition

    def _check_proliferation_budget(self, definition: FeatureDefinition) -> None:
        """Prevent unbounded feature proliferation across views and the store."""
        distinct_features = {
            name
            for (name, _ver), feat in self._definitions.items()
            if feat.stage != LifecycleStage.RETIRED
        }
        if len(distinct_features) >= self.governance.max_total_active_features:
            raise FeatureProliferationError(
                "Total feature budget exceeded; retire unused features before adding new ones",
                details={
                    "current_count": len(distinct_features),
                    "max_total_active_features": self.governance.max_total_active_features,
                    "feature_name": definition.name,
                },
            )

        view_features = {
            name
            for (name, _ver), feat in self._definitions.items()
            if feat.feature_view == definition.feature_view and feat.stage != LifecycleStage.RETIRED
        }
        if len(view_features) >= self.governance.max_active_features_per_view:
            raise FeatureProliferationError(
                f"Feature view '{definition.feature_view}' exceeded per-view feature budget",
                details={
                    "feature_view": definition.feature_view,
                    "current_count": len(view_features),
                    "max_active_features_per_view": self.governance.max_active_features_per_view,
                    "feature_name": definition.name,
                },
            )

    def _validate_schema_evolution(
        self,
        previous: FeatureDefinition,
        candidate: FeatureDefinition,
    ) -> None:
        """Enforce SemVer discipline for breaking vs backward-compatible schema changes."""
        prev_sem = parse_semver(previous.version)
        cand_sem = parse_semver(candidate.version)
        if cand_sem <= prev_sem:
            raise SchemaCompatibilityError(
                f"New version '{candidate.version}' must be strictly greater than latest version '{previous.version}'",
                details={
                    "feature_name": candidate.name,
                    "previous_version": previous.version,
                    "candidate_version": candidate.version,
                },
            )

        breaking_reasons: list[str] = []
        if candidate.entity_type != previous.entity_type:
            breaking_reasons.append(f"entity_type changed ({previous.entity_type} -> {candidate.entity_type})")
        if candidate.value_type != previous.value_type:
            breaking_reasons.append(f"value_type changed ({previous.value_type} -> {candidate.value_type})")
        if candidate.unit != previous.unit:
            breaking_reasons.append(f"unit changed ({previous.unit} -> {candidate.unit})")
        if candidate.currency != previous.currency:
            breaking_reasons.append(f"currency changed ({previous.currency} -> {candidate.currency})")

        if breaking_reasons and cand_sem[0] <= prev_sem[0]:
            raise SchemaCompatibilityError(
                f"Breaking schema change for '{candidate.name}' requires a MAJOR version bump",
                details={
                    "feature_name": candidate.name,
                    "previous_version": previous.version,
                    "candidate_version": candidate.version,
                    "breaking_changes": breaking_reasons,
                },
            )

        # Transform or source logic change without breaking contract requires at least minor bump
        transform_changed = candidate.transform.transform_hash != previous.transform.transform_hash
        source_changed = (
            candidate.source.source_id != previous.source.source_id
            or candidate.source.source_version != previous.source.source_version
            or candidate.source.schema_hash != previous.source.schema_hash
        )
        if (transform_changed or source_changed) and cand_sem[0] == prev_sem[0] and cand_sem[1] == prev_sem[1]:
            raise SchemaCompatibilityError(
                f"Transform or source change for '{candidate.name}' requires at least a MINOR version bump",
                details={
                    "feature_name": candidate.name,
                    "previous_version": previous.version,
                    "candidate_version": candidate.version,
                    "transform_changed": transform_changed,
                    "source_changed": source_changed,
                },
            )

    def promote(
        self,
        name: str,
        version: str,
        target_stage: LifecycleStage,
        quality_report: QualityReport | None = None,
    ) -> FeatureDefinition:
        """Promote a registered feature version; ACTIVE promotion requires a passing QualityReport."""
        feat = self.get(name, version)
        if target_stage == LifecycleStage.ACTIVE:
            if quality_report is None:
                raise QualityGateError(
                    f"Cannot promote '{name}@{version}' to ACTIVE without a QualityReport",
                    details={"feature_name": name, "version": version},
                )
            if (
                quality_report.feature_name != feat.name
                or quality_report.feature_version != feat.version
                or quality_report.version_id != feat.version_id
            ):
                raise VersionMismatchError(
                    "QualityReport does not match the target feature definition version_id",
                    details={
                        "expected_version_id": feat.version_id,
                        "report_version_id": quality_report.version_id,
                    },
                )
            if not quality_report.passed:
                failed_checks = [c.check_name for c in quality_report.checks if not c.passed]
                raise QualityGateError(
                    f"Promotion blocked for '{name}@{version}': data quality checks failed",
                    details={
                        "feature_name": name,
                        "version": version,
                        "version_id": feat.version_id,
                        "failed_checks": failed_checks,
                    },
                )
            # Mark prior active version as deprecated (remains queryable by explicit version)
            prev_active = self._active_versions.get(name)
            if prev_active and prev_active != version:
                prev_def = self._definitions[(name, prev_active)]
                self._definitions[(name, prev_active)] = prev_def.with_stage(LifecycleStage.DEPRECATED)

            updated = feat.with_stage(LifecycleStage.ACTIVE)
            self._definitions[(name, version)] = updated
            self._active_versions[name] = version
            self._promotion_evidence[(name, version)] = quality_report
            return updated

        updated = feat.with_stage(target_stage)
        self._definitions[(name, version)] = updated
        if target_stage in (LifecycleStage.DEPRECATED, LifecycleStage.RETIRED):
            if self._active_versions.get(name) == version:
                del self._active_versions[name]
        return updated

    def get(self, name: str, version: str | None = None) -> FeatureDefinition:
        """Retrieve a specific feature version or the currently ACTIVE version."""
        if version is None:
            active_ver = self._active_versions.get(name)
            if active_ver is None:
                raise RegistryError(
                    f"No ACTIVE version registered for feature '{name}'",
                    details={"feature_name": name},
                )
            return self._definitions[(name, active_ver)]

        key = (name, version)
        if key not in self._definitions:
            raise VersionMismatchError(
                f"Feature '{name}' version '{version}' is not registered",
                details={
                    "feature_name": name,
                    "requested_version": version,
                    "available_versions": [v.version for v in self.list_versions(name)],
                },
            )
        return self._definitions[key]

    def resolve(self, ref: FeatureRef | str) -> FeatureDefinition:
        """Resolve a FeatureRef or feature name string and verify optional version_id pin."""
        if isinstance(ref, str):
            return self.get(ref)
        feat = self.get(ref.name, ref.version)
        if ref.expected_version_id is not None and feat.version_id != ref.expected_version_id:
            raise VersionMismatchError(
                f"Pinned version_id mismatch for '{ref.name}'",
                details={
                    "feature_name": ref.name,
                    "expected_version_id": ref.expected_version_id,
                    "actual_version_id": feat.version_id,
                },
            )
        return feat

    def list_versions(self, name: str) -> list[FeatureDefinition]:
        """Return all registered versions of a feature sorted by SemVer ascending."""
        matching = [feat for (fname, _ver), feat in self._definitions.items() if fname == name]
        return sorted(matching, key=lambda f: parse_semver(f.version))

    def list_all(self, *, stage: LifecycleStage | None = None) -> list[FeatureDefinition]:
        """List all registered feature definitions optionally filtered by stage."""
        items = list(self._definitions.values())
        if stage is not None:
            items = [f for f in items if f.stage == stage]
        return sorted(items, key=lambda f: (f.name, parse_semver(f.version)))

    def get_promotion_evidence(self, name: str, version: str) -> QualityReport | None:
        return self._promotion_evidence.get((name, version))

    def snapshot_hash(self) -> str:
        payload = [
            {"version_id": f.version_id, "stage": f.stage.value}
            for f in self.list_all()
        ]
        return canonical_sha256(payload)[:16]

    def save(self, path: str | Path) -> Path:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        payload: dict[str, Any] = {
            "snapshot_hash": self.snapshot_hash(),
            "governance": self.governance.model_dump(mode="json"),
            "active_versions": dict(sorted(self._active_versions.items())),
            "definitions": [f.model_dump(mode="json") for f in self.list_all()],
            "promotion_evidence": {
                f"{name}@{ver}": rep.model_dump(mode="json")
                for (name, ver), rep in sorted(self._promotion_evidence.items())
            },
        }
        target.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        return target

    @classmethod
    def load(cls, path: str | Path) -> FeatureRegistry:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        governance = GovernanceConfig.model_validate(raw["governance"])
        registry = cls(governance=governance)
        for item in raw["definitions"]:
            feat = FeatureDefinition.model_validate(item)
            registry._definitions[(feat.name, feat.version)] = feat
        registry._active_versions = dict(raw["active_versions"])
        for key_str, rep_data in raw.get("promotion_evidence", {}).items():
            name, ver = key_str.split("@", 1)
            registry._promotion_evidence[(name, ver)] = QualityReport.model_validate(rep_data)
        return registry
