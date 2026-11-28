#!/usr/bin/env python3
"""Reproducible end-to-end demonstration of the Security Master Platform.

Executes all core scenarios (ticker reuse, dual listing, late reference correction,
conflicting IDs quarantine, reviewer correction workflow, corporate-action lineage,
pre-observation boundary lookup, and durable SQLite replay) and writes machine-readable
baseline outputs to ``reports/demo_baseline.json``.
"""

from __future__ import annotations

import json
import platform
import sys
from pathlib import Path
from typing import Any, Dict

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from sec_master.config import PlatformConfig, StorageConfig, load_config
from sec_master.errors import (
    AmbiguousMappingError,
    ConflictBlockedError,
    PreObservationLookupError,
)
from sec_master.models import ConflictStatus, IdentifierNamespace, ReviewAction, parse_ts
from sec_master.service import SecurityMasterService


def run_demo(db_path: Path, report_path: Path) -> Dict[str, Any]:
    if db_path.exists():
        db_path.unlink()

    base_cfg = load_config(PROJECT_ROOT / "config/default.json")
    cfg = PlatformConfig(
        config_version=base_cfg.config_version,
        schema_version=base_cfg.schema_version,
        environment=base_cfg.environment,
        storage=StorageConfig(backend="sqlite", sqlite_path=str(db_path)),
        licensing=base_cfg.licensing,
        conflict_resolution=base_cfg.conflict_resolution,
        quality=base_cfg.quality,
        reproducibility=base_cfg.reproducibility,
        http=base_cfg.http,
    )

    service = SecurityMasterService(cfg)
    outcomes: Dict[str, Any] = {}

    for fixture_name in (
        "ticker_reuse_dual_listing.json",
        "late_correction_conflicts.json",
        "corporate_actions.json",
    ):
        raw = json.loads((PROJECT_ROOT / "data/fixtures" / fixture_name).read_text(encoding="utf-8"))
        res = service.ingest_raw_records(raw["events"])
        outcomes[fixture_name] = [o.to_dict() for o in res]

    # 1. Ticker reuse: XNYS:ACME in 2020 vs 2023
    acme_2020 = service.lookup_as_of(
        IdentifierNamespace.MIC_TICKER,
        "XNYS:ACME",
        parse_ts("2020-06-01T00:00:00Z"),  # type: ignore[arg-type]
        parse_ts("2024-01-01T00:00:00Z"),  # type: ignore[arg-type]
    )
    acme_2023 = service.lookup_as_of(
        IdentifierNamespace.MIC_TICKER,
        "XNYS:ACME",
        parse_ts("2023-06-01T00:00:00Z"),  # type: ignore[arg-type]
        parse_ts("2024-01-01T00:00:00Z"),  # type: ignore[arg-type]
    )

    # 2. Dual listing: bare ORION must fail as AMBIGUOUS_MAPPING; XNYS/XLON resolve uniquely
    dual_ambiguity_error = None
    try:
        service.lookup_as_of(
            IdentifierNamespace.TICKER,
            "ORION",
            parse_ts("2023-06-01T00:00:00Z"),  # type: ignore[arg-type]
            parse_ts("2024-01-01T00:00:00Z"),  # type: ignore[arg-type]
        )
    except AmbiguousMappingError as exc:
        dual_ambiguity_error = exc.to_dict()

    orion_xnys = service.lookup_as_of(
        IdentifierNamespace.TICKER,
        "ORION",
        parse_ts("2023-06-01T00:00:00Z"),  # type: ignore[arg-type]
        parse_ts("2024-01-01T00:00:00Z"),  # type: ignore[arg-type]
        mic="XNYS",
    )
    orion_xlon = service.lookup_as_of(
        IdentifierNamespace.TICKER,
        "ORION",
        parse_ts("2023-06-01T00:00:00Z"),  # type: ignore[arg-type]
        parse_ts("2024-01-01T00:00:00Z"),  # type: ignore[arg-type]
        mic="XLON",
    )

    # 3. Conflicting ISIN blocks lookup BEFORE reviewer resolution
    open_conflicts = service.list_conflicts(ConflictStatus.OPEN)
    assert len(open_conflicts) == 1
    conflict_ticket = open_conflicts[0]

    blocked_error = None
    try:
        service.lookup_as_of(
            IdentifierNamespace.MIC_TICKER,
            "XNAS:NOVA",
            parse_ts("2024-01-05T00:00:00Z"),  # type: ignore[arg-type]
            parse_ts("2024-01-15T00:00:00Z"),  # type: ignore[arg-type]
        )
    except ConflictBlockedError as exc:
        blocked_error = exc.to_dict()

    # 4. Reviewer resolves the conflict (KEEP_EXISTING after verifying primary exchange filing)
    review_outcome = service.resolve_conflict(
        conflict_id=conflict_ticket.conflict_id,
        reviewer_id="analyst_data_ops",
        action=ReviewAction.KEEP_EXISTING,
        rationale="Verified US0000004046 against primary exchange prospectus; VENDOR_BETA feed stale.",
        observed_at=parse_ts("2024-01-13T09:00:00Z"),
    )

    # 5. Late reference correction bitemporal check (pre-correction vs post-correction observation time)
    nova_before_corr = service.lookup_as_of(
        IdentifierNamespace.MIC_TICKER,
        "XNAS:NOVA",
        parse_ts("2024-01-05T00:00:00Z"),  # type: ignore[arg-type]
        parse_ts("2024-01-05T00:00:00Z"),  # type: ignore[arg-type]
    )
    nova_after_corr = service.lookup_as_of(
        IdentifierNamespace.MIC_TICKER,
        "XNAS:NOVA",
        parse_ts("2024-01-05T00:00:00Z"),  # type: ignore[arg-type]
        parse_ts("2024-01-15T00:00:00Z"),  # type: ignore[arg-type]
    )

    # 6. Pre-observation boundary check (historical lookup before observation time)
    pre_obs_error = None
    try:
        service.lookup_as_of(
            IdentifierNamespace.MIC_TICKER,
            "XNAS:NOVA",
            parse_ts("2024-01-01T12:00:00Z"),  # type: ignore[arg-type]
            parse_ts("2024-01-01T12:00:00Z"),  # type: ignore[arg-type]
        )
    except PreObservationLookupError as exc:
        pre_obs_error = exc.to_dict()

    # 7. Corporate action lineage trace from NOVA -> NVAX -> MERGER(VRTX) -> SPLIT + SPINOFF(SPCO)
    lineage_trace = service.trace_corporate_actions(
        "SEC-4524211962DF",
        parse_ts("2024-12-01T00:00:00Z"),  # type: ignore[arg-type]
        parse_ts("2024-12-01T00:00:00Z"),  # type: ignore[arg-type]
    )

    # 8. Quality metrics & deterministic journal replay verification
    quality = service.quality_metrics(parse_ts("2024-11-01T12:00:00Z"))  # type: ignore[arg-type]
    pre_replay_hash = service.diagnostics()["snapshot_hash"]
    replay_result = service.replay_from_journal()
    post_replay_hash = replay_result["snapshot_hash"]
    assert pre_replay_hash == post_replay_hash

    service.close()

    # Restart verification from durable SQLite file
    restarted = SecurityMasterService(cfg)
    restarted_hash = restarted.diagnostics()["snapshot_hash"]
    assert restarted_hash == pre_replay_hash
    restarted.close()

    report = {
        "report_type": "demo_baseline",
        "data_provenance": "synthetic_deterministic_fixtures",
        "runtime": {
            "python_version": platform.python_version(),
            "platform": platform.platform(),
            "config_version": cfg.config_version,
            "schema_version": cfg.schema_version,
            "deterministic_seed": cfg.reproducibility.deterministic_seed,
        },
        "ingest_outcomes": outcomes,
        "scenarios": {
            "ticker_reuse": {
                "acme_2020_instrument_id": acme_2020.instrument.instrument_id,
                "acme_2020_name": acme_2020.instrument.name,
                "acme_2023_instrument_id": acme_2023.instrument.instrument_id,
                "acme_2023_name": acme_2023.instrument.name,
            },
            "dual_listing": {
                "bare_ticker_error": dual_ambiguity_error,
                "xnys_listing_id": orion_xnys.listing.listing_id if orion_xnys.listing else None,
                "xnys_currency": orion_xnys.listing.currency if orion_xnys.listing else None,
                "xlon_listing_id": orion_xlon.listing.listing_id if orion_xlon.listing else None,
                "xlon_currency": orion_xlon.listing.currency if orion_xlon.listing else None,
                "shared_instrument_id": orion_xnys.instrument.instrument_id,
            },
            "conflict_quarantine_and_review": {
                "conflict_id": conflict_ticket.conflict_id,
                "conflict_type": conflict_ticket.conflict_type,
                "blocked_lookup_error": blocked_error,
                "review_outcome": review_outcome.to_dict(),
            },
            "late_reference_correction": {
                "before_correction_name": nova_before_corr.instrument.name,
                "after_correction_name": nova_after_corr.instrument.name,
                "after_correction_lineage": nova_after_corr.instrument.lineage.to_dict(),
            },
            "pre_observation_boundary": pre_obs_error,
            "corporate_action_lineage": lineage_trace,
        },
        "quality_metrics": quality.to_dict(),
        "reproducibility": {
            "pre_replay_snapshot_hash": pre_replay_hash,
            "post_replay_snapshot_hash": post_replay_hash,
            "restarted_snapshot_hash": restarted_hash,
            "exact_match": (pre_replay_hash == post_replay_hash == restarted_hash),
        },
    }

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return report


def main() -> int:
    report = run_demo(
        db_path=PROJECT_ROOT / ".runtime/demo_security_master.db",
        report_path=PROJECT_ROOT / "reports/demo_baseline.json",
    )
    print(
        json.dumps(
            {
                "status": "DEMO_PASSED",
                "snapshot_hash": report["reproducibility"]["pre_replay_snapshot_hash"],
                "active_instruments": report["quality_metrics"]["active_instruments"],
                "active_listings": report["quality_metrics"]["active_listings"],
                "open_conflicts": report["quality_metrics"]["open_conflicts"],
                "resolved_conflicts": report["quality_metrics"]["resolved_conflicts"],
                "report_path": "reports/demo_baseline.json",
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
