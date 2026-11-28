#!/usr/bin/env python3
"""Reproducible end-to-end demo exercising M1-M4 and saving versioned outputs to reports/."""

from __future__ import annotations

import json
import platform
import sys
from datetime import date, datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from corp_actions.adapters.json_adapter import load_fixture_bundle
from corp_actions.adapters.polars_adapter import polars_available
from corp_actions.config import EngineConfig
from corp_actions.engine import CorporateActionsEngine


def _utc(iso: str) -> datetime:
    return datetime.fromisoformat(iso.replace("Z", "+00:00")).astimezone(timezone.utc)


def run_demo() -> dict:
    config = EngineConfig.from_file(PROJECT_ROOT / "config" / "engine_config.json")
    engine = CorporateActionsEngine(config=config)
    prices, positions, fx_rates, actions = load_fixture_bundle(PROJECT_ROOT / "data" / "fixtures")

    engine.ingest_prices(prices)
    engine.ingest_positions(positions)
    engine.ingest_fx_rates(fx_rates)
    engine.ingest_actions(actions)

    # 1. PIT query before any actions go ex (no hindsight check)
    acme_early = engine.query_as_of(
        "SEC-ACME",
        as_of_time=_utc("2025-01-05T12:00:00Z"),
        account_id="ACCT-SIM-001",
    )

    # 2. PIT query before late dividend correction arrives (v1 = $0.90)
    acme_before_corr = engine.query_as_of(
        "SEC-ACME",
        as_of_time=_utc("2025-01-13T22:00:00Z"),
        end_date=date(2025, 1, 13),
        account_id="ACCT-SIM-001",
    )

    # 3. PIT query after late dividend correction arrives (v2 = $1.80)
    acme_after_corr = engine.query_as_of(
        "SEC-ACME",
        as_of_time=_utc("2025-01-15T22:00:00Z"),
        end_date=date(2025, 1, 15),
        account_id="ACCT-SIM-001",
    )

    # 4. Replay diff across the late correction
    acme_replay = engine.replay_correction(
        "SEC-ACME",
        from_as_of=_utc("2025-01-13T22:00:00Z"),
        to_as_of=_utc("2025-01-15T22:00:00Z"),
        end_date=date(2025, 1, 13),
        account_id="ACCT-SIM-001",
    )

    # 5. Same-day split + FX dividend + unsupported merger quarantine
    euro_snap = engine.query_as_of(
        "SEC-EURO",
        as_of_time=_utc("2025-01-10T22:00:00Z"),
        end_date=date(2025, 1, 10),
        account_id="ACCT-SIM-001",
    )

    # 6. Reverse split + cancelled dividend
    rev_snap = engine.query_as_of(
        "SEC-REV",
        as_of_time=_utc("2025-01-08T22:00:00Z"),
        end_date=date(2025, 1, 8),
        account_id="ACCT-SIM-001",
    )

    # Verify reproducibility by re-running ACME snapshot in a fresh engine
    engine_verify = CorporateActionsEngine(config=config)
    engine_verify.ingest_prices(prices)
    engine_verify.ingest_positions(positions)
    engine_verify.ingest_fx_rates(fx_rates)
    engine_verify.ingest_actions(actions)
    acme_verify = engine_verify.query_as_of(
        "SEC-ACME",
        as_of_time=_utc("2025-01-15T22:00:00Z"),
        end_date=date(2025, 1, 15),
        account_id="ACCT-SIM-001",
    )
    reproducible = acme_after_corr.output_digest == acme_verify.output_digest
    engine_verify.close()

    runtime_metadata = {
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "polars_installed": polars_available(),
        "config": config.to_dict(),
        "reproducible_digest_match": reproducible,
    }

    demo_payload = {
        "report_type": "end_to_end_demo_output",
        "data_provenance": "synthetic_deterministic_fixtures",
        "runtime_metadata": runtime_metadata,
        "engine_counters": engine.counters.snapshot(),
        "scenarios": {
            "acme_pit_early_no_hindsight": acme_early.to_dict(),
            "acme_pit_before_correction": acme_before_corr.to_dict(),
            "acme_pit_after_correction": acme_after_corr.to_dict(),
            "acme_late_correction_replay": acme_replay.to_dict(),
            "euro_same_day_split_fx_div_and_merger": euro_snap.to_dict(),
            "rev_reverse_split_and_cancelled_div": rev_snap.to_dict(),
        },
    }

    reconciliation_payload = {
        "report_type": "reconciliation_and_data_quality_diagnostics",
        "data_provenance": "synthetic_deterministic_fixtures",
        "runtime_metadata": runtime_metadata,
        "reconciliations": {
            "SEC-ACME (post-correction)": acme_after_corr.reconciliation.to_dict() if acme_after_corr.reconciliation else None,
            "SEC-EURO (same-day split+FX div, merger quarantined)": euro_snap.reconciliation.to_dict() if euro_snap.reconciliation else None,
            "SEC-REV (reverse split, cancelled div)": rev_snap.reconciliation.to_dict() if rev_snap.reconciliation else None,
        },
    }

    reports_dir = PROJECT_ROOT / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "demo_output.json").write_text(
        json.dumps(demo_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    (reports_dir / "reconciliation_report.json").write_text(
        json.dumps(reconciliation_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )

    engine.close()

    print("=== Financial Corporate Actions Engine Demo ===")
    print(f"Python: {runtime_metadata['python_version']} | Platform: {runtime_metadata['platform']}")
    print(f"Reproducible digest match: {reproducible}")
    print(f"[SEC-ACME early PIT 2025-01-05] applied={len(acme_early.effective_actions)}, pending={acme_early.diagnostics['pending_future_ex_actions']}")
    print(f"[SEC-ACME post-correction 2025-01-15] digest={acme_after_corr.output_digest[:16]}, end_qty={acme_after_corr.position_ledger[-1].quantity}, end_cash={acme_after_corr.position_ledger[-1].settled_cash_balance}, reconciled={acme_after_corr.reconciliation.reconciled}")
    print(f"[SEC-ACME replay v1->v2] corrected={acme_replay.corrected_actions}, cash_delta={acme_replay.cash_delta_end}, changed_dates={len(acme_replay.changed_dates)}")
    print(f"[SEC-EURO same-day split+FX div] end_qty={euro_snap.position_ledger[-1].quantity}, end_cash={euro_snap.position_ledger[-1].settled_cash_balance}, quarantined={[q.action_id for q in euro_snap.quarantined_actions]}")
    print(f"[SEC-REV reverse split+cancel] end_qty={rev_snap.position_ledger[-1].quantity}, cancelled={rev_snap.diagnostics['cancelled_actions']}")
    print(f"Wrote reports/demo_output.json and reports/reconciliation_report.json")
    return demo_payload


if __name__ == "__main__":
    run_demo()
