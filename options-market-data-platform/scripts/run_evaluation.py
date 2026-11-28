#!/usr/bin/env python3
"""Reference baseline evaluation script comparing derived features against analytical oracle.

Writes raw machine-readable comparison evidence to reports/evaluation_report.json.
"""

from __future__ import annotations

import json
import platform as py_platform
import sys
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from omdp.adapters.json_adapter import load_fixture_bundle
from omdp.config import StorageConfig, load_config
from omdp.domain.contracts import IVStatus
from omdp.service.platform import OptionsMarketDataPlatform


def main() -> int:
    cfg = load_config(ROOT / "config" / "default_config.json")
    cfg = replace(cfg, storage=StorageConfig(backend="sqlite", sqlite_path=":memory:"))
    bundle = load_fixture_bundle(ROOT / "data" / "fixtures" / "reference_chain.json")

    platform = OptionsMarketDataPlatform(config=cfg)
    try:
        results = platform.ingest_bundle(bundle)
        res = results[0]
        oracle = bundle.expected_oracle
        iv_tol = float(oracle["iv_tolerance"])
        tte_tol = float(oracle["tte_tolerance"])

        comparisons = []
        max_iv_err = 0.0
        max_tte_err = 0.0
        max_residual = 0.0
        all_passed = True

        for feat in res.features:
            expected = oracle["contracts"][feat.option_id]
            iv_err = abs(feat.implied_vol - expected["implied_vol"]) if feat.implied_vol is not None else float("inf")
            tte_err = (
                abs(feat.time_to_expiry_years - expected["time_to_expiry_years"])
                if feat.time_to_expiry_years is not None
                else float("inf")
            )
            residual = feat.provenance.price_residual or 0.0
            passed = (
                feat.iv_status == IVStatus.COMPUTED
                and iv_err <= iv_tol
                and tte_err <= tte_tol
                and feat.provenance.option_quote_hash is not None
                and feat.provenance.underlying_quote_hash is not None
                and feat.provenance.contract_hash is not None
            )
            all_passed = all_passed and passed
            max_iv_err = max(max_iv_err, iv_err)
            max_tte_err = max(max_tte_err, tte_err)
            max_residual = max(max_residual, residual)

            comparisons.append(
                {
                    "option_id": feat.option_id,
                    "feature_id": feat.feature_id,
                    "iv_status": feat.iv_status.value,
                    "expected_iv": expected["implied_vol"],
                    "computed_iv": feat.implied_vol,
                    "abs_iv_error": iv_err,
                    "expected_tte_years": expected["time_to_expiry_years"],
                    "computed_tte_years": feat.time_to_expiry_years,
                    "abs_tte_error": tte_err,
                    "price_residual": residual,
                    "solver_iterations": feat.provenance.solver_iterations,
                    "passed": passed,
                }
            )

        replay = platform.replay_snapshot(res.snapshot.snapshot_id)
        all_passed = all_passed and replay.is_identical

        report = {
            "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "runtime": {
                "python_version": py_platform.python_version(),
                "system": py_platform.system(),
                "machine": py_platform.machine(),
            },
            "fixture": bundle.bundle_name,
            "config_version": cfg.config_version,
            "config_hash": cfg.config_hash(),
            "tolerances": {"iv_tolerance": iv_tol, "tte_tolerance": tte_tol},
            "summary": {
                "contracts_evaluated": len(comparisons),
                "all_passed": all_passed,
                "max_abs_iv_error": max_iv_err,
                "max_abs_tte_error": max_tte_err,
                "max_price_residual": max_residual,
                "replay_identical": replay.is_identical,
                "snapshot_hash": res.snapshot.snapshot_hash,
            },
            "comparisons": comparisons,
        }

        out_path = ROOT / "reports" / "evaluation_report.json"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(
            f"Evaluation complete: {len(comparisons)} contracts, all_passed={all_passed}, "
            f"max_iv_err={max_iv_err:.3e}, replay_identical={replay.is_identical}"
        )
        return 0 if all_passed else 1
    finally:
        platform.close()


if __name__ == "__main__":
    raise SystemExit(main())
