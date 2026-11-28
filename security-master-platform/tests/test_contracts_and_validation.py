"""M1 contract, schema validation, licensing, and configuration failure tests."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from sec_master.adapters import raw_record_to_envelope
from sec_master.config import PlatformConfig, StorageConfig, load_config
from sec_master.domain import BitemporalStateEngine
from sec_master.errors import (
    ConfigError,
    LicensingError,
    SecurityMasterError,
    UnsupportedEntityError,
    ValidationError,
)
from sec_master.models import make_canonical_instrument_id, make_canonical_listing_id
from sec_master.validation import compute_isin_check_digit, validate_isin_check_digit

PROJECT_ROOT = Path(__file__).resolve().parent.parent


class TestContractsAndValidation(unittest.TestCase):
    def setUp(self) -> None:
        base = load_config(PROJECT_ROOT / "config/default.json")
        self.cfg = PlatformConfig(
            config_version=base.config_version,
            schema_version=base.schema_version,
            environment=base.environment,
            storage=StorageConfig(backend="memory", sqlite_path=":memory:"),
            licensing=base.licensing,
            conflict_resolution=base.conflict_resolution,
            quality=base.quality,
            reproducibility=base.reproducibility,
            http=base.http,
        )

    def test_canonical_ids_are_deterministic_and_ticker_independent(self) -> None:
        id1 = make_canonical_instrument_id("SYNTH_ACME_LEGACY", "EQUITY")
        id2 = make_canonical_instrument_id("synth_acme_legacy", "equity")
        self.assertEqual(id1, id2)
        self.assertTrue(id1.startswith("SEC-"))
        self.assertEqual(len(id1), 16)

        lst_ny = make_canonical_listing_id(id1, "XNYS", "USD")
        lst_ln = make_canonical_listing_id(id1, "XLON", "GBP")
        self.assertNotEqual(lst_ny, lst_ln)
        self.assertTrue(lst_ny.startswith("LST-"))

    def test_isin_luhn_check_digit_validation(self) -> None:
        body = "US000000101"
        chk = compute_isin_check_digit(body)
        valid_isin = body + chk
        self.assertTrue(validate_isin_check_digit(valid_isin))
        bad_digit = str((int(chk) + 1) % 10)
        self.assertFalse(validate_isin_check_digit(body + bad_digit))
        self.assertFalse(validate_isin_check_digit("INVALID"))

    def test_boundary_and_invalid_fixture_cases(self) -> None:
        fixture = json.loads(
            (PROJECT_ROOT / "data/fixtures/boundary_and_invalid.json").read_text(encoding="utf-8")
        )
        engine = BitemporalStateEngine(self.cfg)
        for case in fixture["cases"]:
            with self.subTest(case=case["case_id"]):
                with self.assertRaises(SecurityMasterError) as ctx:
                    env = raw_record_to_envelope(case["event"])
                    engine.apply_event(env)
                self.assertEqual(ctx.exception.code, case["expected_error_code"])

    def test_unlicensed_sedol_rejected_with_licensing_error(self) -> None:
        engine = BitemporalStateEngine(self.cfg)
        env = raw_record_to_envelope(
            {
                "event_type": "IDENTIFIER_MAP",
                "schema_version": "1.0.0",
                "source_system": "VENDOR_ALPHA",
                "source_event_id": "LIC-SEDOL-1",
                "valid_from": "2024-01-01T00:00:00Z",
                "valid_to": None,
                "observed_at": "2024-01-01T02:00:00Z",
                "payload": {
                    "instrument_id": "SEC-E19954A68605",
                    "listing_id": None,
                    "namespace": "SEDOL",
                    "identifier_value": "B000001",
                    "mic": None,
                },
            }
        )
        with self.assertRaises(LicensingError):
            engine.apply_event(env)

    def test_config_errors_on_missing_malformed_or_unsupported(self) -> None:
        with self.assertRaises(ConfigError):
            load_config(None)

        with self.assertRaises(ConfigError):
            load_config(PROJECT_ROOT / "config/does_not_exist.json")

        with tempfile.TemporaryDirectory() as tmp:
            bad_json = Path(tmp) / "bad.json"
            bad_json.write_text("{not valid json", encoding="utf-8")
            with self.assertRaises(ConfigError):
                load_config(bad_json)

            pg_cfg = json.loads((PROJECT_ROOT / "config/default.json").read_text(encoding="utf-8"))
            pg_cfg["storage"]["backend"] = "postgres"
            pg_path = Path(tmp) / "pg.json"
            pg_path.write_text(json.dumps(pg_cfg), encoding="utf-8")
            with self.assertRaises(ConfigError):
                load_config(pg_path)

            silent_cfg = json.loads(
                (PROJECT_ROOT / "config/default.json").read_text(encoding="utf-8")
            )
            silent_cfg["conflict_resolution"]["policy"] = "silent_last_write_wins"
            silent_path = Path(tmp) / "silent.json"
            silent_path.write_text(json.dumps(silent_cfg), encoding="utf-8")
            with self.assertRaises(ConfigError):
                load_config(silent_path)


if __name__ == "__main__":
    unittest.main()
