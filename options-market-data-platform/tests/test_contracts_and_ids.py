"""Tests for canonical option IDs, contract validation, CSV adapter, and optional Polars adapter."""

from __future__ import annotations

import sys
import tempfile
import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from omdp.adapters.csv_adapter import load_option_quotes_csv, load_underlying_quotes_csv
from omdp.adapters.polars_adapter import (
    OptionalDependencyError,
    features_to_polars_dataframe,
    features_to_records,
    is_polars_available,
)
from omdp.domain.contracts import (
    CanonicalOptionID,
    ContractMetadata,
    ContractValidationError,
    OptionRight,
    parse_utc_timestamp,
)


class TestCanonicalOptionIDAndContracts(unittest.TestCase):
    def test_canonical_id_and_occ_roundtrip(self) -> None:
        cid = CanonicalOptionID(
            underlying_symbol="aapl",
            expiry_date=date(2026, 6, 19),
            option_right=OptionRight.CALL,
            strike=Decimal("150.5"),
        )
        self.assertEqual(cid.canonical, "OPT:AAPL:20260619:C:150.5000")
        self.assertEqual(cid.occ_symbol, "AAPL  260619C00150500")

        parsed = CanonicalOptionID.parse(cid.canonical)
        self.assertEqual(parsed, cid)

        from_occ = CanonicalOptionID.from_occ("AAPL  260619C00150500")
        self.assertEqual(from_occ, cid)

    def test_invalid_canonical_id_and_naive_timestamp_rejected(self) -> None:
        with self.assertRaises(ContractValidationError):
            CanonicalOptionID.parse("INVALID:ID")
        with self.assertRaises(ContractValidationError):
            CanonicalOptionID("AAPL", date(2026, 6, 19), OptionRight.CALL, Decimal("-10"))
        with self.assertRaises(ContractValidationError):
            CanonicalOptionID("AAPL", date(2026, 6, 19), OptionRight.CALL, Decimal("100.12345"))
        with self.assertRaises(ContractValidationError):
            parse_utc_timestamp("2026-06-19T20:00:00")  # Missing timezone

    def test_contract_metadata_rejects_mismatched_id_or_excess_decimals(self) -> None:
        with self.assertRaises(ContractValidationError):
            ContractMetadata(
                option_id="OPT:AAPL:20260619:C:155.0000",
                contract_version=1,
                underlying_symbol="AAPL",
                expiry_date=date(2026, 6, 19),
                option_right=OptionRight.CALL,
                strike=Decimal("150.00"),
                strike_scale_decimals=2,
                contract_multiplier=100,
                exercise_style="EUROPEAN",
                currency="USD",
                source_id="test",
                schema_version="1.0.0",
                ingestion_time=datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc),
            )

        with self.assertRaises(ContractValidationError):
            ContractMetadata(
                option_id="OPT:AAPL:20260619:C:150.1250",
                contract_version=1,
                underlying_symbol="AAPL",
                expiry_date=date(2026, 6, 19),
                option_right=OptionRight.CALL,
                strike=Decimal("150.125"),
                strike_scale_decimals=2,
                contract_multiplier=100,
                exercise_style="EUROPEAN",
                currency="USD",
                source_id="test",
                schema_version="1.0.0",
                ingestion_time=datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc),
            )

    def test_csv_adapters_parse_valid_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            opt_csv = Path(tmpdir) / "options.csv"
            opt_csv.write_text(
                "snapshot_id,option_id,quote_version,underlying_symbol,reported_strike,"
                "event_time,ingestion_time,bid,ask,bid_size,ask_size,source_id,source_sequence,schema_version\n"
                "S1,OPT:SYNTH:20270619:C:100.0000,1,SYNTH,100.00,"
                "2026-06-19T19:59:59Z,2026-06-19T20:00:00Z,9.75,9.85,10,12,csv_feed,1,1.0.0\n",
                encoding="utf-8",
            )
            und_csv = Path(tmpdir) / "underlying.csv"
            und_csv.write_text(
                "underlying_symbol,quote_version,event_time,ingestion_time,bid,ask,last_price,"
                "source_id,source_sequence,schema_version\n"
                "SYNTH,1,2026-06-19T19:59:58Z,2026-06-19T19:59:59Z,99.95,100.05,100.00,csv_feed,1,1.0.0\n",
                encoding="utf-8",
            )
            oqs = load_option_quotes_csv(opt_csv)
            uqs = load_underlying_quotes_csv(und_csv)
            self.assertEqual(len(oqs), 1)
            self.assertEqual(oqs[0].mid, Decimal("9.80"))
            self.assertEqual(len(uqs), 1)
            self.assertEqual(uqs[0].mid, Decimal("100.00"))

    def test_polars_adapter_handles_availability_explicitly(self) -> None:
        self.assertEqual(features_to_records([]), [])
        if not is_polars_available():
            with self.assertRaises(OptionalDependencyError):
                features_to_polars_dataframe([])


if __name__ == "__main__":
    unittest.main()
