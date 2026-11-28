"""M4 tests: total-return reconciliation, same-day action boundaries, operational resilience, CLI."""

from __future__ import annotations

import io
import json
import random
import unittest
from contextlib import redirect_stderr, redirect_stdout
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from corp_actions.cli import main as cli_main
from corp_actions.config import EngineConfig
from corp_actions.contracts import (
    ActionType,
    CorporateActionEvent,
    FxRate,
    PositionSnapshot,
    RawPriceBar,
    RevisionType,
)
from corp_actions.engine import CorporateActionsEngine
from corp_actions.errors import (
    MissingFxRateError,
    MissingMarketDataError,
    SameDayConflictError,
    ValidationError,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _utc(y: int, m: int, d: int, h: int = 12) -> datetime:
    return datetime(y, m, d, h, 0, tzinfo=timezone.utc)


def _bar(sec: str, sym: str, d: date, close: str, vol: str = "10000", lag_sec: int = 300) -> RawPriceBar:
    c = Decimal(close)
    et = datetime(d.year, d.month, d.day, 21, 0, tzinfo=timezone.utc)
    return RawPriceBar(
        security_id=sec,
        symbol=sym,
        trade_date=d,
        open=c,
        high=c,
        low=c,
        close=c,
        volume=Decimal(vol),
        currency="USD",
        event_time=et,
        ingestion_time=et + timedelta(seconds=lag_sec),
        source="synthetic-test",
    )


class TestReconciliationAndBoundaries(unittest.TestCase):
    def test_same_day_compatible_split_regular_and_special_dividend_with_market_noise(self) -> None:
        """Multiple compatible actions on same day (2:1 split + regular + special div) with noisy prices."""
        rng = random.Random(42)
        eng = CorporateActionsEngine(config=EngineConfig(same_day_dividend_basis="post_split"))
        try:
            bars = [
                _bar("SEC-MULTI", "MLT", date(2025, 1, 2), "100.00"),
                _bar("SEC-MULTI", "MLT", date(2025, 1, 3), "104.00"),
                # Ex-date 2025-01-06: 2:1 split (ref close = 52.00) + $1.00 reg + $3.00 special = $4.00 div
                # Ex-open ref = 48.00, plus +2.5% noisy move -> close = 49.20
                _bar("SEC-MULTI", "MLT", date(2025, 1, 6), "49.20", vol="20000"),
                _bar("SEC-MULTI", "MLT", date(2025, 1, 7), str((Decimal("49.20") * Decimal(str(round(1 + rng.uniform(-0.03, 0.03), 4)))).quantize(Decimal("0.01")))),
            ]
            pos = PositionSnapshot(
                "ACCT-M", "SEC-MULTI", "MLT", date(2025, 1, 2),
                Decimal("100"), Decimal("80.00"), "USD", Decimal("0.00"),
                _utc(2025, 1, 2, 21), _utc(2025, 1, 2, 21), "test",
            )
            actions = [
                CorporateActionEvent(
                    action_id="CA-M-SPLIT", version=1, revision_type=RevisionType.NEW,
                    action_type=ActionType.SPLIT, security_id="SEC-MULTI", symbol="MLT",
                    announcement_date=date(2025, 1, 3), ex_date=date(2025, 1, 6),
                    ratio_numerator=Decimal("2"), ratio_denominator=Decimal("1"),
                    event_time=_utc(2025, 1, 3, 12), ingestion_time=_utc(2025, 1, 3, 12),
                    availability_time=_utc(2025, 1, 3, 12), source="test", lineage_id="L1",
                ),
                CorporateActionEvent(
                    action_id="CA-M-DIV", version=1, revision_type=RevisionType.NEW,
                    action_type=ActionType.CASH_DIVIDEND, security_id="SEC-MULTI", symbol="MLT",
                    announcement_date=date(2025, 1, 3), ex_date=date(2025, 1, 6),
                    pay_date=date(2025, 1, 7), cash_amount=Decimal("1.00"), currency="USD",
                    event_time=_utc(2025, 1, 3, 12), ingestion_time=_utc(2025, 1, 3, 12),
                    availability_time=_utc(2025, 1, 3, 12), source="test", lineage_id="L2",
                ),
                CorporateActionEvent(
                    action_id="CA-M-SDIV", version=1, revision_type=RevisionType.NEW,
                    action_type=ActionType.SPECIAL_DIVIDEND, security_id="SEC-MULTI", symbol="MLT",
                    announcement_date=date(2025, 1, 3), ex_date=date(2025, 1, 6),
                    pay_date=date(2025, 1, 7), cash_amount=Decimal("3.00"), currency="USD",
                    event_time=_utc(2025, 1, 3, 12), ingestion_time=_utc(2025, 1, 3, 12),
                    availability_time=_utc(2025, 1, 3, 12), source="test", lineage_id="L3",
                ),
            ]
            eng.ingest_prices(bars)
            eng.ingest_positions([pos])
            eng.ingest_actions(actions)

            snap = eng.query_as_of("SEC-MULTI", as_of_time=_utc(2025, 1, 8, 12), account_id="ACCT-M")
            # Split 2:1 -> 200 shares; $4.00/share total dividend -> $800.00 cash settled on Jan 7
            self.assertEqual(snap.position_ledger[-1].quantity, Decimal("200.000000"))
            self.assertEqual(snap.position_ledger[-1].settled_cash_balance, Decimal("800.00"))
            # Factor on Jan 3: split 0.5, div (52-4)/52 = 48/52 = 12/13
            # TR adjusted series reconciles EXACTLY with reinvested portfolio return even with price moves!
            assert snap.reconciliation is not None
            self.assertTrue(snap.reconciliation.reconciled)
            self.assertLessEqual(snap.reconciliation.return_discrepancy, Decimal("0.000001"))
        finally:
            eng.close()

    def test_same_day_pre_split_dividend_basis_matches_post_split_equivalence(self) -> None:
        """A $2.00 pre-split dividend on a 2:1 split day equals a $1.00 post-split dividend."""
        eng = CorporateActionsEngine(config=EngineConfig(same_day_dividend_basis="pre_split"))
        try:
            bars = [
                _bar("SEC-PRE", "PRE", date(2025, 1, 3), "100.00"),
                _bar("SEC-PRE", "PRE", date(2025, 1, 6), "49.00"),
            ]
            pos = PositionSnapshot(
                "ACCT-P", "SEC-PRE", "PRE", date(2025, 1, 3),
                Decimal("50"), Decimal("100.00"), "USD", Decimal("0.00"),
                _utc(2025, 1, 3, 21), _utc(2025, 1, 3, 21), "test",
            )
            actions = [
                CorporateActionEvent(
                    action_id="CA-P-SPLIT", version=1, revision_type=RevisionType.NEW,
                    action_type=ActionType.SPLIT, security_id="SEC-PRE", symbol="PRE",
                    announcement_date=date(2025, 1, 3), ex_date=date(2025, 1, 6),
                    ratio_numerator=Decimal("2"), ratio_denominator=Decimal("1"),
                    event_time=_utc(2025, 1, 3, 12), ingestion_time=_utc(2025, 1, 3, 12),
                    availability_time=_utc(2025, 1, 3, 12), source="test", lineage_id="LP1",
                ),
                CorporateActionEvent(
                    action_id="CA-P-DIV", version=1, revision_type=RevisionType.NEW,
                    action_type=ActionType.CASH_DIVIDEND, security_id="SEC-PRE", symbol="PRE",
                    announcement_date=date(2025, 1, 3), ex_date=date(2025, 1, 6),
                    pay_date=date(2025, 1, 6), cash_amount=Decimal("2.00"), currency="USD",
                    dividend_basis="pre_split",
                    event_time=_utc(2025, 1, 3, 12), ingestion_time=_utc(2025, 1, 3, 12),
                    availability_time=_utc(2025, 1, 3, 12), source="test", lineage_id="LP2",
                ),
            ]
            eng.ingest_prices(bars)
            eng.ingest_positions([pos])
            eng.ingest_actions(actions)
            snap = eng.query_as_of("SEC-PRE", as_of_time=_utc(2025, 1, 7, 12), account_id="ACCT-P")
            # 50 pre-split shares * $2.00 = $100.00; factor = 0.5 * (50-1)/50 = 0.49
            self.assertEqual(snap.adjusted_series[0].total_return_factor, Decimal("0.49"))
            self.assertEqual(snap.position_ledger[-1].settled_cash_balance, Decimal("100.00"))
            assert snap.reconciliation is not None
            self.assertTrue(snap.reconciliation.reconciled)
        finally:
            eng.close()

    def test_conflicting_duplicate_splits_on_same_day_rejected(self) -> None:
        eng = CorporateActionsEngine()
        try:
            eng.ingest_prices([
                _bar("SEC-CONF", "CNF", date(2025, 1, 3), "100.00"),
                _bar("SEC-CONF", "CNF", date(2025, 1, 6), "50.00"),
            ])
            eng.ingest_actions([
                CorporateActionEvent(
                    action_id="CA-C1", version=1, revision_type=RevisionType.NEW,
                    action_type=ActionType.SPLIT, security_id="SEC-CONF", symbol="CNF",
                    announcement_date=date(2025, 1, 3), ex_date=date(2025, 1, 6),
                    ratio_numerator=Decimal("2"), ratio_denominator=Decimal("1"),
                    event_time=_utc(2025, 1, 3, 12), ingestion_time=_utc(2025, 1, 3, 12),
                    availability_time=_utc(2025, 1, 3, 12), source="vendor-a", lineage_id="LC1",
                ),
                CorporateActionEvent(
                    action_id="CA-C2", version=1, revision_type=RevisionType.NEW,
                    action_type=ActionType.SPLIT, security_id="SEC-CONF", symbol="CNF",
                    announcement_date=date(2025, 1, 3), ex_date=date(2025, 1, 6),
                    ratio_numerator=Decimal("3"), ratio_denominator=Decimal("1"),
                    event_time=_utc(2025, 1, 3, 12), ingestion_time=_utc(2025, 1, 3, 12),
                    availability_time=_utc(2025, 1, 3, 12), source="vendor-b", lineage_id="LC2",
                ),
            ])
            with self.assertRaises(SameDayConflictError) as ctx:
                eng.query_as_of("SEC-CONF", as_of_time=_utc(2025, 1, 7, 12))
            self.assertEqual(ctx.exception.code, "SAME_DAY_CONFLICT")
        finally:
            eng.close()

    def test_dividend_exceeding_prior_close_and_missing_fx_or_prior_bar(self) -> None:
        # 1. Dividend >= prior close raises ValidationError
        eng1 = CorporateActionsEngine()
        try:
            eng1.ingest_prices([
                _bar("SEC-BIGD", "BGD", date(2025, 1, 3), "10.00"),
                _bar("SEC-BIGD", "BGD", date(2025, 1, 6), "1.00"),
            ])
            eng1.ingest_actions([
                CorporateActionEvent(
                    action_id="CA-BGD", version=1, revision_type=RevisionType.NEW,
                    action_type=ActionType.SPECIAL_DIVIDEND, security_id="SEC-BIGD", symbol="BGD",
                    announcement_date=date(2025, 1, 3), ex_date=date(2025, 1, 6),
                    pay_date=date(2025, 1, 8), cash_amount=Decimal("12.00"), currency="USD",
                    event_time=_utc(2025, 1, 3, 12), ingestion_time=_utc(2025, 1, 3, 12),
                    availability_time=_utc(2025, 1, 3, 12), source="test", lineage_id="LB1",
                )
            ])
            with self.assertRaises(ValidationError):
                eng1.query_as_of("SEC-BIGD", as_of_time=_utc(2025, 1, 7, 12))
        finally:
            eng1.close()

        # 2. Missing FX rate raises MissingFxRateError
        eng2 = CorporateActionsEngine()
        try:
            eng2.ingest_prices([
                _bar("SEC-NOFX", "NFX", date(2025, 1, 3), "100.00"),
                _bar("SEC-NOFX", "NFX", date(2025, 1, 6), "98.00"),
            ])
            eng2.ingest_actions([
                CorporateActionEvent(
                    action_id="CA-NFX", version=1, revision_type=RevisionType.NEW,
                    action_type=ActionType.CASH_DIVIDEND, security_id="SEC-NOFX", symbol="NFX",
                    announcement_date=date(2025, 1, 3), ex_date=date(2025, 1, 6),
                    pay_date=date(2025, 1, 8), cash_amount=Decimal("2.00"), currency="GBP",
                    event_time=_utc(2025, 1, 3, 12), ingestion_time=_utc(2025, 1, 3, 12),
                    availability_time=_utc(2025, 1, 3, 12), source="test", lineage_id="LN1",
                )
            ])
            with self.assertRaises(MissingFxRateError):
                eng2.query_as_of("SEC-NOFX", as_of_time=_utc(2025, 1, 7, 12))
        finally:
            eng2.close()

        # 3. Missing prior close bar on dividend ex-date raises MissingMarketDataError
        eng3 = CorporateActionsEngine()
        try:
            eng3.ingest_prices([_bar("SEC-NOPR", "NPR", date(2025, 1, 6), "98.00")])
            eng3.ingest_actions([
                CorporateActionEvent(
                    action_id="CA-NPR", version=1, revision_type=RevisionType.NEW,
                    action_type=ActionType.CASH_DIVIDEND, security_id="SEC-NOPR", symbol="NPR",
                    announcement_date=date(2025, 1, 3), ex_date=date(2025, 1, 6),
                    pay_date=date(2025, 1, 8), cash_amount=Decimal("2.00"), currency="USD",
                    event_time=_utc(2025, 1, 3, 12), ingestion_time=_utc(2025, 1, 3, 12),
                    availability_time=_utc(2025, 1, 3, 12), source="test", lineage_id="LP0",
                )
            ])
            with self.assertRaises(MissingMarketDataError):
                eng3.query_as_of("SEC-NOPR", as_of_time=_utc(2025, 1, 7, 12))
        finally:
            eng3.close()

    def test_freshness_sla_breach_detected_in_diagnostics(self) -> None:
        eng = CorporateActionsEngine(config=EngineConfig(freshness_sla_seconds=600))
        try:
            eng.ingest_prices([
                _bar("SEC-LAG", "LAG", date(2025, 1, 2), "50.00", lag_sec=1200),
                _bar("SEC-LAG", "LAG", date(2025, 1, 3), "51.00", lag_sec=300),
            ])
            snap = eng.query_as_of("SEC-LAG", as_of_time=_utc(2025, 1, 4, 12))
            assert snap.reconciliation is not None
            self.assertEqual(snap.reconciliation.freshness_sla_breaches, 1)
            self.assertEqual(snap.reconciliation.max_freshness_lag_seconds, 1200.0)
        finally:
            eng.close()

    def test_cli_query_replay_and_error_exit_codes(self) -> None:
        out = io.StringIO()
        with redirect_stdout(out):
            rc = cli_main([
                "--config", str(PROJECT_ROOT / "config" / "engine_config.json"),
                "--fixtures", str(PROJECT_ROOT / "data" / "fixtures"),
                "query",
                "--security-id", "SEC-ACME",
                "--as-of", "2025-01-15T22:00:00Z",
            ])
        self.assertEqual(rc, 0)
        parsed = json.loads(out.getvalue())
        self.assertEqual(parsed["security_id"], "SEC-ACME")

        err = io.StringIO()
        with redirect_stderr(err):
            rc_err = cli_main([
                "--config", str(PROJECT_ROOT / "config" / "does_not_exist.json"),
                "--fixtures", str(PROJECT_ROOT / "data" / "fixtures"),
                "query",
                "--security-id", "SEC-ACME",
                "--as-of", "2025-01-15T22:00:00Z",
            ])
        self.assertEqual(rc_err, 2)
        self.assertEqual(json.loads(err.getvalue())["error_code"], "CONFIGURATION_ERROR")


if __name__ == "__main__":
    unittest.main()
