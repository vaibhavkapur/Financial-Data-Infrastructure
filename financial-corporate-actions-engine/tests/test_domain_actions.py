"""M2 tests: domain correctness for splits, dividends, symbol changes, and oracle matching."""

from __future__ import annotations

import json
import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

from corp_actions.adapters.json_adapter import load_fixture_bundle
from corp_actions.config import EngineConfig
from corp_actions.engine import CorporateActionsEngine

PROJECT_ROOT = Path(__file__).resolve().parents[1]
FIXTURES_DIR = PROJECT_ROOT / "data" / "fixtures"


def _utc(iso: str) -> datetime:
    return datetime.fromisoformat(iso.replace("Z", "+00:00")).astimezone(timezone.utc)


class TestDomainActionsAndOracle(unittest.TestCase):
    def setUp(self) -> None:
        self.config = EngineConfig.from_file(PROJECT_ROOT / "config" / "engine_config.json")
        self.engine = CorporateActionsEngine(config=self.config)
        prices, positions, fx_rates, actions = load_fixture_bundle(FIXTURES_DIR)
        self.engine.ingest_prices(prices)
        self.engine.ingest_positions(positions)
        self.engine.ingest_fx_rates(fx_rates)
        self.engine.ingest_actions(actions)
        self.oracle = json.loads(
            (FIXTURES_DIR / "expected_oracle.json").read_text(encoding="utf-8")
        )["scenarios"]

    def tearDown(self) -> None:
        self.engine.close()

    def test_acme_split_special_div_symbol_change_and_regular_div_matches_oracle(self) -> None:
        spec = self.oracle["acme_pit_after_correction"]
        snap = self.engine.query_as_of(
            spec["security_id"],
            as_of_time=_utc(spec["as_of_time"]),
            end_date=date.fromisoformat(spec["end_date"]),
            account_id=spec["account_id"],
        )

        # Effective actions match oracle
        got_ids = [f"{a.action_id}@v{a.version}" for a in snap.effective_actions]
        self.assertEqual(got_ids, spec["expected_effective_action_ids"])

        # Raw series is completely untouched and separated from adjusted series
        raw_by_date = {b.trade_date.isoformat(): b for b in snap.raw_series}
        adj_by_date = {b.trade_date.isoformat(): b for b in snap.adjusted_series}
        self.assertEqual(raw_by_date["2025-01-02"].close, Decimal("200.00"))
        self.assertEqual(adj_by_date["2025-01-02"].raw_close, Decimal("200.000000"))
        self.assertEqual(adj_by_date["2025-01-02"].split_adj_close, Decimal("50.000000"))
        self.assertEqual(adj_by_date["2025-01-02"].tr_adj_close, Decimal("43.200000"))
        # Volume adjusted inversely to price on 4:1 split
        self.assertEqual(adj_by_date["2025-01-02"].raw_volume, Decimal("10000.000000"))
        self.assertEqual(adj_by_date["2025-01-02"].split_adj_volume, Decimal("40000.000000"))

        # Check all oracle factor checkpoints
        for d_str, exp in spec["expected_factors_by_date"].items():
            bar = adj_by_date[d_str]
            self.assertEqual(bar.split_price_factor, Decimal(exp["split_price_factor"]))
            self.assertEqual(bar.dividend_factor, Decimal(exp["dividend_factor"]))
            self.assertEqual(bar.total_return_factor, Decimal(exp["total_return_factor"]))
            self.assertEqual(format(bar.split_adj_close, "f"), exp["split_adj_close"])
            self.assertEqual(format(bar.tr_adj_close, "f"), exp["tr_adj_close"])
            self.assertEqual(bar.active_symbol, exp["active_symbol"])

        # Verify symbol continuity across ACME -> ACMX on 2025-01-10
        self.assertEqual(adj_by_date["2025-01-09"].active_symbol, "ACME")
        self.assertEqual(adj_by_date["2025-01-10"].active_symbol, "ACMX")

        # Verify position quantity, cost basis conservation, and dividend cash ledger
        ledger_by_date = {p.trade_date.isoformat(): p for p in snap.position_ledger}
        # Pre-split
        self.assertEqual(ledger_by_date["2025-01-03"].quantity, Decimal("100.000000"))
        self.assertEqual(ledger_by_date["2025-01-03"].total_cost_basis, Decimal("16000.00"))
        # Post-split: 400 shares @ $40 cost basis = $16,000 preserved
        self.assertEqual(ledger_by_date["2025-01-06"].quantity, Decimal("400.000000"))
        self.assertEqual(ledger_by_date["2025-01-06"].cost_basis_per_share, Decimal("40.000000"))
        self.assertEqual(ledger_by_date["2025-01-06"].total_cost_basis, Decimal("16000.00"))
        # Special div accrues on ex_date (Jan 8) and settles on pay_date (Jan 10)
        self.assertEqual(ledger_by_date["2025-01-08"].pending_receivable_cash, Decimal("2000.00"))
        self.assertEqual(ledger_by_date["2025-01-08"].settled_cash_balance, Decimal("1000.00"))
        self.assertEqual(ledger_by_date["2025-01-10"].pending_receivable_cash, Decimal("0.00"))
        self.assertEqual(ledger_by_date["2025-01-10"].settled_cash_balance, Decimal("3000.00"))

        # End position matches oracle
        end_pos = snap.position_ledger[-1]
        exp_pos = spec["expected_end_position"]
        self.assertEqual(format(end_pos.quantity, "f"), exp_pos["quantity"])
        self.assertEqual(format(end_pos.cost_basis_per_share, "f"), exp_pos["cost_basis_per_share"])
        self.assertEqual(format(end_pos.settled_cash_balance, "f"), exp_pos["settled_cash_balance"])
        self.assertEqual(format(end_pos.pending_receivable_cash, "f"), exp_pos["pending_receivable_cash"])
        self.assertEqual(format(end_pos.cumulative_net_dividends, "f"), exp_pos["cumulative_net_dividends"])
        self.assertEqual(end_pos.active_symbol, exp_pos["active_symbol"])

        # Total-return reconciliation exact
        assert snap.reconciliation is not None
        self.assertTrue(snap.reconciliation.reconciled)
        self.assertTrue(snap.reconciliation.split_jump_checks_passed)
        self.assertEqual(
            format(snap.reconciliation.tr_series_return, "f"),
            spec["expected_reconciliation"]["tr_series_return"],
        )

    def test_reverse_split_and_cancelled_dividend_matches_oracle(self) -> None:
        spec = self.oracle["rev_reverse_split_and_cancelled_div"]
        snap = self.engine.query_as_of(
            spec["security_id"],
            as_of_time=_utc(spec["as_of_time"]),
            end_date=date.fromisoformat(spec["end_date"]),
            account_id=spec["account_id"],
        )
        adj_by_date = {b.trade_date.isoformat(): b for b in snap.adjusted_series}
        for d_str, exp in spec["expected_factors_by_date"].items():
            bar = adj_by_date[d_str]
            self.assertEqual(bar.split_price_factor, Decimal(exp["split_price_factor"]))
            self.assertEqual(bar.dividend_factor, Decimal(exp["dividend_factor"]))
            self.assertEqual(format(bar.tr_adj_close, "f"), exp["tr_adj_close"])

        end_pos = snap.position_ledger[-1]
        exp_pos = spec["expected_end_position"]
        self.assertEqual(format(end_pos.quantity, "f"), exp_pos["quantity"])
        self.assertEqual(format(end_pos.cost_basis_per_share, "f"), exp_pos["cost_basis_per_share"])
        self.assertEqual(format(end_pos.settled_cash_balance, "f"), exp_pos["settled_cash_balance"])
        self.assertEqual(format(end_pos.cumulative_net_dividends, "f"), exp_pos["cumulative_net_dividends"])
        # Cancelled dividend recorded in diagnostics
        self.assertIn("CA-REV-DIV-01@v2", snap.diagnostics["cancelled_actions"])

    def test_fx_dividend_withholding_and_same_day_split_matches_oracle(self) -> None:
        spec = self.oracle["euro_same_day_split_fx_div_and_merger"]
        snap = self.engine.query_as_of(
            spec["security_id"],
            as_of_time=_utc(spec["as_of_time"]),
            end_date=date.fromisoformat(spec["end_date"]),
            account_id=spec["account_id"],
        )
        adj_by_date = {b.trade_date.isoformat(): b for b in snap.adjusted_series}
        for d_str, exp in spec["expected_factors_by_date"].items():
            bar = adj_by_date[d_str]
            self.assertEqual(bar.split_price_factor, Decimal(exp["split_price_factor"]))
            self.assertEqual(bar.dividend_factor, Decimal(exp["dividend_factor"]))
            self.assertEqual(bar.total_return_factor, Decimal(exp["total_return_factor"]))
            self.assertEqual(format(bar.tr_adj_close, "f"), exp["tr_adj_close"])

        end_pos = snap.position_ledger[-1]
        exp_pos = spec["expected_end_position"]
        self.assertEqual(format(end_pos.quantity, "f"), exp_pos["quantity"])
        self.assertEqual(format(end_pos.settled_cash_balance, "f"), exp_pos["settled_cash_balance"])
        self.assertEqual(format(end_pos.cumulative_gross_dividends, "f"), exp_pos["cumulative_gross_dividends"])
        self.assertEqual(format(end_pos.cumulative_net_dividends, "f"), exp_pos["cumulative_net_dividends"])


if __name__ == "__main__":
    unittest.main()
