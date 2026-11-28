"""M3 integration tests: PIT no-hindsight queries, late correction replay, merger quarantine, persistence."""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

from corp_actions.adapters.json_adapter import load_fixture_bundle
from corp_actions.config import EngineConfig
from corp_actions.engine import CorporateActionsEngine
from corp_actions.errors import UnsupportedActionError

PROJECT_ROOT = Path(__file__).resolve().parents[1]
FIXTURES_DIR = PROJECT_ROOT / "data" / "fixtures"


def _utc(iso: str) -> datetime:
    return datetime.fromisoformat(iso.replace("Z", "+00:00")).astimezone(timezone.utc)


class TestPitQueriesAndReplay(unittest.TestCase):
    def setUp(self) -> None:
        self.config = EngineConfig.from_file(PROJECT_ROOT / "config" / "engine_config.json")
        self.engine = CorporateActionsEngine(config=self.config)
        self.bundle = load_fixture_bundle(FIXTURES_DIR)
        prices, positions, fx_rates, actions = self.bundle
        self.engine.ingest_prices(prices)
        self.engine.ingest_positions(positions)
        self.engine.ingest_fx_rates(fx_rates)
        self.engine.ingest_actions(actions)
        self.oracle = json.loads(
            (FIXTURES_DIR / "expected_oracle.json").read_text(encoding="utf-8")
        )["scenarios"]

    def tearDown(self) -> None:
        self.engine.close()

    def test_no_future_known_action_in_pit_snapshot(self) -> None:
        # On 2025-01-05T12:00:00Z, the Jan 6 split was announced (Jan 3) but has NOT gone ex yet,
        # and the Jan 8 special dividend, Jan 10 symbol change, and Jan 13 dividend are NOT even announced.
        snap = self.engine.query_as_of(
            "SEC-ACME",
            as_of_time=_utc("2025-01-05T12:00:00Z"),
            account_id="ACCT-SIM-001",
        )
        # Zero actions applied on historical bars up to 2025-01-03
        self.assertEqual(len(snap.effective_actions), 0)
        # Pending announced split is tracked transparently in diagnostics
        self.assertEqual(
            snap.diagnostics["pending_future_ex_actions"],
            ["CA-ACME-SPLIT-01@v1(ex=2025-01-06)"],
        )
        # No future-known events leaked anywhere
        all_mentioned = str(snap.to_dict())
        for future_id in ["CA-ACME-SDIV-01", "CA-ACME-SYM-01", "CA-ACME-DIV-01"]:
            self.assertNotIn(future_id, all_mentioned)
        # Prices remain unadjusted at 200.00
        self.assertEqual(snap.adjusted_series[-1].tr_adj_close, Decimal("200.000000"))
        self.assertEqual(snap.position_ledger[-1].quantity, Decimal("100.000000"))

    def test_late_correction_pit_before_and_replay_diff(self) -> None:
        # Before v2 correction arrives (2025-01-13T17:00:00Z), only v1 ($0.90) is known
        spec = self.oracle["acme_pit_before_correction"]
        snap_before = self.engine.query_as_of(
            spec["security_id"],
            as_of_time=_utc(spec["as_of_time"]),
            end_date=date.fromisoformat(spec["end_date"]),
            account_id=spec["account_id"],
        )
        self.assertEqual(
            [f"{a.action_id}@v{a.version}" for a in snap_before.effective_actions],
            spec["expected_effective_action_ids"],
        )
        adj_before = {b.trade_date.isoformat(): b for b in snap_before.adjusted_series}
        self.assertEqual(format(adj_before["2025-01-02"].tr_adj_close, "f"), "44.100000")
        self.assertEqual(format(snap_before.position_ledger[-1].pending_receivable_cash, "f"), "360.00")

        # Replay from before correction to after correction (same end_date window 2025-01-13)
        diff = self.engine.replay_correction(
            "SEC-ACME",
            from_as_of=_utc("2025-01-13T22:00:00Z"),
            to_as_of=_utc("2025-01-15T22:00:00Z"),
            end_date=date(2025, 1, 13),
            account_id="ACCT-SIM-001",
        )
        self.assertEqual(diff.corrected_actions, ("CA-ACME-DIV-01@v1->v2",))
        self.assertEqual(diff.added_actions, ())
        self.assertEqual(diff.cancelled_actions, ())
        # All dates before 2025-01-13 changed their TR adjusted close by -0.90 (44.10 -> 43.20)
        self.assertIn(date(2025, 1, 2), diff.changed_dates)
        self.assertNotIn(date(2025, 1, 13), diff.changed_dates)
        self.assertEqual(diff.price_deltas["2025-01-02"]["tr_adj_close_delta"], "-0.900000")
        # 400 shares * ($1.80 - $0.90) = +$360.00 cash delta
        self.assertEqual(diff.cash_delta_end, Decimal("360.00"))
        self.assertEqual(diff.quantity_delta_end, Decimal("0.000000"))
        self.assertNotEqual(diff.from_digest, diff.to_digest)

    def test_unsupported_merger_quarantined_and_strict_mode_raises(self) -> None:
        # Default mode: merger explicitly quarantined, prior valid actions preserved
        snap = self.engine.query_as_of(
            "SEC-EURO",
            as_of_time=_utc("2025-01-10T22:00:00Z"),
            account_id="ACCT-SIM-001",
        )
        self.assertEqual(len(snap.quarantined_actions), 1)
        q = snap.quarantined_actions[0]
        self.assertEqual(q.action_id, "CA-EURO-MRG-01")
        self.assertEqual(q.action_type, "MERGER")
        self.assertEqual(q.reason_code, "UNSUPPORTED_ACTION_TYPE")
        assert snap.reconciliation is not None
        self.assertEqual(snap.reconciliation.quarantined_action_count, 1)

        # Strict mode: unsupported merger fails explicitly on ingestion/query
        strict_cfg = EngineConfig(strict_unsupported_actions=True)
        strict_engine = CorporateActionsEngine(config=strict_cfg)
        try:
            prices, positions, fx_rates, actions = self.bundle
            strict_engine.ingest_prices(prices)
            strict_engine.ingest_positions(positions)
            strict_engine.ingest_fx_rates(fx_rates)
            with self.assertRaises(UnsupportedActionError):
                strict_engine.ingest_actions(actions)
        finally:
            strict_engine.close()

    def test_durable_sqlite_persistence_across_restart_rerun(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_file = str(Path(tmpdir) / "corp_actions.sqlite")
            cfg = EngineConfig(sqlite_path=db_file)

            # First process lifecycle: ingest and query
            eng1 = CorporateActionsEngine(config=cfg)
            prices, positions, fx_rates, actions = self.bundle
            eng1.ingest_prices(prices)
            eng1.ingest_positions(positions)
            eng1.ingest_fx_rates(fx_rates)
            eng1.ingest_actions(actions)
            snap1 = eng1.query_as_of(
                "SEC-ACME",
                as_of_time=_utc("2025-01-15T22:00:00Z"),
                account_id="ACCT-SIM-001",
            )
            eng1.close()

            # Second process lifecycle (simulated restart): open existing DB without re-ingesting
            eng2 = CorporateActionsEngine(config=cfg)
            try:
                snap2 = eng2.query_as_of(
                    "SEC-ACME",
                    as_of_time=_utc("2025-01-15T22:00:00Z"),
                    account_id="ACCT-SIM-001",
                )
            finally:
                eng2.close()

            self.assertEqual(snap1.output_digest, snap2.output_digest)
            self.assertEqual(snap1.run_id, snap2.run_id)
            self.assertEqual(snap1.adjusted_series, snap2.adjusted_series)
            self.assertEqual(snap1.position_ledger, snap2.position_ledger)


if __name__ == "__main__":
    unittest.main()
