import json
from unittest.mock import Mock

from altdata.connectors import FixtureConnector, TemporaryFailure
from altdata.domain import Problem
from tests.helpers import FIRST, SECOND, PlatformTest


class Operations(PlatformTest):
    def test_late_publication_and_embargo(self):
        self.stamp = SECOND  # Worker returns two days after publication.
        self.ingest("macro")
        self.assertEqual(self.export("macro", as_of=FIRST), [])
        self.assertEqual({r["available_at"] for r in self.export("macro")}, {"2025-02-03T10:00:00.000000Z"})
        self.stamp = FIRST
        self.ingest("news")
        self.assertEqual(self.export("news"), [])
        self.stamp = "2025-02-01T10:00:00Z"
        self.assertEqual(len(self.export("news")), 1)

    def test_publication_after_early_delivery(self):
        self.stamp = "2025-01-31T00:00:00Z"
        self.ingest("macro")
        self.assertEqual(self.export("macro"), [])
        self.stamp = FIRST
        self.assertEqual(len(self.export("macro")), 2)

    def test_revised_macro_series_and_missingness(self):
        self.limit("macro", 1)
        self.ingest("macro")
        original = self.export("macro")
        self.stamp = SECOND
        self.limit("macro", 2)
        self.ingest("macro")
        self.assertEqual(self.export("macro", as_of=FIRST), original)
        self.assertEqual([r["value"] for r in self.export("macro")], [None, "102.4"])
        self.assertEqual(original[0]["missing_reason"], "survey unavailable")
        history = self.export("macro", history=True)
        self.assertEqual([(r["revision"], r["value"]) for r in history if r["value"]],
                         [(2, "102.4"), (1, "101.2")])

    def test_duplicate_article_and_ambiguous_entity(self):
        health = self.ingest("news")
        self.assertEqual(health["events"]["duplicate"], 1)
        self.assertEqual(health["quality"], {"ambiguous_entity": 1})
        self.assertEqual(health["records"], 1)
        issue = self.store.db.execute("SELECT ordinal, detail FROM issues").fetchone()
        self.assertEqual(tuple(issue), (2, "aster"))
        self.assertEqual(self.ingest("news")["events"]["duplicate"], 1)

    def test_rate_limit_retry_after(self):
        class Limited(FixtureConnector):
            calls = 0

            def fetch(self, sequence):
                self.calls += 1
                if self.calls == 1:
                    raise TemporaryFailure("rate_limit", "fixture 429", retry_after=2)
                return super().fetch(sequence)

        sleeper = Mock()
        self.store.ingest("macro", self.feeds, factory=Limited, sleep=sleeper)
        sleeper.assert_called_once_with(2)
        health = self.store.status("macro")
        self.assertEqual(health["cursor"], 2)
        self.assertEqual(health["events"]["rate_limit"], 1)
        self.assertEqual(self.ingest("macro")["records"], 3)

    def test_outage_and_resume(self):
        class Down(FixtureConnector):
            def fetch(self, sequence):
                if sequence == 2:
                    raise TemporaryFailure("source_unavailable", "outage")
                return super().fetch(sequence)

        with self.assertRaises(TemporaryFailure):
            self.ingest("macro", Down)
        health = self.store.status("macro")
        self.assertEqual((health["cursor"], health["pending_pages"], health["next_sequence"]), (1, 1, 2))
        self.assertEqual(health["events"]["source_unavailable"], 3)
        health = self.ingest("macro")
        self.assertEqual((health["records"], health["last_error"]), (3, None))

    def test_missing_page_blocks_later_pages(self):
        page = self.feeds / "news/1.ndjson"
        original = page.read_bytes()
        page.unlink()
        with self.assertRaises(Problem):
            self.ingest("news")
        health = self.store.status("news")
        self.assertEqual((health["last_error"], health["pending_pages"]), ("feed_gap", 2))
        page.write_bytes(original)
        self.assertEqual(self.ingest("news")["cursor"], 2)

    def test_revoked_entitlement_and_source_removal(self):
        self.ingest("news")
        self.policy["sources"]["news"]["enabled"] = False
        self.change()
        factory = Mock()
        with self.assertRaises(Problem):
            self.ingest("news", factory)
        factory.assert_not_called()
        with self.assertRaises(Problem):
            self.export("news", as_of=FIRST)
        del self.policy["sources"]["news"]
        self.change()
        with self.assertRaises(Problem):
            self.export("news")
        self.assertFalse(self.store.status("news")["entitled"])
        self.assertEqual(self.store.status("news")["records"], 1)

    def test_revocation_during_fetch(self):
        outer = self

        class Revoked(FixtureConnector):
            def fetch(self, sequence):
                outer.policy["sources"]["macro"]["enabled"] = False
                outer.change()
                return super().fetch(sequence)

        with self.assertRaises(Problem):
            self.ingest("macro", Revoked)
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM blobs").fetchone()[0], 0)

    def test_historical_license_survives_policy_change(self):
        self.stamp = SECOND
        self.ingest("news")
        self.policy["sources"]["news"]["export_purposes"].append("research")
        self.change()
        with self.assertRaises(Problem):
            self.store.export(["news"], "research")
        bundle = self.store.export(["news"], "demo")
        self.assertEqual({x["registry_version"] for x in bundle["licenses"]}, {1, 2})
        self.assertTrue(all("No redistribution" in x["terms"] for x in bundle["licenses"]))

    def test_expiration_checked_even_for_historical_export(self):
        self.ingest("macro")
        self.stamp = "2099-01-01T00:00:00Z"
        with self.assertRaises(Problem):
            self.export("macro", as_of=FIRST)
        with self.assertRaises(Problem):
            self.ingest("macro")

    def test_corrupt_payload_archived_and_recoverable(self):
        page = self.feeds / "macro/2.json"
        good = page.read_bytes()
        page.write_bytes(b"{broken")
        with self.assertRaises(Problem):
            self.ingest("macro")
        health = self.store.status("macro")
        self.assertEqual((health["cursor"], health["last_error"]), (1, "invalid_json"))
        self.assertEqual(health["quality"], {"invalid_json": 1})
        page.write_bytes(good)
        self.assertEqual(self.ingest("macro")["records"], 3)
        receipts = self.store.db.execute("SELECT b.payload FROM receipts r JOIN blobs b ON r.hash=b.hash "
                                         "WHERE r.sequence=2 ORDER BY r.id").fetchall()
        self.assertEqual([r[0] for r in receipts], [b"{broken", good])

    def test_conflicting_revision_is_quarantined(self):
        page = self.feeds / "macro/2.json"
        doc = json.loads(page.read_bytes())
        doc["observations"][0]["revision"] = 1
        page.write_text(json.dumps(doc))
        health = self.ingest("macro")
        self.assertEqual(health["quality"], {"revision_conflict": 1})
        self.assertEqual(self.export("macro")[-1]["value"], "101.2")
