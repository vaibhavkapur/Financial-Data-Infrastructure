import hashlib
import json
import sqlite3
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from unittest.mock import patch

from altdata.connectors import FixtureConnector
from altdata.domain import Problem
from altdata.store import Store
from tests.helpers import FIRST, ROOT, PlatformTest


class Pipeline(PlatformTest):
    def test_archive_round_trip_and_restart(self):
        self.ingest("macro")
        raw = (self.feeds / "macro/1.json").read_bytes()
        digest = hashlib.sha256(raw).hexdigest()
        self.assertEqual(self.store.db.execute("SELECT payload FROM blobs WHERE hash=?", (digest,)).fetchone()[0], raw)
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.db.execute("UPDATE blobs SET payload=? WHERE hash=?", (b"changed", digest))
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.db.execute("DELETE FROM receipts")
        reopened = Store(self.path / "db", clock=lambda: FIRST)
        try:
            reopened.ingest("macro", self.feeds)
            self.assertEqual(reopened.db.execute("SELECT count(*) FROM receipts").fetchone()[0], 2)
            row = reopened.export(["macro"], "demo")["records"][0]
            self.assertEqual(row["raw_sha256"], digest)
            self.assertEqual(row["registry_version"], 1)
            self.assertEqual(row["source"], "macro")
        finally:
            reopened.db.close()

    def test_transaction_recovery(self):
        with patch.object(self.store, "save_record", side_effect=RuntimeError("interrupted")):
            with self.assertRaises(RuntimeError):
                self.ingest("macro")
        self.assertEqual(self.store.status("macro")["cursor"], 0)
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM receipts").fetchone()[0], 0)
        self.assertEqual(self.ingest("macro")["records"], 3)

    def test_concurrent_workers_commit_page_once(self):
        gate = Barrier(2)

        class Together(FixtureConnector):
            def fetch(self, sequence):
                if sequence == 1:
                    gate.wait(timeout=5)
                return super().fetch(sequence)

        def worker(_):
            local = Store(self.path / "db", clock=lambda: FIRST)
            try:
                local.ingest("macro", self.feeds, factory=Together)
            finally:
                local.db.close()

        with ThreadPoolExecutor(2) as pool:
            list(pool.map(worker, range(2)))
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM receipts").fetchone()[0], 2)
        self.assertEqual(self.store.status("macro")["records"], 3)

    def test_registry_replay_and_rollback(self):
        self.store.apply(self.policy)
        self.policy["sources"]["news"]["enabled"] = False
        with self.assertRaisesRegex(Problem, "higher version"):
            self.store.apply(self.policy)
        self.change()
        self.policy["version"] = 1
        with self.assertRaises(Problem):
            self.store.apply(self.policy)

    def test_cli_curated_export_and_denial(self):
        def cli(*args):
            return subprocess.run([sys.executable, "-m", "altdata", "--db", str(self.path/"cli.db"), *args],
                                  cwd=ROOT, text=True, capture_output=True)
        self.assertEqual(cli("apply-registry", "config/registry.json").returncode, 0)
        self.assertEqual(cli("ingest", "news").returncode, 0)
        good = cli("export", "--source", "news", "--purpose", "demo")
        self.assertEqual(json.loads(good.stdout)["records"][0]["entity_id"], "company-aster")
        denied = cli("export", "--source", "macro", "--source", "news", "--purpose", "research")
        self.assertNotEqual(denied.returncode, 0)
        self.assertEqual(denied.stdout, "")
        self.assertEqual(json.loads(denied.stderr)["error"], "export_denied")
