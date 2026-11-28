"""End-to-end tests for HTTP serving API and CLI commands."""

from __future__ import annotations

import io
import json
import sys
import tempfile
import threading
import unittest
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import replace
from http import HTTPStatus
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from omdp.adapters.json_adapter import load_fixture_bundle
from omdp.cli import main as cli_main
from omdp.config import StorageConfig, load_config
from omdp.service.http_api import dispatch_request
from omdp.service.platform import OptionsMarketDataPlatform


class TestHTTPAndCLI(unittest.TestCase):
    def test_http_endpoints_serve_chains_features_quality_and_replay(self) -> None:
        cfg = load_config(ROOT / "config" / "default_config.json")
        cfg = replace(cfg, storage=StorageConfig(backend="sqlite", sqlite_path=":memory:"))
        platform = OptionsMarketDataPlatform(config=cfg)
        try:
            bundle = load_fixture_bundle(ROOT / "data" / "fixtures" / "reference_chain.json")
            res = platform.ingest_bundle(bundle)[0]
            snap_id = res.snapshot.snapshot_id

            status, health = dispatch_request(platform, "GET", "/health")
            self.assertEqual(status, HTTPStatus.OK)
            self.assertEqual(health["status"], "ok")

            status, chains = dispatch_request(platform, "GET", "/v1/chains?underlying=SYNTH")
            self.assertEqual(status, HTTPStatus.OK)
            self.assertEqual(chains["count"], 1)

            status, snap_body = dispatch_request(platform, "GET", f"/v1/chains/{snap_id}")
            self.assertEqual(status, HTTPStatus.OK)
            self.assertEqual(snap_body["snapshot_hash"], res.snapshot.snapshot_hash)

            status, feats = dispatch_request(platform, "GET", f"/v1/chains/{snap_id}/features")
            self.assertEqual(status, HTTPStatus.OK)
            self.assertEqual(feats["count"], 6)

            status, qual = dispatch_request(platform, "GET", f"/v1/chains/{snap_id}/quality")
            self.assertEqual(status, HTTPStatus.OK)
            self.assertEqual(qual["computed_iv_count"], 6)
            self.assertEqual(qual["completeness_ratio"], 1.0)

            status, replay = dispatch_request(platform, "POST", f"/v1/chains/{snap_id}/replay")
            self.assertEqual(status, HTTPStatus.OK)
            self.assertTrue(replay["is_identical"])

            status, metrics = dispatch_request(platform, "GET", "/v1/metrics")
            self.assertEqual(status, HTTPStatus.OK)
            self.assertEqual(metrics["counters"]["replays_executed"], 1)

            status_404, _ = dispatch_request(platform, "GET", "/v1/chains/NONEXISTENT")
            self.assertEqual(status_404, HTTPStatus.NOT_FOUND)

            status_400, _ = dispatch_request(platform, "GET", "/v1/chains?start=not-a-timestamp")
            self.assertEqual(status_400, HTTPStatus.BAD_REQUEST)
        finally:
            platform.close()

    def test_cli_ingest_query_quality_replay_and_bad_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = str(Path(tmpdir) / "cli.sqlite3")
            cfg_path = str(ROOT / "config" / "default_config.json")
            fixture_path = str(ROOT / "data" / "fixtures" / "reference_chain.json")

            out = io.StringIO()
            with redirect_stdout(out):
                rc = cli_main(["--config", cfg_path, "--db", db_path, "ingest", "--input", fixture_path])
            self.assertEqual(rc, 0)
            ingested = json.loads(out.getvalue())
            self.assertEqual(ingested["snapshots_ingested"], 1)

            snap_id = ingested["snapshots"][0]["snapshot_id"]

            out_q = io.StringIO()
            with redirect_stdout(out_q):
                rc_q = cli_main(["--config", cfg_path, "--db", db_path, "quality", "--snapshot-id", snap_id])
            self.assertEqual(rc_q, 0)
            self.assertEqual(json.loads(out_q.getvalue())["computed_iv_count"], 6)

            out_r = io.StringIO()
            with redirect_stdout(out_r):
                rc_r = cli_main(["--config", cfg_path, "--db", db_path, "replay", "--snapshot-id", snap_id])
            self.assertEqual(rc_r, 0)
            self.assertTrue(json.loads(out_r.getvalue())["is_identical"])

            err = io.StringIO()
            with redirect_stderr(err):
                rc_bad = cli_main(["--config", "/missing/cfg.json", "query"])
            self.assertEqual(rc_bad, 2)


if __name__ == "__main__":
    unittest.main()
