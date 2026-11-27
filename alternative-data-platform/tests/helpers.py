import json
import shutil
import tempfile
import unittest
from pathlib import Path

from altdata.connectors import FixtureConnector
from altdata.domain import unpack
from altdata.store import Store

ROOT = Path(__file__).resolve().parents[1]
FIRST = "2025-02-01T09:45:00Z"
SECOND = "2025-02-03T10:00:00Z"


class PlatformTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.path = Path(tmp.name)
        self.feeds = self.path / "feeds"
        shutil.copytree(ROOT / "data/fixtures", self.feeds)
        self.policy = unpack((ROOT / "config/registry.json").read_bytes())
        self.stamp = FIRST
        self.store = Store(self.path / "db", clock=lambda: self.stamp)
        self.addCleanup(self.store.db.close)
        self.store.apply(self.policy)

    def limit(self, source, latest):
        (self.feeds / source / "manifest.json").write_text(json.dumps(dict(schema_version=1, latest=latest)))

    def ingest(self, source, factory=FixtureConnector):
        return self.store.ingest(source, self.feeds, factory=factory, sleep=lambda _: None)

    def export(self, source, **kwargs):
        return self.store.export([source], "demo", **kwargs)["records"]

    def change(self):
        self.policy["version"] += 1
        self.store.apply(self.policy)
