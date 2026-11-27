"""Reproduce two feed arrivals and a point-in-time export under a controlled UTC clock."""
import json
import shutil
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from altdata.domain import unpack
from altdata.store import Store


def replay(work):
    fixtures = work / "feeds"
    shutil.copytree(ROOT / "data/fixtures", fixtures)
    first = "2025-02-01T09:45:00Z"
    second = "2025-02-03T10:00:00Z"
    stamp = first
    store = Store(work / "archive.sqlite", clock=lambda: stamp)
    try:
        store.apply(unpack((ROOT / "config/registry.json").read_bytes()))
        for stamp, latest in [(first, 1), (second, 2)]:
            for source in ("macro", "news"):
                (fixtures / source / "manifest.json").write_text(
                    json.dumps({"schema_version": 1, "latest": latest}))
                store.ingest(source, fixtures)
        for source in ("macro", "news"):
            store.ingest(source, fixtures)
        return dict(first_cut=store.export(["macro", "news"], "demo", first),
                    final_history=store.export(["macro", "news"], "demo", second, history=True),
                    health=[store.status(s) for s in ("macro", "news")])
    finally:
        store.db.close()


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as tmp:
        print(json.dumps(replay(Path(tmp)), indent=2))
