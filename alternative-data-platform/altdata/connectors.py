"""Finite ordered fixture feeds. A provider adapter implements latest, fetch and decode."""
from pathlib import Path

from .domain import Problem, field, require, unpack

MAX_BYTES = 4 * 1024 * 1024


class TemporaryFailure(Problem):
    def __init__(self, code, detail, retry_after=0):
        super().__init__(code, detail)
        self.retry_after = retry_after


class FixtureConnector:
    def __init__(self, root, source, kind):
        self.directory = Path(root) / source
        self.kind = kind

    def read(self, path):
        try:
            with path.open("rb") as stream:
                data = stream.read(MAX_BYTES + 1)
        except FileNotFoundError as exc:
            raise Problem("feed_gap", path.name) from exc
        except OSError as exc:
            raise TemporaryFailure("source_unavailable", path.name) from exc
        require(len(data) <= MAX_BYTES, "payload_too_large", path.name)
        return data

    def latest(self):
        manifest = unpack(self.read(self.directory / "manifest.json"))
        latest = field(manifest, "latest")
        require(type(field(manifest, "schema_version")) is int and manifest["schema_version"] == 1,
                "unsupported_schema", "feed manifest schema must be 1")
        require(type(latest) is int and 0 <= latest <= 2**31-1, "invalid_manifest", "invalid high water")
        return latest

    def fetch(self, sequence):
        suffix = "json" if self.kind == "macro" else "ndjson"
        return self.read(self.directory / f"{sequence}.{suffix}")

    def decode(self, raw):
        if self.kind == "macro":
            records = field(unpack(raw), "observations")
            require(isinstance(records, list), "invalid_page", "observations must be an array")
            return records
        return [unpack(line) for line in raw.splitlines() if line.strip()]
