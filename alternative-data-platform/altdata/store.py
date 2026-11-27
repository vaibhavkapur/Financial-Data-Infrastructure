"""Local ledger, ordered ingestion, monitoring and point-in-time exports."""
import hashlib
import sqlite3
import time
from contextlib import contextmanager

from .connectors import FixtureConnector, TemporaryFailure
from .domain import Problem, authorize, normalized, now, pack, registry, require, token, unpack, utc


SCHEMA = """
CREATE TABLE registries(version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL, document TEXT NOT NULL);
CREATE TABLE progress(source TEXT PRIMARY KEY, cursor INTEGER NOT NULL DEFAULT 0,
    high_water INTEGER NOT NULL DEFAULT 0, last_success TEXT, last_error TEXT);
CREATE TABLE events(id INTEGER PRIMARY KEY, at TEXT NOT NULL, source TEXT NOT NULL,
    sequence INTEGER, code TEXT NOT NULL, detail TEXT NOT NULL);
CREATE TABLE blobs(hash TEXT PRIMARY KEY, payload BLOB NOT NULL);
CREATE TABLE receipts(id INTEGER PRIMARY KEY, source TEXT NOT NULL, sequence INTEGER NOT NULL,
    registry_version INTEGER NOT NULL REFERENCES registries, observed_at TEXT NOT NULL,
    hash TEXT NOT NULL REFERENCES blobs);
CREATE TABLE records(source TEXT NOT NULL, record_key TEXT NOT NULL, revision INTEGER NOT NULL,
    receipt INTEGER NOT NULL REFERENCES receipts, ordinal INTEGER NOT NULL,
    available_at TEXT NOT NULL, document TEXT NOT NULL, PRIMARY KEY(source,record_key,revision));
CREATE TABLE issues(receipt INTEGER NOT NULL REFERENCES receipts, ordinal INTEGER,
    code TEXT NOT NULL, detail TEXT NOT NULL);
CREATE INDEX record_cutoff ON records(source, available_at);
CREATE INDEX event_source ON events(source, id);
"""


class Store:
    def __init__(self, path, clock=now):
        self.clock = clock
        self.db = sqlite3.connect(path, isolation_level=None, timeout=10)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA foreign_keys=ON")
        with self.transaction():
            version = self.db.execute("PRAGMA user_version").fetchone()[0]
            require(version in (0, 1), "unsupported_schema", "unsupported database version")
            if version == 0:
                for statement in SCHEMA.strip().split(";"):
                    if statement.strip():
                        self.db.execute(statement)
                for table in ("blobs", "receipts", "registries", "records", "issues", "events"):
                    for action in ("UPDATE", "DELETE"):
                        self.db.execute(f"CREATE TRIGGER preserve_{table}_{action} BEFORE {action} ON {table} "
                                        "BEGIN SELECT RAISE(ABORT, 'append-only table'); END")
                self.db.execute("PRAGMA user_version=1")

    @contextmanager
    def transaction(self, write=True):
        self.db.execute("BEGIN IMMEDIATE" if write else "BEGIN")
        try:
            yield
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise

    def stamp(self):
        return utc(self.clock())

    def apply(self, doc):
        registry(doc)
        with self.transaction():
            prev = self.db.execute("SELECT * FROM registries ORDER BY version DESC LIMIT 1").fetchone()
            if prev and prev["document"] == pack(doc):
                return doc["version"]
            require(prev is None or doc["version"] > prev["version"], "registry_rollback",
                    "registry changes require a strictly higher version")
            for old in self.db.execute("SELECT document FROM registries"):
                for source, spec in unpack(old[0])["sources"].items():
                    if source in doc["sources"]:
                        require(spec["connector"] == doc["sources"][source]["connector"],
                                "source_redefined", source)
            self.db.execute("INSERT INTO registries VALUES(?,?,?)",
                            (doc["version"], self.stamp(), pack(doc)))
            self.db.executemany("INSERT OR IGNORE INTO progress(source) VALUES(?)",
                                [(src,) for src in doc["sources"]])
        return doc["version"]

    def policy(self, version=None):
        if version is None:
            row = self.db.execute("SELECT document FROM registries ORDER BY version DESC LIMIT 1").fetchone()
        else:
            row = self.db.execute("SELECT document FROM registries WHERE version=?", (version,)).fetchone()
        require(row is not None, "missing_registry", "apply a registry first")
        return unpack(row[0])

    def event(self, source, sequence, code, detail):
        self.db.execute("INSERT INTO events(at,source,sequence,code,detail) VALUES(?,?,?,?,?)",
                        (self.stamp(), source, sequence, code, detail))

    def check(self, source, version):
        current = self.policy()
        authorize(current["sources"].get(source), self.stamp())
        require(current["version"] == version, "registry_changed", "restart with the current registry")

    def call(self, operation, source, sequence, version, sleep):
        for attempt in range(3):
            self.check(source, version)
            try:
                return operation()
            except TemporaryFailure as exc:
                self.event(source, sequence, exc.code, exc.detail)
                if attempt == 2 or exc.retry_after > 30:
                    raise
                sleep(max(exc.retry_after, 0.25 * 2**attempt))

    def ingest(self, source, root, factory=FixtureConnector, sleep=time.sleep):
        token(source)
        sequence = None
        policy = self.policy()
        version = policy["version"]
        spec = policy["sources"].get(source)
        try:
            self.check(source, version)
            adapter = factory(root, source, spec["connector"])
            latest = self.call(adapter.latest, source, None, version, sleep)
            with self.transaction():
                self.check(source, version)
                state = self.db.execute("SELECT * FROM progress WHERE source=?", (source,)).fetchone()
                cursor = state["cursor"]
                require(latest >= state["high_water"], "feed_regressed", "advertised sequence decreased")
                self.db.execute("UPDATE progress SET high_water=? WHERE source=?", (latest, source))
            for sequence in range(cursor + 1, latest + 1):
                raw = self.call(lambda: adapter.fetch(sequence), source, sequence, version, sleep)
                problem = self.accept(source, sequence, version, spec, adapter, raw)
                if problem:
                    raise problem
            with self.transaction():
                self.check(source, version)
                self.db.execute("UPDATE progress SET last_success=?,last_error=NULL WHERE source=?",
                                (self.stamp(), source))
            return self.status(source)
        except Problem as exc:
            with self.transaction():
                self.event(source, sequence, "ingest_failed", exc.code)
                self.db.execute("UPDATE progress SET last_error=? WHERE source=?", (exc.code, source))
            raise

    def accept(self, source, sequence, version, spec, adapter, raw):
        with self.transaction():
            self.check(source, version)
            cursor = self.db.execute("SELECT cursor FROM progress WHERE source=?", (source,)).fetchone()[0]
            if sequence <= cursor:  # Another worker committed this page.
                return None
            require(sequence == cursor + 1, "feed_gap", "unexpected sequence")
            observed = self.stamp()
            digest = hashlib.sha256(raw).hexdigest()
            self.db.execute("INSERT OR IGNORE INTO blobs VALUES(?,?)", (digest, raw))
            receipt = self.db.execute("INSERT INTO receipts(source,sequence,registry_version,observed_at,hash) "
                                      "VALUES(?,?,?,?,?)", (source, sequence, version, observed, digest)).lastrowid
            try:
                rows = adapter.decode(raw)
            except Problem as exc:
                self.issue(receipt, None, exc)
                return exc  # Commit the rejected receipt, leave the cursor in place.
            for ordinal, row in enumerate(rows, start=1):
                try:
                    key, record = normalized(spec["connector"], row, spec)
                    self.save_record(source, key, record, receipt, ordinal, observed)
                except Problem as exc:
                    self.issue(receipt, ordinal, exc)
            self.db.execute("UPDATE progress SET cursor=? WHERE source=?", (sequence, source))
        return None

    def issue(self, receipt, ordinal, problem):
        self.db.execute("INSERT INTO issues VALUES(?,?,?,?)",
                        (receipt, ordinal, problem.code, problem.detail))

    def save_record(self, source, key, record, receipt, ordinal, observed):
        identity = (source, key, record["revision"])
        document = pack(record)
        old = self.db.execute("SELECT document FROM records WHERE source=? AND record_key=? AND revision=?",
                              identity).fetchone()
        if old:
            require(document == old[0], "revision_conflict", key)
            self.event(source, None, "duplicate", key)
            return
        available = max(observed, record["published_at"], record["embargo_until"])
        self.db.execute("INSERT INTO records VALUES(?,?,?,?,?,?,?)",
                        (*identity, receipt, ordinal, available, document))

    def status(self, source):
        with self.transaction(write=False):
            state = self.db.execute("SELECT * FROM progress WHERE source=?", (source,)).fetchone()
            require(state, "unknown_source", source)
            result = dict(state)
            spec = self.policy()["sources"].get(source)
            result["entitled"] = bool(spec and spec["enabled"] and self.stamp() < utc(spec["valid_until"]))
            result["pending_pages"] = state["high_water"] - state["cursor"]
            result["next_sequence"] = state["cursor"] + 1 if result["pending_pages"] else None
            result["events"] = dict(self.db.execute(
                "SELECT code,count(*) FROM events WHERE source=? GROUP BY code", (source,)))
            result["quality"] = dict(self.db.execute(
                "SELECT i.code,count(*) FROM issues i JOIN receipts r ON r.id=i.receipt "
                "WHERE r.source=? GROUP BY i.code", (source,)))
            result["records"] = self.db.execute("SELECT count(*) FROM records WHERE source=?", (source,)).fetchone()[0]
            return result

    def export(self, sources, purpose, as_of=None, history=False):
        token(purpose)
        require(sources and len(set(sources)) == len(sources), "invalid_selection", "select distinct sources")
        with self.transaction(write=False):
            current = self.policy()
            exported = self.stamp()
            cutoff = utc(as_of) if as_of is not None else exported
            require(cutoff <= exported, "future_cutoff", "as-of must be at or before export time")
            licenses = {}
            policies = {current["version"]: current}
            output = []
            for source in sorted(sources):
                spec = current["sources"].get(source)
                authorize(spec, exported, purpose)
                self.license(licenses, source, current, spec)
                rows = self.db.execute("""
                    SELECT d.*, r.sequence, r.observed_at, r.hash, r.registry_version
                    FROM records d JOIN receipts r ON r.id=d.receipt
                    WHERE d.source=? AND d.available_at<=?
                    ORDER BY d.record_key,d.revision DESC
                """, (source, cutoff))
                seen = set()
                for row in rows:
                    key = row["record_key"]
                    if not history and key in seen:
                        continue
                    seen.add(key)
                    version = row["registry_version"]
                    if version not in policies:
                        policies[version] = self.policy(version)
                    origin = policies[version]
                    original = origin["sources"][source]
                    authorize(original, exported, purpose)
                    self.license(licenses, source, origin, original)
                    output.append(dict(unpack(row["document"]), source=source, record_key=key,
                                       available_at=row["available_at"], observed_at=row["observed_at"],
                                       raw_sha256=row["hash"], sequence=row["sequence"], ordinal=row["ordinal"],
                                       registry_version=origin["version"], license_id=original["license_id"]))
            return dict(schema_version=1, as_of=cutoff, exported_at=exported, purpose=purpose,
                        history=history, current_registry_version=current["version"],
                        licenses=list(licenses.values()), records=output)

    @staticmethod
    def license(licenses, source, policy, spec):
        licenses[source, policy["version"]] = dict(source=source, registry_version=policy["version"],
            **{key: spec[key] for key in ("license_id", "terms", "export_purposes", "valid_until", "provenance")})
