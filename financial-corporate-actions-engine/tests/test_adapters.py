"""Adapter boundary and optional dependency graceful-degradation tests."""

from __future__ import annotations

import unittest
from pathlib import Path

from corp_actions.adapters.polars_adapter import (
    adjusted_series_to_polars,
    polars_available,
)
from corp_actions.errors import DependencyUnavailableError
from corp_actions.storage.base import CorporateActionStore
from corp_actions.storage.postgres_store import PostgresStore
from corp_actions.storage.sqlite_store import SQLiteStore

PROJECT_ROOT = Path(__file__).resolve().parents[1]


class TestAdaptersAndProtocols(unittest.TestCase):
    def test_sqlite_store_conforms_to_protocol(self) -> None:
        store = SQLiteStore(":memory:")
        try:
            self.assertIsInstance(store, CorporateActionStore)
            self.assertEqual(store.list_securities(), [])
        finally:
            store.close()

    def test_postgres_ddl_exists_and_graceful_missing_driver(self) -> None:
        ddl_path = PROJECT_ROOT / "sql" / "postgres_schema.sql"
        self.assertTrue(ddl_path.exists())
        ddl = ddl_path.read_text(encoding="utf-8")
        self.assertIn("CREATE TABLE IF NOT EXISTS corporate_actions", ddl)
        self.assertIn("TIMESTAMPTZ", ddl)

        try:
            import psycopg  # type: ignore  # noqa: F401
            has_psycopg = True
        except ImportError:
            has_psycopg = False

        if not has_psycopg:
            with self.assertRaises(DependencyUnavailableError) as ctx:
                PostgresStore("postgresql://localhost/nonexistent")
            self.assertEqual(ctx.exception.code, "DEPENDENCY_UNAVAILABLE")

    def test_polars_adapter_graceful_degradation(self) -> None:
        if not polars_available():
            with self.assertRaises(DependencyUnavailableError) as ctx:
                adjusted_series_to_polars([])
            self.assertEqual(ctx.exception.code, "DEPENDENCY_UNAVAILABLE")


if __name__ == "__main__":
    unittest.main()
