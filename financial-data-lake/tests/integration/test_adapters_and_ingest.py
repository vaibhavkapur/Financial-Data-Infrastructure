"""Integration tests for ClickHouse adapter, fixture verification, medallion ingest, and CLI."""

from __future__ import annotations

import json
import shutil
from datetime import UTC
from pathlib import Path

import pytest

from financial_data_lake import (
    FinancialDataLake,
    FixtureVerificationError,
    SchemaValidationError,
)
from financial_data_lake.adapters import ClickHouseTickAdapter, FixtureLoader
from financial_data_lake.cli import main as cli_main


def test_clickhouse_adapter_maps_and_validates(repo_root: Path) -> None:
    adapter = ClickHouseTickAdapter({"NVDA": "INST-NVDA"})
    rows = adapter.adapt_jsonl_file(repo_root / "data" / "fixtures" / "clickhouse_ticks.jsonl")
    assert len(rows) == 3
    assert rows[0]["instrument_id"] == "INST-NVDA"
    assert rows[0]["side"] == "BUY"
    assert rows[0]["event_ts"].tzinfo == UTC
    assert rows[0]["available_at"] >= rows[0]["event_ts"]

    # Unknown symbol fails fast
    bad_adapter = ClickHouseTickAdapter({})
    with pytest.raises(SchemaValidationError, match="Unknown symbol"):
        bad_adapter.adapt_jsonl_file(repo_root / "data" / "fixtures" / "clickhouse_ticks.jsonl")


def test_fixture_loader_detects_tampering(repo_root: Path, tmp_path: Path) -> None:
    fx_copy = tmp_path / "fixtures"
    shutil.copytree(repo_root / "data" / "fixtures", fx_copy)
    loader = FixtureLoader(fx_copy)
    assert len(loader.verify_all()) == 7

    # Tamper with ticks.jsonl
    (fx_copy / "ticks.jsonl").write_text('{"tampered": true}\n', encoding="utf-8")
    with pytest.raises(FixtureVerificationError, match="checksum mismatch"):
        loader.load_verified("ticks.jsonl")


def test_medallion_lineage_and_manifests(
    populated_lake: FinancialDataLake,
) -> None:
    raw_parts = populated_lake.catalog.list_partitions(layer="raw")
    norm_parts = populated_lake.catalog.list_partitions(layer="normalized")
    cur_parts = populated_lake.catalog.list_partitions(layer="curated")
    assert len(raw_parts) >= 7
    assert len(norm_parts) >= 10
    assert len(cur_parts) == len(norm_parts)

    # Every curated partition has provenance, license_id, and checksum
    for p in cur_parts:
        assert p.license_id
        assert p.provenance
        assert len(p.sha256) == 64
        assert populated_lake.storage.verify_object(p.object_path, p.sha256)

    # Lineage edges connect raw -> normalized -> curated
    edges = populated_lake.catalog.list_lineage()
    assert any(e.source_layer == "raw" and e.target_layer == "normalized" for e in edges)
    assert any(e.source_layer == "normalized" and e.target_layer == "curated" for e in edges)


def test_cli_end_to_end(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    lake_root = tmp_path / "cli_lake"
    rc = cli_main(["--lake-root", str(lake_root), "ingest-fixtures"])
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert len(out) == 7

    rc = cli_main(["--lake-root", str(lake_root), "verify"])
    assert rc == 0
    verify_out = json.loads(capsys.readouterr().out)
    assert verify_out["corrupt_partition_ids"] == []

    rc = cli_main(
        [
            "--lake-root",
            str(lake_root),
            "query-as-of",
            "--dataset",
            "fundamentals",
            "--as-of",
            "2024-06-07T23:59:59Z",
        ]
    )
    assert rc == 0
    rows = json.loads(capsys.readouterr().out)
    aapl_rev = next(r for r in rows if r["instrument_id"] == "INST-AAPL" and r["metric_name"] == "revenue")
    assert aapl_rev["metric_value"] == 90753000000.0
