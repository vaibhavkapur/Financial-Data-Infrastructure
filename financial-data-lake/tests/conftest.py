"""Shared pytest fixtures for Financial Data Lake tests."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from financial_data_lake import AccessContext, FinancialDataLake

REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture()
def repo_root() -> Path:
    return REPO_ROOT


@pytest.fixture()
def lake(tmp_path: Path) -> FinancialDataLake:
    return FinancialDataLake(
        lake_root=tmp_path / "lake",
        config_path=REPO_ROOT / "config" / "lake_config.json",
        entitlements_path=REPO_ROOT / "config" / "entitlements.json",
        schemas_path=REPO_ROOT / "config" / "schemas" / "registry.json",
    )


@pytest.fixture()
def populated_lake(lake: FinancialDataLake, eng_ctx: AccessContext) -> FinancialDataLake:
    lake.ingest_all_fixtures(REPO_ROOT / "data" / "fixtures", eng_ctx)
    return lake


@pytest.fixture()
def eng_ctx() -> AccessContext:
    return AccessContext(
        principal="test-engineer",
        role="data_engineer",
        purpose="ingest",
        requested_at=datetime(2025, 1, 15, 12, 0, 0, tzinfo=UTC),
    )


@pytest.fixture()
def research_ctx() -> AccessContext:
    return AccessContext(
        principal="test-quant",
        role="quant_research",
        purpose="research",
        requested_at=datetime(2025, 1, 15, 12, 0, 0, tzinfo=UTC),
    )


@pytest.fixture()
def recovery_ctx() -> AccessContext:
    return AccessContext(
        principal="test-sre",
        role="system_recovery",
        purpose="recovery",
        requested_at=datetime(2025, 1, 15, 12, 0, 0, tzinfo=UTC),
    )
