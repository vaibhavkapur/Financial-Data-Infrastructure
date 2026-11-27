"""Unit tests for SQLite catalog, entitlement registry, and audit logging."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from financial_data_lake import AccessContext, EntitlementError, FinancialDataLake


def test_entitlement_allows_permitted_role_and_logs_audit(
    lake: FinancialDataLake,
    research_ctx: AccessContext,
) -> None:
    policy = lake.catalog.authorize("prices_eod", research_ctx, action="query")
    assert policy.license_id == "SYNTH-INTERNAL-003"
    logs = lake.catalog.get_audit_log("prices_eod")
    assert len(logs) == 1
    assert logs[0]["decision"] == "ALLOW"


def test_entitlement_denies_unregistered_dataset(
    lake: FinancialDataLake,
    research_ctx: AccessContext,
) -> None:
    with pytest.raises(EntitlementError, match="no registered entitlement"):
        lake.catalog.authorize("nonexistent_feed", research_ctx, action="query")
    logs = lake.catalog.get_audit_log("nonexistent_feed")
    assert logs[-1]["decision"] == "DENY"


def test_entitlement_denies_expired_dataset(lake: FinancialDataLake) -> None:
    ctx = AccessContext(
        principal="quant-1",
        role="quant_research",
        purpose="research",
        requested_at=datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC),
    )
    with pytest.raises(EntitlementError, match="not active"):
        lake.catalog.authorize("legacy_expired_feed", ctx, action="query")


def test_entitlement_denies_redistribution(lake: FinancialDataLake) -> None:
    ctx = AccessContext(
        principal="quant-1",
        role="quant_research",
        purpose="redistribution",
        requested_at=datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC),
    )
    with pytest.raises(EntitlementError, match="Redistribution"):
        lake.catalog.authorize("prices_eod", ctx, action="export")


def test_entitlement_denies_unauthorized_role_on_restricted_alt_data(lake: FinancialDataLake) -> None:
    auditor = AccessContext(
        principal="external-auditor",
        role="auditor",
        purpose="research",
        requested_at=datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC),
    )
    with pytest.raises(EntitlementError, match="not entitled"):
        lake.catalog.authorize("alt_sentiment", auditor, action="query")
