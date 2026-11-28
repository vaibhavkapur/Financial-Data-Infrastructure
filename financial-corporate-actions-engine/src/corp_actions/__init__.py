"""Financial Corporate Actions Engine — bi-temporal, exact-Decimal corporate actions processing."""

from __future__ import annotations

from corp_actions.config import EngineConfig
from corp_actions.contracts import (
    SCHEMA_VERSION,
    ActionType,
    AdjustedPriceBar,
    AdjustmentFactorPoint,
    CorporateActionEvent,
    FxRate,
    PositionLedgerEntry,
    PositionSnapshot,
    QuarantineRecord,
    RawPriceBar,
    RevisionType,
)
from corp_actions.engine import CorporateActionsEngine

__all__ = [
    "SCHEMA_VERSION",
    "ActionType",
    "AdjustedPriceBar",
    "AdjustmentFactorPoint",
    "CorporateActionEvent",
    "CorporateActionsEngine",
    "EngineConfig",
    "FxRate",
    "PositionLedgerEntry",
    "PositionSnapshot",
    "QuarantineRecord",
    "RawPriceBar",
    "RevisionType",
]

__version__ = "1.0.0"
