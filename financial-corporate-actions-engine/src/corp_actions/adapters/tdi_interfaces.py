"""Independent adapter boundary interfaces for sibling Trading-Data-Infrastructure projects.

Sibling projects in Trading-Data-Infrastructure:
- security-master-platform/
- distributed-tick-data-database/
- distributed-quant-research-platform/

These Protocols define stable, independently runnable adapter contracts without cross-project
runtime imports.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Dict, List, Optional, Protocol, Sequence, runtime_checkable

from corp_actions.contracts import RawPriceBar, SnapshotResult


@runtime_checkable
class SecurityMasterAdapter(Protocol):
    """Boundary contract for resolving immutable security_id <-> point-in-time ticker symbol."""

    def resolve_security_id(self, symbol: str, as_of_date: date) -> Optional[str]:
        ...

    def list_symbol_history(self, security_id: str) -> List[Dict[str, str]]:
        ...


@runtime_checkable
class TickDataPriceAdapter(Protocol):
    """Boundary contract for fetching unadjusted daily OHLCV bars from a tick/bar store."""

    def fetch_raw_bars(
        self,
        security_id: str,
        start_date: date,
        end_date: date,
        as_of_time: datetime,
    ) -> Sequence[RawPriceBar]:
        ...


@runtime_checkable
class QuantResearchSeriesConsumer(Protocol):
    """Boundary contract for publishing PIT adjusted series & factor schedules to research."""

    def publish_snapshot(self, snapshot: SnapshotResult) -> str:
        ...
