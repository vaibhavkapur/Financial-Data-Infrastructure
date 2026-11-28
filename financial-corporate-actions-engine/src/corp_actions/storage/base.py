"""Abstract protocol defining the durable bi-temporal store contract."""

from __future__ import annotations

from datetime import date, datetime
from typing import Iterable, List, Optional, Protocol, runtime_checkable

from corp_actions.contracts import (
    CorporateActionEvent,
    FxRate,
    PositionSnapshot,
    RawPriceBar,
)


@runtime_checkable
class CorporateActionStore(Protocol):
    """Append-only bi-temporal repository for prices, positions, FX, and corporate actions."""

    def initialize(self) -> None:
        """Create schema/tables idempotently."""
        ...

    def close(self) -> None:
        """Close underlying connections."""
        ...

    def append_prices(self, bars: Iterable[RawPriceBar]) -> int:
        ...

    def append_positions(self, positions: Iterable[PositionSnapshot]) -> int:
        ...

    def append_fx_rates(self, rates: Iterable[FxRate]) -> int:
        ...

    def append_actions(self, actions: Iterable[CorporateActionEvent]) -> int:
        ...

    def get_prices(
        self,
        security_id: str,
        *,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        as_of_time: Optional[datetime] = None,
    ) -> List[RawPriceBar]:
        ...

    def get_initial_position(
        self,
        account_id: str,
        security_id: str,
        *,
        as_of_time: Optional[datetime] = None,
    ) -> Optional[PositionSnapshot]:
        ...

    def get_fx_rates(
        self,
        *,
        as_of_time: Optional[datetime] = None,
    ) -> List[FxRate]:
        ...

    def get_actions(
        self,
        security_id: str,
        *,
        as_of_time: Optional[datetime] = None,
    ) -> List[CorporateActionEvent]:
        ...

    def list_securities(self) -> List[str]:
        ...
