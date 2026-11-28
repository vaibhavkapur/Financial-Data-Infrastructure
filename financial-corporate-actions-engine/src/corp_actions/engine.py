"""Main CorporateActionsEngine orchestrating ingestion, PIT as-of queries, and replay."""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional, Sequence

from corp_actions.config import EngineConfig
from corp_actions.contracts import (
    CorporateActionEvent,
    FxRate,
    PositionSnapshot,
    RawPriceBar,
    ReplayDiff,
    SnapshotResult,
    serialize_value,
)
from corp_actions.domain.factors import compute_factor_schedule, resolve_pit_actions
from corp_actions.domain.positions import apply_actions_to_position
from corp_actions.domain.reconcile import reconcile_snapshot
from corp_actions.domain.series import build_adjusted_series
from corp_actions.errors import ConfigurationError
from corp_actions.logging_utils import DiagnosticCounters, get_structured_logger
from corp_actions.storage.base import CorporateActionStore
from corp_actions.storage.sqlite_store import SQLiteStore
from corp_actions.validation import (
    validate_corporate_action,
    validate_fx_rate,
    validate_position_snapshot,
    validate_price_bar,
)


def _compute_digest(payload: Dict[str, Any]) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


class CorporateActionsEngine:
    """Bi-temporal corporate actions engine with strict no-hindsight PIT queries and replay."""

    def __init__(
        self,
        config: Optional[EngineConfig] = None,
        store: Optional[CorporateActionStore] = None,
    ) -> None:
        self.config = config or EngineConfig()
        if store is not None:
            self.store: CorporateActionStore = store
        elif self.config.storage_backend == "sqlite":
            self.store = SQLiteStore(self.config.sqlite_path)
        else:
            raise ConfigurationError(
                f"Backend '{self.config.storage_backend}' requires passing an initialized store instance"
            )
        self.counters = DiagnosticCounters()
        self.logger = get_structured_logger("corp_actions.engine")

    def close(self) -> None:
        self.store.close()

    def ingest_prices(self, bars: Iterable[RawPriceBar]) -> int:
        validated: List[RawPriceBar] = []
        for bar in bars:
            try:
                validate_price_bar(bar)
            except Exception:
                self.counters.validation_errors += 1
                raise
            validated.append(bar)
        count = self.store.append_prices(validated)
        self.counters.prices_ingested += count
        self.counters.record_event("ingest_prices", count=count)
        return count

    def ingest_positions(self, positions: Iterable[PositionSnapshot]) -> int:
        validated: List[PositionSnapshot] = []
        for pos in positions:
            try:
                validate_position_snapshot(pos)
            except Exception:
                self.counters.validation_errors += 1
                raise
            validated.append(pos)
        count = self.store.append_positions(validated)
        self.counters.positions_ingested += count
        self.counters.record_event("ingest_positions", count=count)
        return count

    def ingest_fx_rates(self, rates: Iterable[FxRate]) -> int:
        validated: List[FxRate] = []
        for fx in rates:
            try:
                validate_fx_rate(fx)
            except Exception:
                self.counters.validation_errors += 1
                raise
            validated.append(fx)
        count = self.store.append_fx_rates(validated)
        self.counters.fx_rates_ingested += count
        self.counters.record_event("ingest_fx_rates", count=count)
        return count

    def ingest_actions(self, actions: Iterable[CorporateActionEvent]) -> int:
        validated: List[CorporateActionEvent] = []
        allow_unsupported = not self.config.strict_unsupported_actions
        for act in actions:
            try:
                validate_corporate_action(act, allow_unsupported=allow_unsupported)
            except Exception:
                self.counters.validation_errors += 1
                raise
            validated.append(act)
        count = self.store.append_actions(validated)
        self.counters.actions_ingested += count
        self.counters.record_event("ingest_actions", count=count)
        return count

    def query_as_of(
        self,
        security_id: str,
        *,
        as_of_time: datetime,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        account_id: Optional[str] = None,
    ) -> SnapshotResult:
        """Execute a strictly point-in-time (no-hindsight) query for a security.

        Only raw prices, positions, FX rates, and corporate action revisions known
        at or before `as_of_time` are read. Furthermore, only actions whose `ex_date`
        is on or before the effective snapshot date (`min(end_date, as_of_time.date())`)
        are applied to the historical adjustment schedule.
        """
        as_of_utc = _ensure_utc(as_of_time)
        effective_end = min(end_date, as_of_utc.date()) if end_date is not None else as_of_utc.date()

        raw_bars = self.store.get_prices(
            security_id,
            start_date=start_date,
            end_date=effective_end,
            as_of_time=as_of_utc,
        )
        all_known_actions = self.store.get_actions(security_id, as_of_time=as_of_utc)
        fx_rates = self.store.get_fx_rates(as_of_time=as_of_utc)

        resolved = resolve_pit_actions(
            all_known_actions,
            as_of_time=as_of_utc,
            effective_cutoff_date=effective_end,
            strict_unsupported=self.config.strict_unsupported_actions,
        )

        self.counters.queries_executed += 1
        self.counters.actions_applied += len(resolved.effective)
        self.counters.actions_cancelled += len(resolved.cancelled)
        self.counters.actions_quarantined += len(resolved.quarantined)

        if resolved.quarantined:
            for q in resolved.quarantined:
                self.logger.warning(
                    "Quarantined unsupported corporate action",
                    extra={"structured": q.to_dict()},
                )

        factor_schedule = compute_factor_schedule(
            raw_bars,
            resolved.effective,
            fx_rates,
            self.config,
            as_of_time=as_of_utc,
        )
        adjusted_series = build_adjusted_series(raw_bars, factor_schedule, self.config)

        position_ledger = ()
        if account_id is not None:
            initial_pos = self.store.get_initial_position(
                account_id, security_id, as_of_time=as_of_utc
            )
            if initial_pos is not None:
                position_ledger = apply_actions_to_position(
                    initial_pos,
                    raw_bars,
                    resolved.effective,
                    fx_rates,
                    self.config,
                    as_of_time=as_of_utc,
                )

        reconciliation = reconcile_snapshot(
            security_id=security_id,
            as_of_time=as_of_utc,
            raw_bars=raw_bars,
            adjusted_bars=adjusted_series,
            position_ledger=position_ledger,
            effective_actions=resolved.effective,
            quarantined_actions=resolved.quarantined,
            config=self.config,
        )

        diagnostics: Dict[str, Any] = {
            "effective_cutoff_date": effective_end.isoformat(),
            "pending_future_ex_actions": [
                f"{a.action_id}@v{a.version}(ex={a.ex_date.isoformat()})"
                for a in resolved.pending_future_ex
            ],
            "cancelled_actions": [
                f"{a.action_id}@v{a.version}" for a in resolved.cancelled
            ],
            "quarantined_count": len(resolved.quarantined),
            "counters": self.counters.snapshot(),
        }

        # Stable deterministic digest over logical outputs (excluding volatile runtime counters)
        digest_payload = {
            "security_id": security_id,
            "as_of_time": serialize_value(as_of_utc),
            "config_version": self.config.config_version,
            "schema_version": self.config.schema_version,
            "adjusted_series": [a.to_dict() for a in adjusted_series],
            "position_ledger": [p.to_dict() for p in position_ledger],
            "effective_actions": [f"{a.action_id}@v{a.version}" for a in resolved.effective],
            "quarantined_actions": [q.to_dict() for q in resolved.quarantined],
        }
        output_digest = _compute_digest(digest_payload)
        run_id = f"pit-{security_id.lower()}-{output_digest[:12]}"

        self.counters.record_event(
            "query_as_of",
            run_id=run_id,
            security_id=security_id,
            as_of_time=serialize_value(as_of_utc),
            effective_actions=len(resolved.effective),
            quarantined=len(resolved.quarantined),
            digest=output_digest[:12],
        )

        return SnapshotResult(
            run_id=run_id,
            security_id=security_id,
            as_of_time=as_of_utc,
            config_version=self.config.config_version,
            schema_version=self.config.schema_version,
            raw_series=tuple(raw_bars),
            adjusted_series=adjusted_series,
            factor_schedule=factor_schedule,
            position_ledger=position_ledger,
            effective_actions=resolved.effective,
            quarantined_actions=resolved.quarantined,
            reconciliation=reconciliation,
            output_digest=output_digest,
            diagnostics=diagnostics,
        )

    def replay_correction(
        self,
        security_id: str,
        *,
        from_as_of: datetime,
        to_as_of: datetime,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        account_id: Optional[str] = None,
    ) -> ReplayDiff:
        """Deterministically compare two PIT snapshots to audit late corrections or cancellations."""
        snap_from = self.query_as_of(
            security_id,
            as_of_time=from_as_of,
            start_date=start_date,
            end_date=end_date,
            account_id=account_id,
        )
        snap_to = self.query_as_of(
            security_id,
            as_of_time=to_as_of,
            start_date=start_date,
            end_date=end_date,
            account_id=account_id,
        )
        self.counters.replays_executed += 1

        from_versions = {a.action_id: a.version for a in snap_from.effective_actions}
        to_versions = {a.action_id: a.version for a in snap_to.effective_actions}

        added = sorted(
            f"{aid}@v{ver}" for aid, ver in to_versions.items() if aid not in from_versions
        )
        corrected = sorted(
            f"{aid}@v{from_versions[aid]}->v{ver}"
            for aid, ver in to_versions.items()
            if aid in from_versions and ver != from_versions[aid]
        )
        cancelled = sorted(
            f"{aid}@v{ver}" for aid, ver in from_versions.items() if aid not in to_versions
        )
        from_q = {q.action_id for q in snap_from.quarantined_actions}
        new_q = sorted(
            f"{q.action_id}({q.action_type})"
            for q in snap_to.quarantined_actions
            if q.action_id not in from_q
        )

        from_adj = {b.trade_date: b for b in snap_from.adjusted_series}
        to_adj = {b.trade_date: b for b in snap_to.adjusted_series}
        common_dates = sorted(set(from_adj.keys()) & set(to_adj.keys()))

        changed_dates: List[date] = []
        price_deltas: Dict[str, Dict[str, str]] = {}
        for d in common_dates:
            b1 = from_adj[d]
            b2 = to_adj[d]
            tr_diff = b2.tr_adj_close - b1.tr_adj_close
            sp_diff = b2.split_adj_close - b1.split_adj_close
            if tr_diff != Decimal("0") or sp_diff != Decimal("0") or b1.active_symbol != b2.active_symbol:
                changed_dates.append(d)
                price_deltas[d.isoformat()] = {
                    "from_tr_adj_close": format(b1.tr_adj_close, "f"),
                    "to_tr_adj_close": format(b2.tr_adj_close, "f"),
                    "tr_adj_close_delta": format(tr_diff, "f"),
                    "split_adj_close_delta": format(sp_diff, "f"),
                }

        cash_delta_end: Optional[Decimal] = None
        qty_delta_end: Optional[Decimal] = None
        if snap_from.position_ledger and snap_to.position_ledger:
            end_from = snap_from.position_ledger[-1]
            end_to = snap_to.position_ledger[-1]
            # Compare total cash (settled + pending receivable) at end of each window
            cash_from = end_from.settled_cash_balance + end_from.pending_receivable_cash
            cash_to = end_to.settled_cash_balance + end_to.pending_receivable_cash
            cash_delta_end = (cash_to - cash_from).quantize(self.config.cash_quantize)
            qty_delta_end = (end_to.quantity - end_from.quantity).quantize(self.config.quantity_quantize)

        return ReplayDiff(
            security_id=security_id,
            from_as_of=_ensure_utc(from_as_of),
            to_as_of=_ensure_utc(to_as_of),
            added_actions=tuple(added),
            corrected_actions=tuple(corrected),
            cancelled_actions=tuple(cancelled),
            quarantined_actions=tuple(new_q),
            changed_dates=tuple(changed_dates),
            price_deltas=price_deltas,
            cash_delta_end=cash_delta_end,
            quantity_delta_end=qty_delta_end,
            from_digest=snap_from.output_digest,
            to_digest=snap_to.output_digest,
        )
