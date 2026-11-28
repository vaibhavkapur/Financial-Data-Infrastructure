"""Embedded ACID SQLite implementation of the bi-temporal CorporateActionStore."""

from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Iterable, List, Optional

from corp_actions.contracts import (
    ActionType,
    CorporateActionEvent,
    FxRate,
    PositionSnapshot,
    RawPriceBar,
    RevisionType,
)
from corp_actions.errors import StorageError


def _parse_iso_dt(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _fmt_iso_dt(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


class SQLiteStore:
    """Durable, restart-safe bi-temporal store backed by Python stdlib sqlite3.

    Stores exact Decimal values as canonical TEXT strings to eliminate IEEE-754 float drift.
    All timestamps are normalized to UTC ISO-8601 strings for lexicographically exact
    bi-temporal ordering and comparisons.
    """

    def __init__(self, db_path: str = ":memory:") -> None:
        self.db_path = db_path
        try:
            self._conn = sqlite3.connect(db_path)
            self._conn.row_factory = sqlite3.Row
        except sqlite3.Error as exc:
            raise StorageError(f"Failed to open SQLite database at {db_path}: {exc}") from exc
        self.initialize()

    def initialize(self) -> None:
        try:
            with self._conn:
                self._conn.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS raw_prices (
                        security_id TEXT NOT NULL,
                        symbol TEXT NOT NULL,
                        trade_date TEXT NOT NULL,
                        open TEXT NOT NULL,
                        high TEXT NOT NULL,
                        low TEXT NOT NULL,
                        close TEXT NOT NULL,
                        volume TEXT NOT NULL,
                        currency TEXT NOT NULL,
                        event_time TEXT NOT NULL,
                        ingestion_time TEXT NOT NULL,
                        source TEXT NOT NULL,
                        schema_version TEXT NOT NULL,
                        PRIMARY KEY (security_id, trade_date, ingestion_time, source)
                    );

                    CREATE INDEX IF NOT EXISTS idx_raw_prices_lookup
                        ON raw_prices (security_id, trade_date, ingestion_time);

                    CREATE TABLE IF NOT EXISTS position_snapshots (
                        account_id TEXT NOT NULL,
                        security_id TEXT NOT NULL,
                        symbol TEXT NOT NULL,
                        as_of_date TEXT NOT NULL,
                        quantity TEXT NOT NULL,
                        cost_basis_per_share TEXT NOT NULL,
                        currency TEXT NOT NULL,
                        cash_balance TEXT NOT NULL,
                        event_time TEXT NOT NULL,
                        ingestion_time TEXT NOT NULL,
                        source TEXT NOT NULL,
                        schema_version TEXT NOT NULL,
                        PRIMARY KEY (account_id, security_id, as_of_date, ingestion_time, source)
                    );

                    CREATE TABLE IF NOT EXISTS fx_rates (
                        base_currency TEXT NOT NULL,
                        quote_currency TEXT NOT NULL,
                        rate_date TEXT NOT NULL,
                        rate TEXT NOT NULL,
                        event_time TEXT NOT NULL,
                        ingestion_time TEXT NOT NULL,
                        source TEXT NOT NULL,
                        schema_version TEXT NOT NULL,
                        PRIMARY KEY (base_currency, quote_currency, rate_date, ingestion_time, source)
                    );

                    CREATE TABLE IF NOT EXISTS corporate_actions (
                        action_id TEXT NOT NULL,
                        version INTEGER NOT NULL,
                        revision_type TEXT NOT NULL,
                        action_type TEXT NOT NULL,
                        security_id TEXT NOT NULL,
                        symbol TEXT NOT NULL,
                        announcement_date TEXT NOT NULL,
                        ex_date TEXT NOT NULL,
                        record_date TEXT,
                        pay_date TEXT,
                        ratio_numerator TEXT,
                        ratio_denominator TEXT,
                        cash_amount TEXT,
                        currency TEXT,
                        tax_withholding_rate TEXT NOT NULL,
                        new_symbol TEXT,
                        dividend_basis TEXT,
                        event_time TEXT NOT NULL,
                        ingestion_time TEXT NOT NULL,
                        availability_time TEXT NOT NULL,
                        source TEXT NOT NULL,
                        lineage_id TEXT NOT NULL,
                        schema_version TEXT NOT NULL,
                        metadata_json TEXT NOT NULL,
                        PRIMARY KEY (action_id, version)
                    );

                    CREATE INDEX IF NOT EXISTS idx_corp_actions_pit
                        ON corporate_actions (security_id, availability_time, ex_date);
                    """
                )
        except sqlite3.Error as exc:
            raise StorageError(f"Failed to initialize SQLite schema: {exc}") from exc

    def close(self) -> None:
        self._conn.close()

    def append_prices(self, bars: Iterable[RawPriceBar]) -> int:
        rows = [
            (
                b.security_id,
                b.symbol,
                b.trade_date.isoformat(),
                format(b.open, "f"),
                format(b.high, "f"),
                format(b.low, "f"),
                format(b.close, "f"),
                format(b.volume, "f"),
                b.currency,
                _fmt_iso_dt(b.event_time),
                _fmt_iso_dt(b.ingestion_time),
                b.source,
                b.schema_version,
            )
            for b in bars
        ]
        if not rows:
            return 0
        try:
            with self._conn:
                self._conn.executemany(
                    """
                    INSERT OR REPLACE INTO raw_prices (
                        security_id, symbol, trade_date, open, high, low, close, volume,
                        currency, event_time, ingestion_time, source, schema_version
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    rows,
                )
        except sqlite3.Error as exc:
            raise StorageError(f"Failed to append prices: {exc}") from exc
        return len(rows)

    def append_positions(self, positions: Iterable[PositionSnapshot]) -> int:
        rows = [
            (
                p.account_id,
                p.security_id,
                p.symbol,
                p.as_of_date.isoformat(),
                format(p.quantity, "f"),
                format(p.cost_basis_per_share, "f"),
                p.currency,
                format(p.cash_balance, "f"),
                _fmt_iso_dt(p.event_time),
                _fmt_iso_dt(p.ingestion_time),
                p.source,
                p.schema_version,
            )
            for p in positions
        ]
        if not rows:
            return 0
        try:
            with self._conn:
                self._conn.executemany(
                    """
                    INSERT OR REPLACE INTO position_snapshots (
                        account_id, security_id, symbol, as_of_date, quantity,
                        cost_basis_per_share, currency, cash_balance,
                        event_time, ingestion_time, source, schema_version
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    rows,
                )
        except sqlite3.Error as exc:
            raise StorageError(f"Failed to append positions: {exc}") from exc
        return len(rows)

    def append_fx_rates(self, rates: Iterable[FxRate]) -> int:
        rows = [
            (
                fx.base_currency,
                fx.quote_currency,
                fx.rate_date.isoformat(),
                format(fx.rate, "f"),
                _fmt_iso_dt(fx.event_time),
                _fmt_iso_dt(fx.ingestion_time),
                fx.source,
                fx.schema_version,
            )
            for fx in rates
        ]
        if not rows:
            return 0
        try:
            with self._conn:
                self._conn.executemany(
                    """
                    INSERT OR REPLACE INTO fx_rates (
                        base_currency, quote_currency, rate_date, rate,
                        event_time, ingestion_time, source, schema_version
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    rows,
                )
        except sqlite3.Error as exc:
            raise StorageError(f"Failed to append FX rates: {exc}") from exc
        return len(rows)

    def append_actions(self, actions: Iterable[CorporateActionEvent]) -> int:
        rows = [
            (
                a.action_id,
                a.version,
                a.revision_type.value,
                a.action_type.value,
                a.security_id,
                a.symbol,
                a.announcement_date.isoformat(),
                a.ex_date.isoformat(),
                a.record_date.isoformat() if a.record_date else None,
                a.pay_date.isoformat() if a.pay_date else None,
                format(a.ratio_numerator, "f") if a.ratio_numerator is not None else None,
                format(a.ratio_denominator, "f") if a.ratio_denominator is not None else None,
                format(a.cash_amount, "f") if a.cash_amount is not None else None,
                a.currency,
                format(a.tax_withholding_rate, "f"),
                a.new_symbol,
                a.dividend_basis,
                _fmt_iso_dt(a.event_time),
                _fmt_iso_dt(a.ingestion_time),
                _fmt_iso_dt(a.availability_time),
                a.source,
                a.lineage_id,
                a.schema_version,
                json.dumps(a.metadata, sort_keys=True),
            )
            for a in actions
        ]
        if not rows:
            return 0
        try:
            with self._conn:
                self._conn.executemany(
                    """
                    INSERT OR REPLACE INTO corporate_actions (
                        action_id, version, revision_type, action_type, security_id, symbol,
                        announcement_date, ex_date, record_date, pay_date,
                        ratio_numerator, ratio_denominator, cash_amount, currency,
                        tax_withholding_rate, new_symbol, dividend_basis,
                        event_time, ingestion_time, availability_time,
                        source, lineage_id, schema_version, metadata_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    rows,
                )
        except sqlite3.Error as exc:
            raise StorageError(f"Failed to append corporate actions: {exc}") from exc
        return len(rows)

    def get_prices(
        self,
        security_id: str,
        *,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        as_of_time: Optional[datetime] = None,
    ) -> List[RawPriceBar]:
        clauses = ["security_id = ?"]
        params: List[object] = [security_id]
        if start_date is not None:
            clauses.append("trade_date >= ?")
            params.append(start_date.isoformat())
        if end_date is not None:
            clauses.append("trade_date <= ?")
            params.append(end_date.isoformat())
        if as_of_time is not None:
            clauses.append("ingestion_time <= ?")
            params.append(_fmt_iso_dt(as_of_time))

        sql = f"""
            SELECT * FROM raw_prices
            WHERE {' AND '.join(clauses)}
            ORDER BY trade_date ASC, ingestion_time ASC
        """
        cur = self._conn.execute(sql, params)
        # Keep latest ingested bar per trade_date
        latest_by_date = {}
        for row in cur.fetchall():
            bar = RawPriceBar(
                security_id=row["security_id"],
                symbol=row["symbol"],
                trade_date=date.fromisoformat(row["trade_date"]),
                open=Decimal(row["open"]),
                high=Decimal(row["high"]),
                low=Decimal(row["low"]),
                close=Decimal(row["close"]),
                volume=Decimal(row["volume"]),
                currency=row["currency"],
                event_time=_parse_iso_dt(row["event_time"]),
                ingestion_time=_parse_iso_dt(row["ingestion_time"]),
                source=row["source"],
                schema_version=row["schema_version"],
            )
            latest_by_date[bar.trade_date] = bar
        return [latest_by_date[d] for d in sorted(latest_by_date.keys())]

    def get_initial_position(
        self,
        account_id: str,
        security_id: str,
        *,
        as_of_time: Optional[datetime] = None,
    ) -> Optional[PositionSnapshot]:
        clauses = ["account_id = ?", "security_id = ?"]
        params: List[object] = [account_id, security_id]
        if as_of_time is not None:
            clauses.append("ingestion_time <= ?")
            params.append(_fmt_iso_dt(as_of_time))
        sql = f"""
            SELECT * FROM position_snapshots
            WHERE {' AND '.join(clauses)}
            ORDER BY as_of_date ASC, ingestion_time DESC
            LIMIT 1
        """
        row = self._conn.execute(sql, params).fetchone()
        if row is None:
            return None
        return PositionSnapshot(
            account_id=row["account_id"],
            security_id=row["security_id"],
            symbol=row["symbol"],
            as_of_date=date.fromisoformat(row["as_of_date"]),
            quantity=Decimal(row["quantity"]),
            cost_basis_per_share=Decimal(row["cost_basis_per_share"]),
            currency=row["currency"],
            cash_balance=Decimal(row["cash_balance"]),
            event_time=_parse_iso_dt(row["event_time"]),
            ingestion_time=_parse_iso_dt(row["ingestion_time"]),
            source=row["source"],
            schema_version=row["schema_version"],
        )

    def get_fx_rates(
        self,
        *,
        as_of_time: Optional[datetime] = None,
    ) -> List[FxRate]:
        if as_of_time is not None:
            cur = self._conn.execute(
                "SELECT * FROM fx_rates WHERE ingestion_time <= ? ORDER BY rate_date ASC",
                (_fmt_iso_dt(as_of_time),),
            )
        else:
            cur = self._conn.execute("SELECT * FROM fx_rates ORDER BY rate_date ASC")
        return [
            FxRate(
                base_currency=row["base_currency"],
                quote_currency=row["quote_currency"],
                rate_date=date.fromisoformat(row["rate_date"]),
                rate=Decimal(row["rate"]),
                event_time=_parse_iso_dt(row["event_time"]),
                ingestion_time=_parse_iso_dt(row["ingestion_time"]),
                source=row["source"],
                schema_version=row["schema_version"],
            )
            for row in cur.fetchall()
        ]

    def get_actions(
        self,
        security_id: str,
        *,
        as_of_time: Optional[datetime] = None,
    ) -> List[CorporateActionEvent]:
        clauses = ["security_id = ?"]
        params: List[object] = [security_id]
        if as_of_time is not None:
            clauses.append("availability_time <= ?")
            params.append(_fmt_iso_dt(as_of_time))
        sql = f"""
            SELECT * FROM corporate_actions
            WHERE {' AND '.join(clauses)}
            ORDER BY ex_date ASC, action_id ASC, version ASC
        """
        cur = self._conn.execute(sql, params)
        return [
            CorporateActionEvent(
                action_id=row["action_id"],
                version=int(row["version"]),
                revision_type=RevisionType(row["revision_type"]),
                action_type=ActionType(row["action_type"]),
                security_id=row["security_id"],
                symbol=row["symbol"],
                announcement_date=date.fromisoformat(row["announcement_date"]),
                ex_date=date.fromisoformat(row["ex_date"]),
                record_date=date.fromisoformat(row["record_date"]) if row["record_date"] else None,
                pay_date=date.fromisoformat(row["pay_date"]) if row["pay_date"] else None,
                ratio_numerator=Decimal(row["ratio_numerator"]) if row["ratio_numerator"] is not None else None,
                ratio_denominator=Decimal(row["ratio_denominator"]) if row["ratio_denominator"] is not None else None,
                cash_amount=Decimal(row["cash_amount"]) if row["cash_amount"] is not None else None,
                currency=row["currency"],
                tax_withholding_rate=Decimal(row["tax_withholding_rate"]),
                new_symbol=row["new_symbol"],
                dividend_basis=row["dividend_basis"],
                event_time=_parse_iso_dt(row["event_time"]),
                ingestion_time=_parse_iso_dt(row["ingestion_time"]),
                availability_time=_parse_iso_dt(row["availability_time"]),
                source=row["source"],
                lineage_id=row["lineage_id"],
                schema_version=row["schema_version"],
                metadata=json.loads(row["metadata_json"]),
            )
            for row in cur.fetchall()
        ]

    def list_securities(self) -> List[str]:
        cur = self._conn.execute(
            """
            SELECT DISTINCT security_id FROM raw_prices
            UNION
            SELECT DISTINCT security_id FROM corporate_actions
            ORDER BY security_id ASC
            """
        )
        return [row[0] for row in cur.fetchall()]
