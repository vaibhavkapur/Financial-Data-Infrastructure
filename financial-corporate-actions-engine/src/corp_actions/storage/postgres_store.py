"""Optional PostgreSQL adapter implementing CorporateActionStore via psycopg3."""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, List, Optional

from corp_actions.contracts import (
    ActionType,
    CorporateActionEvent,
    FxRate,
    PositionSnapshot,
    RawPriceBar,
    RevisionType,
)
from corp_actions.errors import DependencyUnavailableError, StorageError


def _load_psycopg() -> Any:
    try:
        import psycopg  # type: ignore
        return psycopg
    except ImportError as exc:
        raise DependencyUnavailableError(
            "PostgreSQL backend requires optional dependency 'psycopg[binary]>=3.1.0'. "
            "Install with `pip install -e '.[postgres]'` or use storage_backend='sqlite'.",
            details={"missing_package": "psycopg"},
        ) from exc


class PostgresStore:
    """Bi-temporal PostgreSQL adapter using exact NUMERIC columns and TIMESTAMPTZ.

    Schema DDL is maintained in `sql/postgres_schema.sql`.
    Note: Requires an available PostgreSQL instance and `psycopg` optional extra.
    """

    def __init__(self, dsn: str, schema_sql_path: Optional[Path] = None) -> None:
        self.dsn = dsn
        self._psycopg = _load_psycopg()
        self._schema_sql_path = schema_sql_path or (
            Path(__file__).resolve().parents[3] / "sql" / "postgres_schema.sql"
        )
        try:
            self._conn = self._psycopg.connect(dsn)
        except Exception as exc:
            raise StorageError(f"Failed to connect to PostgreSQL: {exc}") from exc
        self.initialize()

    def initialize(self) -> None:
        if not self._schema_sql_path.exists():
            raise StorageError(f"PostgreSQL schema DDL not found: {self._schema_sql_path}")
        ddl = self._schema_sql_path.read_text(encoding="utf-8")
        try:
            with self._conn.cursor() as cur:
                cur.execute(ddl)
            self._conn.commit()
        except Exception as exc:
            self._conn.rollback()
            raise StorageError(f"Failed to initialize PostgreSQL schema: {exc}") from exc

    def close(self) -> None:
        self._conn.close()

    def append_prices(self, bars: Iterable[RawPriceBar]) -> int:
        bar_list = list(bars)
        if not bar_list:
            return 0
        sql = """
            INSERT INTO raw_prices (
                security_id, symbol, trade_date, open, high, low, close, volume,
                currency, event_time, ingestion_time, source, schema_version
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (security_id, trade_date, ingestion_time, source) DO UPDATE SET
                symbol = EXCLUDED.symbol, open = EXCLUDED.open, high = EXCLUDED.high,
                low = EXCLUDED.low, close = EXCLUDED.close, volume = EXCLUDED.volume
        """
        with self._conn.cursor() as cur:
            cur.executemany(
                sql,
                [
                    (
                        b.security_id, b.symbol, b.trade_date, b.open, b.high, b.low,
                        b.close, b.volume, b.currency, b.event_time, b.ingestion_time,
                        b.source, b.schema_version,
                    )
                    for b in bar_list
                ],
            )
        self._conn.commit()
        return len(bar_list)

    def append_positions(self, positions: Iterable[PositionSnapshot]) -> int:
        pos_list = list(positions)
        if not pos_list:
            return 0
        sql = """
            INSERT INTO position_snapshots (
                account_id, security_id, symbol, as_of_date, quantity,
                cost_basis_per_share, currency, cash_balance,
                event_time, ingestion_time, source, schema_version
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (account_id, security_id, as_of_date, ingestion_time, source) DO NOTHING
        """
        with self._conn.cursor() as cur:
            cur.executemany(
                sql,
                [
                    (
                        p.account_id, p.security_id, p.symbol, p.as_of_date, p.quantity,
                        p.cost_basis_per_share, p.currency, p.cash_balance,
                        p.event_time, p.ingestion_time, p.source, p.schema_version,
                    )
                    for p in pos_list
                ],
            )
        self._conn.commit()
        return len(pos_list)

    def append_fx_rates(self, rates: Iterable[FxRate]) -> int:
        rate_list = list(rates)
        if not rate_list:
            return 0
        sql = """
            INSERT INTO fx_rates (
                base_currency, quote_currency, rate_date, rate,
                event_time, ingestion_time, source, schema_version
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (base_currency, quote_currency, rate_date, ingestion_time, source) DO NOTHING
        """
        with self._conn.cursor() as cur:
            cur.executemany(
                sql,
                [
                    (
                        fx.base_currency, fx.quote_currency, fx.rate_date, fx.rate,
                        fx.event_time, fx.ingestion_time, fx.source, fx.schema_version,
                    )
                    for fx in rate_list
                ],
            )
        self._conn.commit()
        return len(rate_list)

    def append_actions(self, actions: Iterable[CorporateActionEvent]) -> int:
        act_list = list(actions)
        if not act_list:
            return 0
        sql = """
            INSERT INTO corporate_actions (
                action_id, version, revision_type, action_type, security_id, symbol,
                announcement_date, ex_date, record_date, pay_date,
                ratio_numerator, ratio_denominator, cash_amount, currency,
                tax_withholding_rate, new_symbol, dividend_basis,
                event_time, ingestion_time, availability_time,
                source, lineage_id, schema_version, metadata_json
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (action_id, version) DO NOTHING
        """
        with self._conn.cursor() as cur:
            cur.executemany(
                sql,
                [
                    (
                        a.action_id, a.version, a.revision_type.value, a.action_type.value,
                        a.security_id, a.symbol, a.announcement_date, a.ex_date,
                        a.record_date, a.pay_date, a.ratio_numerator, a.ratio_denominator,
                        a.cash_amount, a.currency, a.tax_withholding_rate, a.new_symbol,
                        a.dividend_basis, a.event_time, a.ingestion_time, a.availability_time,
                        a.source, a.lineage_id, a.schema_version, json.dumps(a.metadata, sort_keys=True),
                    )
                    for a in act_list
                ],
            )
        self._conn.commit()
        return len(act_list)

    def get_prices(
        self,
        security_id: str,
        *,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        as_of_time: Optional[datetime] = None,
    ) -> List[RawPriceBar]:
        clauses = ["security_id = %s"]
        params: List[Any] = [security_id]
        if start_date is not None:
            clauses.append("trade_date >= %s")
            params.append(start_date)
        if end_date is not None:
            clauses.append("trade_date <= %s")
            params.append(end_date)
        if as_of_time is not None:
            clauses.append("ingestion_time <= %s")
            params.append(as_of_time)
        sql = f"""
            SELECT DISTINCT ON (trade_date)
                security_id, symbol, trade_date, open, high, low, close, volume,
                currency, event_time, ingestion_time, source, schema_version
            FROM raw_prices
            WHERE {' AND '.join(clauses)}
            ORDER BY trade_date ASC, ingestion_time DESC
        """
        with self._conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        return [
            RawPriceBar(
                security_id=r[0], symbol=r[1], trade_date=r[2],
                open=Decimal(str(r[3])), high=Decimal(str(r[4])),
                low=Decimal(str(r[5])), close=Decimal(str(r[6])),
                volume=Decimal(str(r[7])), currency=r[8],
                event_time=r[9].astimezone(timezone.utc),
                ingestion_time=r[10].astimezone(timezone.utc),
                source=r[11], schema_version=r[12],
            )
            for r in rows
        ]

    def get_initial_position(
        self,
        account_id: str,
        security_id: str,
        *,
        as_of_time: Optional[datetime] = None,
    ) -> Optional[PositionSnapshot]:
        clauses = ["account_id = %s", "security_id = %s"]
        params: List[Any] = [account_id, security_id]
        if as_of_time is not None:
            clauses.append("ingestion_time <= %s")
            params.append(as_of_time)
        sql = f"""
            SELECT account_id, security_id, symbol, as_of_date, quantity,
                   cost_basis_per_share, currency, cash_balance,
                   event_time, ingestion_time, source, schema_version
            FROM position_snapshots
            WHERE {' AND '.join(clauses)}
            ORDER BY as_of_date ASC, ingestion_time DESC
            LIMIT 1
        """
        with self._conn.cursor() as cur:
            cur.execute(sql, params)
            r = cur.fetchone()
        if r is None:
            return None
        return PositionSnapshot(
            account_id=r[0], security_id=r[1], symbol=r[2], as_of_date=r[3],
            quantity=Decimal(str(r[4])), cost_basis_per_share=Decimal(str(r[5])),
            currency=r[6], cash_balance=Decimal(str(r[7])),
            event_time=r[8].astimezone(timezone.utc),
            ingestion_time=r[9].astimezone(timezone.utc),
            source=r[10], schema_version=r[11],
        )

    def get_fx_rates(self, *, as_of_time: Optional[datetime] = None) -> List[FxRate]:
        sql = "SELECT base_currency, quote_currency, rate_date, rate, event_time, ingestion_time, source, schema_version FROM fx_rates"
        params: List[Any] = []
        if as_of_time is not None:
            sql += " WHERE ingestion_time <= %s"
            params.append(as_of_time)
        sql += " ORDER BY rate_date ASC"
        with self._conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        return [
            FxRate(
                base_currency=r[0], quote_currency=r[1], rate_date=r[2],
                rate=Decimal(str(r[3])),
                event_time=r[4].astimezone(timezone.utc),
                ingestion_time=r[5].astimezone(timezone.utc),
                source=r[6], schema_version=r[7],
            )
            for r in rows
        ]

    def get_actions(
        self,
        security_id: str,
        *,
        as_of_time: Optional[datetime] = None,
    ) -> List[CorporateActionEvent]:
        clauses = ["security_id = %s"]
        params: List[Any] = [security_id]
        if as_of_time is not None:
            clauses.append("availability_time <= %s")
            params.append(as_of_time)
        sql = f"""
            SELECT action_id, version, revision_type, action_type, security_id, symbol,
                   announcement_date, ex_date, record_date, pay_date,
                   ratio_numerator, ratio_denominator, cash_amount, currency,
                   tax_withholding_rate, new_symbol, dividend_basis,
                   event_time, ingestion_time, availability_time,
                   source, lineage_id, schema_version, metadata_json
            FROM corporate_actions
            WHERE {' AND '.join(clauses)}
            ORDER BY ex_date ASC, action_id ASC, version ASC
        """
        with self._conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        return [
            CorporateActionEvent(
                action_id=r[0], version=int(r[1]),
                revision_type=RevisionType(r[2]), action_type=ActionType(r[3]),
                security_id=r[4], symbol=r[5], announcement_date=r[6], ex_date=r[7],
                record_date=r[8], pay_date=r[9],
                ratio_numerator=Decimal(str(r[10])) if r[10] is not None else None,
                ratio_denominator=Decimal(str(r[11])) if r[11] is not None else None,
                cash_amount=Decimal(str(r[12])) if r[12] is not None else None,
                currency=r[13], tax_withholding_rate=Decimal(str(r[14])),
                new_symbol=r[15], dividend_basis=r[16],
                event_time=r[17].astimezone(timezone.utc),
                ingestion_time=r[18].astimezone(timezone.utc),
                availability_time=r[19].astimezone(timezone.utc),
                source=r[20], lineage_id=r[21], schema_version=r[22],
                metadata=r[23] if isinstance(r[23], dict) else json.loads(r[23]),
            )
            for r in rows
        ]

    def list_securities(self) -> List[str]:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT security_id FROM raw_prices
                UNION
                SELECT DISTINCT security_id FROM corporate_actions
                ORDER BY security_id ASC
                """
            )
            return [r[0] for r in cur.fetchall()]
