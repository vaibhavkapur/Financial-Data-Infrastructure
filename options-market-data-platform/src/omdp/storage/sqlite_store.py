"""Durable SQLite implementation of StorageRepository with versioned records and replay support."""

from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional, Sequence

from omdp.domain.contracts import (
    ChainSnapshot,
    ContractMetadata,
    DataQualityFlag,
    DerivedOptionFeature,
    ExpiryCalendarEntry,
    FeatureProvenance,
    IVStatus,
    OptionQuote,
    OptionRight,
    QualityReport,
    RateCurvePoint,
    UnderlyingQuote,
    ValidatedOptionQuote,
    ValidationStatus,
    format_utc_timestamp,
    parse_utc_timestamp,
)


class SQLiteOptionStore:
    """ACID SQLite store for contracts, calendars, rates, quotes, snapshots, features, and reports."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = str(db_path)
        if self.db_path != ":memory:":
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA foreign_keys = ON;")
        self._conn.execute("PRAGMA journal_mode = WAL;")

    def initialize(self) -> None:
        with self._conn:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS contract_metadata (
                    option_id TEXT NOT NULL,
                    contract_version INTEGER NOT NULL,
                    occ_symbol TEXT,
                    underlying_symbol TEXT NOT NULL,
                    expiry_date TEXT NOT NULL,
                    option_right TEXT NOT NULL,
                    strike TEXT NOT NULL,
                    strike_scale_decimals INTEGER NOT NULL,
                    contract_multiplier INTEGER NOT NULL,
                    exercise_style TEXT NOT NULL,
                    currency TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    schema_version TEXT NOT NULL,
                    ingestion_time TEXT NOT NULL,
                    record_hash TEXT NOT NULL,
                    PRIMARY KEY (option_id, contract_version)
                );

                CREATE TABLE IF NOT EXISTS expiry_calendars (
                    calendar_id TEXT NOT NULL,
                    calendar_version INTEGER NOT NULL,
                    expiry_date TEXT NOT NULL,
                    expiry_timestamp_utc TEXT NOT NULL,
                    is_trading_day INTEGER NOT NULL,
                    settlement_type TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    schema_version TEXT NOT NULL,
                    record_hash TEXT NOT NULL,
                    PRIMARY KEY (calendar_id, calendar_version, expiry_date)
                );

                CREATE TABLE IF NOT EXISTS rate_curves (
                    curve_id TEXT NOT NULL,
                    curve_version INTEGER NOT NULL,
                    as_of_time TEXT NOT NULL,
                    tenor_days INTEGER NOT NULL,
                    risk_free_rate REAL NOT NULL,
                    dividend_yield REAL NOT NULL,
                    source_id TEXT NOT NULL,
                    schema_version TEXT NOT NULL,
                    record_hash TEXT NOT NULL,
                    PRIMARY KEY (curve_id, curve_version, tenor_days)
                );

                CREATE TABLE IF NOT EXISTS underlying_quotes (
                    underlying_symbol TEXT NOT NULL,
                    quote_version INTEGER NOT NULL,
                    event_time TEXT NOT NULL,
                    ingestion_time TEXT NOT NULL,
                    bid TEXT NOT NULL,
                    ask TEXT NOT NULL,
                    last_price TEXT,
                    source_id TEXT NOT NULL,
                    source_sequence INTEGER NOT NULL,
                    schema_version TEXT NOT NULL,
                    record_hash TEXT NOT NULL,
                    PRIMARY KEY (underlying_symbol, event_time, quote_version)
                );

                CREATE TABLE IF NOT EXISTS chain_snapshots (
                    snapshot_id TEXT PRIMARY KEY,
                    underlying_symbol TEXT NOT NULL,
                    snapshot_time TEXT NOT NULL,
                    ingestion_time TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    schema_version TEXT NOT NULL,
                    config_hash TEXT NOT NULL,
                    snapshot_hash TEXT NOT NULL,
                    quote_count INTEGER NOT NULL,
                    valid_quote_count INTEGER NOT NULL,
                    flagged_quote_count INTEGER NOT NULL,
                    rejected_quote_count INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS option_quotes (
                    snapshot_id TEXT NOT NULL REFERENCES chain_snapshots(snapshot_id),
                    option_id TEXT NOT NULL,
                    quote_version INTEGER NOT NULL,
                    underlying_symbol TEXT NOT NULL,
                    reported_strike TEXT NOT NULL,
                    event_time TEXT NOT NULL,
                    ingestion_time TEXT NOT NULL,
                    bid TEXT NOT NULL,
                    ask TEXT NOT NULL,
                    bid_size INTEGER NOT NULL,
                    ask_size INTEGER NOT NULL,
                    source_id TEXT NOT NULL,
                    source_sequence INTEGER NOT NULL,
                    schema_version TEXT NOT NULL,
                    validation_status TEXT NOT NULL,
                    quality_flags_json TEXT NOT NULL,
                    reasons_json TEXT NOT NULL,
                    ingestion_delay_seconds REAL NOT NULL,
                    staleness_seconds REAL NOT NULL,
                    record_hash TEXT NOT NULL,
                    PRIMARY KEY (snapshot_id, option_id, quote_version)
                );

                CREATE TABLE IF NOT EXISTS derived_option_features (
                    feature_id TEXT PRIMARY KEY,
                    snapshot_id TEXT NOT NULL REFERENCES chain_snapshots(snapshot_id),
                    option_id TEXT NOT NULL,
                    underlying_symbol TEXT NOT NULL,
                    valuation_time TEXT NOT NULL,
                    mid_price REAL,
                    spread REAL,
                    time_to_expiry_years REAL,
                    underlying_mid REAL,
                    risk_free_rate REAL,
                    dividend_yield REAL,
                    forward_price REAL,
                    spot_moneyness REAL,
                    log_forward_moneyness REAL,
                    implied_vol REAL,
                    delta REAL,
                    vega REAL,
                    iv_status TEXT NOT NULL,
                    quality_flags_json TEXT NOT NULL,
                    notes_json TEXT NOT NULL,
                    provenance_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS quality_reports (
                    snapshot_id TEXT PRIMARY KEY REFERENCES chain_snapshots(snapshot_id),
                    underlying_symbol TEXT NOT NULL,
                    snapshot_time TEXT NOT NULL,
                    generated_at TEXT NOT NULL,
                    report_json TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_snapshots_sym_time
                    ON chain_snapshots (underlying_symbol, snapshot_time);
                CREATE INDEX IF NOT EXISTS idx_features_snapshot
                    ON derived_option_features (snapshot_id, option_id);
                CREATE INDEX IF NOT EXISTS idx_underlying_sym_time
                    ON underlying_quotes (underlying_symbol, event_time);
                """
            )

    def close(self) -> None:
        self._conn.close()

    def upsert_contracts(self, contracts: Sequence[ContractMetadata]) -> int:
        with self._conn:
            for c in contracts:
                self._conn.execute(
                    """
                    INSERT OR REPLACE INTO contract_metadata (
                        option_id, contract_version, occ_symbol, underlying_symbol, expiry_date,
                        option_right, strike, strike_scale_decimals, contract_multiplier,
                        exercise_style, currency, source_id, schema_version, ingestion_time, record_hash
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        c.option_id,
                        c.contract_version,
                        c.occ_symbol,
                        c.underlying_symbol,
                        c.expiry_date.isoformat(),
                        c.option_right.value,
                        f"{c.strike:.4f}",
                        c.strike_scale_decimals,
                        c.contract_multiplier,
                        c.exercise_style,
                        c.currency,
                        c.source_id,
                        c.schema_version,
                        format_utc_timestamp(c.ingestion_time),
                        c.record_hash,
                    ),
                )
        return len(contracts)

    def _row_to_contract(self, row: sqlite3.Row) -> ContractMetadata:
        return ContractMetadata(
            option_id=row["option_id"],
            contract_version=int(row["contract_version"]),
            occ_symbol=row["occ_symbol"],
            underlying_symbol=row["underlying_symbol"],
            expiry_date=date.fromisoformat(row["expiry_date"]),
            option_right=OptionRight(row["option_right"]),
            strike=Decimal(row["strike"]),
            strike_scale_decimals=int(row["strike_scale_decimals"]),
            contract_multiplier=int(row["contract_multiplier"]),
            exercise_style=row["exercise_style"],
            currency=row["currency"],
            source_id=row["source_id"],
            schema_version=row["schema_version"],
            ingestion_time=parse_utc_timestamp(row["ingestion_time"]),
            record_hash=row["record_hash"],
        )

    def get_latest_contracts(self, underlying_symbol: Optional[str] = None) -> Dict[str, ContractMetadata]:
        if underlying_symbol:
            rows = self._conn.execute(
                """
                SELECT cm.* FROM contract_metadata cm
                INNER JOIN (
                    SELECT option_id, MAX(contract_version) AS max_ver
                    FROM contract_metadata
                    WHERE underlying_symbol = ?
                    GROUP BY option_id
                ) latest ON cm.option_id = latest.option_id AND cm.contract_version = latest.max_ver
                ORDER BY cm.option_id
                """,
                (underlying_symbol.strip().upper(),),
            ).fetchall()
        else:
            rows = self._conn.execute(
                """
                SELECT cm.* FROM contract_metadata cm
                INNER JOIN (
                    SELECT option_id, MAX(contract_version) AS max_ver
                    FROM contract_metadata
                    GROUP BY option_id
                ) latest ON cm.option_id = latest.option_id AND cm.contract_version = latest.max_ver
                ORDER BY cm.option_id
                """
            ).fetchall()
        return {row["option_id"]: self._row_to_contract(row) for row in rows}

    def get_contract_version(self, option_id: str, contract_version: int) -> Optional[ContractMetadata]:
        row = self._conn.execute(
            "SELECT * FROM contract_metadata WHERE option_id = ? AND contract_version = ?",
            (option_id, contract_version),
        ).fetchone()
        return self._row_to_contract(row) if row else None

    def upsert_calendar_entries(self, entries: Sequence[ExpiryCalendarEntry]) -> int:
        with self._conn:
            for e in entries:
                self._conn.execute(
                    """
                    INSERT OR REPLACE INTO expiry_calendars (
                        calendar_id, calendar_version, expiry_date, expiry_timestamp_utc,
                        is_trading_day, settlement_type, source_id, schema_version, record_hash
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        e.calendar_id,
                        e.calendar_version,
                        e.expiry_date.isoformat(),
                        format_utc_timestamp(e.expiry_timestamp_utc),
                        1 if e.is_trading_day else 0,
                        e.settlement_type,
                        e.source_id,
                        e.schema_version,
                        e.record_hash,
                    ),
                )
        return len(entries)

    def get_latest_calendar(self, calendar_id: Optional[str] = None) -> Dict[str, ExpiryCalendarEntry]:
        if calendar_id:
            rows = self._conn.execute(
                """
                SELECT ec.* FROM expiry_calendars ec
                INNER JOIN (
                    SELECT expiry_date, MAX(calendar_version) AS max_ver
                    FROM expiry_calendars
                    WHERE calendar_id = ?
                    GROUP BY expiry_date
                ) latest ON ec.expiry_date = latest.expiry_date AND ec.calendar_version = latest.max_ver
                WHERE ec.calendar_id = ?
                ORDER BY ec.expiry_date
                """,
                (calendar_id, calendar_id),
            ).fetchall()
        else:
            rows = self._conn.execute(
                """
                SELECT ec.* FROM expiry_calendars ec
                INNER JOIN (
                    SELECT expiry_date, MAX(calendar_version) AS max_ver
                    FROM expiry_calendars
                    GROUP BY expiry_date
                ) latest ON ec.expiry_date = latest.expiry_date AND ec.calendar_version = latest.max_ver
                ORDER BY ec.expiry_date
                """
            ).fetchall()
        result: Dict[str, ExpiryCalendarEntry] = {}
        for row in rows:
            entry = ExpiryCalendarEntry(
                calendar_id=row["calendar_id"],
                calendar_version=int(row["calendar_version"]),
                expiry_date=date.fromisoformat(row["expiry_date"]),
                expiry_timestamp_utc=parse_utc_timestamp(row["expiry_timestamp_utc"]),
                is_trading_day=bool(row["is_trading_day"]),
                settlement_type=row["settlement_type"],
                source_id=row["source_id"],
                schema_version=row["schema_version"],
                record_hash=row["record_hash"],
            )
            result[entry.expiry_date.isoformat()] = entry
        return result

    def upsert_rate_curve_points(self, points: Sequence[RateCurvePoint]) -> int:
        with self._conn:
            for p in points:
                self._conn.execute(
                    """
                    INSERT OR REPLACE INTO rate_curves (
                        curve_id, curve_version, as_of_time, tenor_days,
                        risk_free_rate, dividend_yield, source_id, schema_version, record_hash
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        p.curve_id,
                        p.curve_version,
                        format_utc_timestamp(p.as_of_time),
                        p.tenor_days,
                        p.risk_free_rate,
                        p.dividend_yield,
                        p.source_id,
                        p.schema_version,
                        p.record_hash,
                    ),
                )
        return len(points)

    def get_latest_rate_curve(self, as_of_time: datetime) -> List[RateCurvePoint]:
        as_of_iso = format_utc_timestamp(as_of_time)
        header = self._conn.execute(
            """
            SELECT curve_id, curve_version
            FROM rate_curves
            WHERE as_of_time <= ?
            ORDER BY as_of_time DESC, curve_version DESC
            LIMIT 1
            """,
            (as_of_iso,),
        ).fetchone()
        if header is None:
            return []
        rows = self._conn.execute(
            """
            SELECT * FROM rate_curves
            WHERE curve_id = ? AND curve_version = ?
            ORDER BY tenor_days ASC
            """,
            (header["curve_id"], int(header["curve_version"])),
        ).fetchall()
        return [
            RateCurvePoint(
                curve_id=row["curve_id"],
                curve_version=int(row["curve_version"]),
                as_of_time=parse_utc_timestamp(row["as_of_time"]),
                tenor_days=int(row["tenor_days"]),
                risk_free_rate=float(row["risk_free_rate"]),
                dividend_yield=float(row["dividend_yield"]),
                source_id=row["source_id"],
                schema_version=row["schema_version"],
                record_hash=row["record_hash"],
            )
            for row in rows
        ]

    def upsert_underlying_quotes(self, quotes: Sequence[UnderlyingQuote]) -> int:
        with self._conn:
            for u in quotes:
                self._conn.execute(
                    """
                    INSERT OR REPLACE INTO underlying_quotes (
                        underlying_symbol, quote_version, event_time, ingestion_time,
                        bid, ask, last_price, source_id, source_sequence, schema_version, record_hash
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        u.underlying_symbol,
                        u.quote_version,
                        format_utc_timestamp(u.event_time),
                        format_utc_timestamp(u.ingestion_time),
                        str(u.bid),
                        str(u.ask),
                        str(u.last_price) if u.last_price is not None else None,
                        u.source_id,
                        u.source_sequence,
                        u.schema_version,
                        u.record_hash,
                    ),
                )
        return len(quotes)

    def get_underlying_quotes(
        self, underlying_symbol: str, up_to_time: Optional[datetime] = None
    ) -> List[UnderlyingQuote]:
        sym = underlying_symbol.strip().upper()
        if up_to_time is not None:
            rows = self._conn.execute(
                """
                SELECT * FROM underlying_quotes
                WHERE underlying_symbol = ? AND event_time <= ?
                ORDER BY event_time ASC, quote_version ASC, source_sequence ASC
                """,
                (sym, format_utc_timestamp(up_to_time)),
            ).fetchall()
        else:
            rows = self._conn.execute(
                """
                SELECT * FROM underlying_quotes
                WHERE underlying_symbol = ?
                ORDER BY event_time ASC, quote_version ASC, source_sequence ASC
                """,
                (sym,),
            ).fetchall()
        return [
            UnderlyingQuote(
                underlying_symbol=row["underlying_symbol"],
                quote_version=int(row["quote_version"]),
                event_time=parse_utc_timestamp(row["event_time"]),
                ingestion_time=parse_utc_timestamp(row["ingestion_time"]),
                bid=Decimal(row["bid"]),
                ask=Decimal(row["ask"]),
                last_price=Decimal(row["last_price"]) if row["last_price"] is not None else None,
                source_id=row["source_id"],
                source_sequence=int(row["source_sequence"]),
                schema_version=row["schema_version"],
                record_hash=row["record_hash"],
            )
            for row in rows
        ]

    def save_chain_snapshot(self, snapshot: ChainSnapshot) -> None:
        with self._conn:
            # Remove prior rows if re-ingesting same snapshot_id idempotently
            self._conn.execute("DELETE FROM derived_option_features WHERE snapshot_id = ?", (snapshot.snapshot_id,))
            self._conn.execute("DELETE FROM quality_reports WHERE snapshot_id = ?", (snapshot.snapshot_id,))
            self._conn.execute("DELETE FROM option_quotes WHERE snapshot_id = ?", (snapshot.snapshot_id,))
            self._conn.execute(
                """
                INSERT OR REPLACE INTO chain_snapshots (
                    snapshot_id, underlying_symbol, snapshot_time, ingestion_time,
                    source_id, schema_version, config_hash, snapshot_hash,
                    quote_count, valid_quote_count, flagged_quote_count, rejected_quote_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot.snapshot_id,
                    snapshot.underlying_symbol,
                    format_utc_timestamp(snapshot.snapshot_time),
                    format_utc_timestamp(snapshot.ingestion_time),
                    snapshot.source_id,
                    snapshot.schema_version,
                    snapshot.config_hash,
                    snapshot.snapshot_hash,
                    snapshot.quote_count,
                    snapshot.valid_quote_count,
                    snapshot.flagged_quote_count,
                    snapshot.rejected_quote_count,
                ),
            )
            for vq in snapshot.quotes:
                q = vq.quote
                self._conn.execute(
                    """
                    INSERT INTO option_quotes (
                        snapshot_id, option_id, quote_version, underlying_symbol, reported_strike,
                        event_time, ingestion_time, bid, ask, bid_size, ask_size,
                        source_id, source_sequence, schema_version,
                        validation_status, quality_flags_json, reasons_json,
                        ingestion_delay_seconds, staleness_seconds, record_hash
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        q.snapshot_id,
                        q.option_id,
                        q.quote_version,
                        q.underlying_symbol,
                        str(q.reported_strike),
                        format_utc_timestamp(q.event_time),
                        format_utc_timestamp(q.ingestion_time),
                        str(q.bid),
                        str(q.ask),
                        q.bid_size,
                        q.ask_size,
                        q.source_id,
                        q.source_sequence,
                        q.schema_version,
                        vq.status.value,
                        json.dumps([f.value for f in vq.flags]),
                        json.dumps(list(vq.reasons)),
                        vq.ingestion_delay_seconds,
                        vq.staleness_seconds,
                        q.record_hash,
                    ),
                )

    def _hydrate_snapshot(self, header: sqlite3.Row) -> ChainSnapshot:
        q_rows = self._conn.execute(
            """
            SELECT * FROM option_quotes
            WHERE snapshot_id = ?
            ORDER BY option_id ASC, quote_version ASC
            """,
            (header["snapshot_id"],),
        ).fetchall()
        validated_quotes: List[ValidatedOptionQuote] = []
        for row in q_rows:
            quote = OptionQuote(
                snapshot_id=row["snapshot_id"],
                option_id=row["option_id"],
                quote_version=int(row["quote_version"]),
                underlying_symbol=row["underlying_symbol"],
                reported_strike=Decimal(row["reported_strike"]),
                event_time=parse_utc_timestamp(row["event_time"]),
                ingestion_time=parse_utc_timestamp(row["ingestion_time"]),
                bid=Decimal(row["bid"]),
                ask=Decimal(row["ask"]),
                bid_size=int(row["bid_size"]),
                ask_size=int(row["ask_size"]),
                source_id=row["source_id"],
                source_sequence=int(row["source_sequence"]),
                schema_version=row["schema_version"],
                record_hash=row["record_hash"],
            )
            validated_quotes.append(
                ValidatedOptionQuote(
                    quote=quote,
                    status=ValidationStatus(row["validation_status"]),
                    flags=tuple(DataQualityFlag(f) for f in json.loads(row["quality_flags_json"])),
                    reasons=tuple(json.loads(row["reasons_json"])),
                    ingestion_delay_seconds=float(row["ingestion_delay_seconds"]),
                    staleness_seconds=float(row["staleness_seconds"]),
                )
            )
        return ChainSnapshot(
            snapshot_id=header["snapshot_id"],
            underlying_symbol=header["underlying_symbol"],
            snapshot_time=parse_utc_timestamp(header["snapshot_time"]),
            ingestion_time=parse_utc_timestamp(header["ingestion_time"]),
            source_id=header["source_id"],
            schema_version=header["schema_version"],
            config_hash=header["config_hash"],
            quotes=tuple(validated_quotes),
            snapshot_hash=header["snapshot_hash"],
        )

    def get_chain_snapshot(self, snapshot_id: str) -> Optional[ChainSnapshot]:
        header = self._conn.execute(
            "SELECT * FROM chain_snapshots WHERE snapshot_id = ?",
            (snapshot_id,),
        ).fetchone()
        return self._hydrate_snapshot(header) if header else None

    def list_chain_snapshots(
        self,
        underlying_symbol: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> List[ChainSnapshot]:
        clauses: List[str] = []
        params: List[str] = []
        if underlying_symbol:
            clauses.append("underlying_symbol = ?")
            params.append(underlying_symbol.strip().upper())
        if start_time:
            clauses.append("snapshot_time >= ?")
            params.append(format_utc_timestamp(start_time))
        if end_time:
            clauses.append("snapshot_time <= ?")
            params.append(format_utc_timestamp(end_time))
        where_sql = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        headers = self._conn.execute(
            f"SELECT * FROM chain_snapshots {where_sql} ORDER BY snapshot_time ASC, snapshot_id ASC",
            tuple(params),
        ).fetchall()
        return [self._hydrate_snapshot(h) for h in headers]

    def save_derived_features(self, features: Sequence[DerivedOptionFeature]) -> int:
        with self._conn:
            for f in features:
                self._conn.execute(
                    """
                    INSERT OR REPLACE INTO derived_option_features (
                        feature_id, snapshot_id, option_id, underlying_symbol, valuation_time,
                        mid_price, spread, time_to_expiry_years, underlying_mid,
                        risk_free_rate, dividend_yield, forward_price,
                        spot_moneyness, log_forward_moneyness, implied_vol, delta, vega,
                        iv_status, quality_flags_json, notes_json, provenance_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        f.feature_id,
                        f.snapshot_id,
                        f.option_id,
                        f.underlying_symbol,
                        format_utc_timestamp(f.valuation_time),
                        f.mid_price,
                        f.spread,
                        f.time_to_expiry_years,
                        f.underlying_mid,
                        f.risk_free_rate,
                        f.dividend_yield,
                        f.forward_price,
                        f.spot_moneyness,
                        f.log_forward_moneyness,
                        f.implied_vol,
                        f.delta,
                        f.vega,
                        f.iv_status.value,
                        json.dumps([fl.value for fl in f.quality_flags]),
                        json.dumps(list(f.notes)),
                        json.dumps(f.provenance.to_dict(), sort_keys=True),
                    ),
                )
        return len(features)

    def get_derived_features(self, snapshot_id: str) -> List[DerivedOptionFeature]:
        rows = self._conn.execute(
            """
            SELECT * FROM derived_option_features
            WHERE snapshot_id = ?
            ORDER BY option_id ASC
            """,
            (snapshot_id,),
        ).fetchall()
        result: List[DerivedOptionFeature] = []
        for row in rows:
            p = json.loads(row["provenance_json"])
            prov = FeatureProvenance(
                snapshot_id=p["snapshot_id"],
                snapshot_hash=p["snapshot_hash"],
                option_id=p["option_id"],
                contract_version=p["contract_version"],
                contract_hash=p["contract_hash"],
                option_quote_version=int(p["option_quote_version"]),
                option_quote_hash=p["option_quote_hash"],
                underlying_symbol=p["underlying_symbol"],
                underlying_quote_version=p["underlying_quote_version"],
                underlying_quote_hash=p["underlying_quote_hash"],
                rate_curve_id=p["rate_curve_id"],
                rate_curve_version=p["rate_curve_version"],
                rate_curve_hashes=tuple(p["rate_curve_hashes"]),
                calendar_id=p["calendar_id"],
                calendar_version=p["calendar_version"],
                calendar_hash=p["calendar_hash"],
                config_version=p["config_version"],
                config_hash=p["config_hash"],
                schema_version=p["schema_version"],
                solver_name=p.get("solver_name"),
                solver_iterations=p.get("solver_iterations"),
                price_residual=p.get("price_residual"),
            )
            result.append(
                DerivedOptionFeature(
                    feature_id=row["feature_id"],
                    snapshot_id=row["snapshot_id"],
                    option_id=row["option_id"],
                    underlying_symbol=row["underlying_symbol"],
                    valuation_time=parse_utc_timestamp(row["valuation_time"]),
                    mid_price=row["mid_price"],
                    spread=row["spread"],
                    time_to_expiry_years=row["time_to_expiry_years"],
                    underlying_mid=row["underlying_mid"],
                    risk_free_rate=row["risk_free_rate"],
                    dividend_yield=row["dividend_yield"],
                    forward_price=row["forward_price"],
                    spot_moneyness=row["spot_moneyness"],
                    log_forward_moneyness=row["log_forward_moneyness"],
                    implied_vol=row["implied_vol"],
                    delta=row["delta"],
                    vega=row["vega"],
                    iv_status=IVStatus(row["iv_status"]),
                    quality_flags=tuple(DataQualityFlag(f) for f in json.loads(row["quality_flags_json"])),
                    notes=tuple(json.loads(row["notes_json"])),
                    provenance=prov,
                )
            )
        return result

    def save_quality_report(self, report: QualityReport) -> None:
        with self._conn:
            self._conn.execute(
                """
                INSERT OR REPLACE INTO quality_reports (
                    snapshot_id, underlying_symbol, snapshot_time, generated_at, report_json
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    report.snapshot_id,
                    report.underlying_symbol,
                    format_utc_timestamp(report.snapshot_time),
                    format_utc_timestamp(report.generated_at),
                    json.dumps(report.to_dict(), sort_keys=True),
                ),
            )

    def get_quality_report(self, snapshot_id: str) -> Optional[QualityReport]:
        row = self._conn.execute(
            "SELECT report_json FROM quality_reports WHERE snapshot_id = ?",
            (snapshot_id,),
        ).fetchone()
        if not row:
            return None
        d = json.loads(row["report_json"])
        return QualityReport(
            snapshot_id=d["snapshot_id"],
            underlying_symbol=d["underlying_symbol"],
            snapshot_time=parse_utc_timestamp(d["snapshot_time"]),
            generated_at=parse_utc_timestamp(d["generated_at"]),
            total_quotes=int(d["total_quotes"]),
            valid_quotes=int(d["valid_quotes"]),
            flagged_quotes=int(d["flagged_quotes"]),
            rejected_quotes=int(d["rejected_quotes"]),
            computed_iv_count=int(d["computed_iv_count"]),
            completeness_ratio=float(d["completeness_ratio"]),
            iv_coverage_ratio=float(d["iv_coverage_ratio"]),
            max_quote_staleness_seconds=float(d["max_quote_staleness_seconds"]),
            max_ingestion_delay_seconds=float(d["max_ingestion_delay_seconds"]),
            flag_counts=dict(d["flag_counts"]),
            iv_status_counts=dict(d["iv_status_counts"]),
            snapshot_hash=d["snapshot_hash"],
        )
