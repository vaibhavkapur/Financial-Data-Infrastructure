"""Shared strictly-causal financial feature transforms for offline & online parity."""

from __future__ import annotations

import math
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import polars as pl

from financial_feature_store.contracts import (
    FeatureDefinition,
    NullPolicy,
    ValueType,
    ensure_utc,
)
from financial_feature_store.errors import (
    LeakageViolationError,
    NullPolicyViolationError,
    TimestampContractError,
)

# A TransformKernel takes a single-entity, strictly time-sorted list of row dicts
# and transform params, returning a list of causal feature values of identical length.
TransformKernel = Callable[[list[dict[str, Any]], dict[str, Any], float], list[Any]]


def _is_missing(val: Any) -> bool:
    if val is None:
        return True
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
        return True
    return False


# --- Order-book microstructure transforms (clean contract inspired by LOB predictor) ---


def _lob_mid_price(rows: list[dict[str, Any]], params: dict[str, Any], _lookback: float) -> list[float | None]:
    bid_col = params.get("bid_col", "best_bid")
    ask_col = params.get("ask_col", "best_ask")
    out: list[float | None] = []
    for r in rows:
        bid, ask = r.get(bid_col), r.get(ask_col)
        if _is_missing(bid) or _is_missing(ask) or bid <= 0 or ask <= 0 or ask < bid:
            out.append(None)
        else:
            out.append(0.5 * (float(bid) + float(ask)))
    return out


def _lob_spread_bps(rows: list[dict[str, Any]], params: dict[str, Any], _lookback: float) -> list[float | None]:
    bid_col = params.get("bid_col", "best_bid")
    ask_col = params.get("ask_col", "best_ask")
    out: list[float | None] = []
    for r in rows:
        bid, ask = r.get(bid_col), r.get(ask_col)
        if _is_missing(bid) or _is_missing(ask) or bid <= 0 or ask <= 0 or ask < bid:
            out.append(None)
        else:
            mid = 0.5 * (float(bid) + float(ask))
            out.append(10_000.0 * (float(ask) - float(bid)) / mid)
    return out


def _lob_order_book_imbalance(rows: list[dict[str, Any]], params: dict[str, Any], _lookback: float) -> list[float | None]:
    bid_sz_col = params.get("bid_size_col", "bid_size")
    ask_sz_col = params.get("ask_size_col", "ask_size")
    out: list[float | None] = []
    for r in rows:
        bsz, asz = r.get(bid_sz_col), r.get(ask_sz_col)
        if _is_missing(bsz) or _is_missing(asz):
            out.append(None)
            continue
        denom = float(bsz) + float(asz)
        if denom <= 0.0:
            out.append(None)
        else:
            out.append((float(bsz) - float(asz)) / denom)
    return out


def _lob_microprice(rows: list[dict[str, Any]], params: dict[str, Any], _lookback: float) -> list[float | None]:
    bid_col = params.get("bid_col", "best_bid")
    ask_col = params.get("ask_col", "best_ask")
    bid_sz_col = params.get("bid_size_col", "bid_size")
    ask_sz_col = params.get("ask_size_col", "ask_size")
    out: list[float | None] = []
    for r in rows:
        bid, ask, bsz, asz = r.get(bid_col), r.get(ask_col), r.get(bid_sz_col), r.get(ask_sz_col)
        if _is_missing(bid) or _is_missing(ask) or _is_missing(bsz) or _is_missing(asz):
            out.append(None)
            continue
        denom = float(bsz) + float(asz)
        if denom <= 0.0 or float(ask) < float(bid):
            out.append(None)
        else:
            out.append((float(bid) * float(asz) + float(ask) * float(bsz)) / denom)
    return out


def _lob_rolling_imbalance_mean(rows: list[dict[str, Any]], params: dict[str, Any], lookback_seconds: float) -> list[float | None]:
    """Strictly causal rolling mean of order book imbalance over trailing window."""
    min_periods = int(params.get("min_periods", 2))
    window_events = params.get("window_events")
    imbalances = _lob_order_book_imbalance(rows, params, lookback_seconds)
    out: list[float | None] = []

    for idx, row in enumerate(rows):
        t_cur: datetime = row["event_time"]
        avail_cur: datetime = row["availability_time"]
        vals: list[float] = []
        # Walk strictly backward (j <= idx) and include ONLY events already available at avail_cur
        for j in range(idx, -1, -1):
            if window_events is not None and (idx - j) >= int(window_events):
                break
            dt_sec = (t_cur - rows[j]["event_time"]).total_seconds()
            if lookback_seconds > 0.0 and dt_sec > lookback_seconds:
                break
            # Causal barrier: never incorporate another row whose availability_time > avail_cur
            if rows[j]["availability_time"] > avail_cur:
                continue
            v = imbalances[j]
            if v is not None:
                vals.append(v)
        if len(vals) < min_periods:
            out.append(None)
        else:
            out.append(sum(vals) / len(vals))
    return out


# --- Market bar / time-series transforms ---


def _bar_log_return(rows: list[dict[str, Any]], params: dict[str, Any], _lookback: float) -> list[float | None]:
    price_col = params.get("price_col", "close")
    lag = int(params.get("lag", 1))
    if lag < 1:
        raise LeakageViolationError(
            "bar_log_return lag must be >= 1 (negative or zero lag would cause lookahead)",
            details={"lag": lag},
        )
    out: list[float | None] = []
    for idx, row in enumerate(rows):
        cur_p = row.get(price_col)
        avail_cur: datetime = row["availability_time"]
        if _is_missing(cur_p) or float(cur_p) <= 0.0:
            out.append(None)
            continue
        # Find the lag-th strictly prior row that was ALREADY available by avail_cur
        eligible_prior: list[float] = []
        for j in range(idx - 1, -1, -1):
            if rows[j]["availability_time"] <= avail_cur:
                p_prev = rows[j].get(price_col)
                if not _is_missing(p_prev) and float(p_prev) > 0.0:
                    eligible_prior.append(float(p_prev))
                    if len(eligible_prior) == lag:
                        break
        if len(eligible_prior) < lag:
            out.append(None)
        else:
            out.append(math.log(float(cur_p) / eligible_prior[-1]))
    return out


def _bar_rolling_volatility(rows: list[dict[str, Any]], params: dict[str, Any], lookback_seconds: float) -> list[float | None]:
    window_events = int(params.get("window_events", 5))
    min_periods = int(params.get("min_periods", 3))
    returns = _bar_log_return(rows, {"price_col": params.get("price_col", "close"), "lag": 1}, lookback_seconds)
    out: list[float | None] = []
    for idx, row in enumerate(rows):
        t_cur: datetime = row["event_time"]
        avail_cur: datetime = row["availability_time"]
        vals: list[float] = []
        for j in range(idx, -1, -1):
            if (idx - j) >= window_events:
                break
            if lookback_seconds > 0.0 and (t_cur - rows[j]["event_time"]).total_seconds() > lookback_seconds:
                break
            if rows[j]["availability_time"] > avail_cur:
                continue
            r = returns[j]
            if r is not None:
                vals.append(r)
        if len(vals) < min_periods:
            out.append(None)
        else:
            mean_v = sum(vals) / len(vals)
            var = sum((x - mean_v) ** 2 for x in vals) / (len(vals) - 1)
            out.append(math.sqrt(var))
    return out


def _bar_vwap_deviation_bps(rows: list[dict[str, Any]], params: dict[str, Any], _lookback: float) -> list[float | None]:
    close_col = params.get("close_col", "close")
    vwap_col = params.get("vwap_col", "vwap")
    out: list[float | None] = []
    for r in rows:
        close, vwap = r.get(close_col), r.get(vwap_col)
        if _is_missing(close) or _is_missing(vwap) or float(vwap) <= 0.0:
            out.append(None)
        else:
            out.append(10_000.0 * (float(close) - float(vwap)) / float(vwap))
    return out


# --- Entity / risk / fraud velocity transforms (clean contract inspired by Fraud Detector) ---


def _tx_rolling_count(rows: list[dict[str, Any]], _params: dict[str, Any], lookback_seconds: float) -> list[int]:
    out: list[int] = []
    for idx, row in enumerate(rows):
        t_cur: datetime = row["event_time"]
        avail_cur: datetime = row["availability_time"]
        cnt = 0
        for j in range(idx, -1, -1):
            if lookback_seconds > 0.0 and (t_cur - rows[j]["event_time"]).total_seconds() > lookback_seconds:
                break
            if rows[j]["availability_time"] <= avail_cur:
                cnt += 1
        out.append(cnt)
    return out


def _tx_rolling_amount_sum(rows: list[dict[str, Any]], params: dict[str, Any], lookback_seconds: float) -> list[float | None]:
    amount_col = params.get("amount_col", "amount")
    out: list[float | None] = []
    for idx, row in enumerate(rows):
        t_cur: datetime = row["event_time"]
        avail_cur: datetime = row["availability_time"]
        total = 0.0
        seen = 0
        for j in range(idx, -1, -1):
            if lookback_seconds > 0.0 and (t_cur - rows[j]["event_time"]).total_seconds() > lookback_seconds:
                break
            if rows[j]["availability_time"] > avail_cur:
                continue
            amt = rows[j].get(amount_col)
            if not _is_missing(amt):
                total += float(amt)
                seen += 1
        out.append(total if seen > 0 else None)
    return out


def _tx_amount_zscore(rows: list[dict[str, Any]], params: dict[str, Any], lookback_seconds: float) -> list[float | None]:
    """Causal z-score of current amount relative to strictly prior trailing window."""
    amount_col = params.get("amount_col", "amount")
    min_periods = int(params.get("min_periods", 3))
    out: list[float | None] = []
    for idx, row in enumerate(rows):
        cur_amt = row.get(amount_col)
        if _is_missing(cur_amt):
            out.append(None)
            continue
        t_cur: datetime = row["event_time"]
        avail_cur: datetime = row["availability_time"]
        prior_vals: list[float] = []
        for j in range(idx - 1, -1, -1):
            if lookback_seconds > 0.0 and (t_cur - rows[j]["event_time"]).total_seconds() > lookback_seconds:
                break
            if rows[j]["availability_time"] > avail_cur:
                continue
            v = rows[j].get(amount_col)
            if not _is_missing(v):
                prior_vals.append(float(v))
        if len(prior_vals) < min_periods:
            out.append(None)
            continue
        mean_v = sum(prior_vals) / len(prior_vals)
        var = sum((x - mean_v) ** 2 for x in prior_vals) / (len(prior_vals) - 1)
        std = math.sqrt(var)
        if std == 0.0:
            out.append(0.0)
        else:
            out.append((float(cur_amt) - mean_v) / std)
    return out


TRANSFORM_REGISTRY: dict[str, TransformKernel] = {
    "lob_mid_price": _lob_mid_price,
    "lob_spread_bps": _lob_spread_bps,
    "lob_order_book_imbalance": _lob_order_book_imbalance,
    "lob_microprice": _lob_microprice,
    "lob_rolling_imbalance_mean": _lob_rolling_imbalance_mean,
    "bar_log_return": _bar_log_return,
    "bar_rolling_volatility": _bar_rolling_volatility,
    "bar_vwap_deviation_bps": _bar_vwap_deviation_bps,
    "tx_rolling_count": _tx_rolling_count,
    "tx_rolling_amount_sum": _tx_rolling_amount_sum,
    "tx_amount_zscore": _tx_amount_zscore,
}


def register_causal_transform(name: str, kernel: TransformKernel) -> None:
    """Register an additional custom causal transform kernel."""
    TRANSFORM_REGISTRY[name] = kernel


def _polars_dtype(value_type: ValueType) -> pl.DataType:
    if value_type == ValueType.FLOAT64:
        return pl.Float64()
    if value_type == ValueType.INT64:
        return pl.Int64()
    if value_type == ValueType.BOOL:
        return pl.Boolean()
    return pl.String()


def _cast_value(val: Any, value_type: ValueType) -> Any:
    if val is None:
        return None
    if value_type == ValueType.FLOAT64:
        return float(val)
    if value_type == ValueType.INT64:
        return int(val)
    if value_type == ValueType.BOOL:
        return bool(val)
    return str(val)


def validate_raw_frame(raw_df: pl.DataFrame, required_columns: tuple[str, ...]) -> None:
    """Validate mandatory bi-temporal columns, UTC awareness, and causality on raw inputs."""
    mandatory = ("entity_id", "event_time", "availability_time") + required_columns
    missing = [c for c in mandatory if c not in raw_df.columns]
    if missing:
        raise TimestampContractError(
            f"Raw dataframe missing required columns: {missing}",
            details={"missing_columns": missing, "present_columns": raw_df.columns},
        )

    for ts_col in ("event_time", "availability_time"):
        dtype = raw_df.schema[ts_col]
        if not isinstance(dtype, pl.Datetime) or dtype.time_zone is None:
            raise TimestampContractError(
                f"Column '{ts_col}' must be timezone-aware UTC Datetime",
                details={"column": ts_col, "dtype": str(dtype)},
            )
        if raw_df[ts_col].null_count() > 0:
            raise TimestampContractError(
                f"Column '{ts_col}' contains null timestamps",
                details={"column": ts_col, "null_count": raw_df[ts_col].null_count()},
            )

    invalid_causality = raw_df.filter(pl.col("availability_time") < pl.col("event_time"))
    if invalid_causality.height > 0:
        sample = invalid_causality.head(1).to_dicts()[0]
        raise LeakageViolationError(
            "Detected raw records where availability_time precedes event_time",
            details={
                "invalid_row_count": invalid_causality.height,
                "sample_entity_id": sample["entity_id"],
                "sample_event_time": str(sample["event_time"]),
                "sample_availability_time": str(sample["availability_time"]),
            },
        )


def compute_causal_feature_frame(
    raw_df: pl.DataFrame,
    feature_def: FeatureDefinition,
    *,
    run_id: str,
    materialized_at: datetime | None = None,
) -> pl.DataFrame:
    """Compute a strictly causal materialized feature frame shared by offline and online paths."""
    validate_raw_frame(raw_df, feature_def.transform.source_columns)

    t_name = feature_def.transform.transform_name
    if t_name not in TRANSFORM_REGISTRY:
        raise KeyError(f"Unknown causal transform '{t_name}'. Registered: {sorted(TRANSFORM_REGISTRY)}")

    kernel = TRANSFORM_REGISTRY[t_name]
    mat_ts = ensure_utc(materialized_at or datetime.now(UTC), "materialized_at")

    # Normalize timestamps to UTC microsecond precision and sort deterministically
    normalized = raw_df.with_columns(
        pl.col("event_time").dt.convert_time_zone("UTC").dt.cast_time_unit("us"),
        pl.col("availability_time").dt.convert_time_zone("UTC").dt.cast_time_unit("us"),
    ).sort(["entity_id", "event_time", "availability_time"])

    output_rows: list[dict[str, Any]] = []
    for _key, group_df in normalized.group_by("entity_id", maintain_order=True):
        rows = group_df.to_dicts()
        values = kernel(rows, dict(feature_def.transform.params), feature_def.transform.lookback_seconds)
        if len(values) != len(rows):
            raise LeakageViolationError(
                "Transform output length did not match input length",
                details={"transform": t_name, "expected": len(rows), "actual": len(values)},
            )
        for row, raw_val in zip(rows, values, strict=True):
            val = raw_val
            if _is_missing(val):
                if feature_def.null_policy == NullPolicy.REJECT:
                    raise NullPolicyViolationError(
                        f"Null value encountered for feature '{feature_def.name}' under REJECT null policy",
                        details={
                            "feature_name": feature_def.name,
                            "entity_id": row["entity_id"],
                            "event_time": row["event_time"].isoformat(),
                        },
                    )
                if feature_def.null_policy == NullPolicy.DEFAULT_VALUE:
                    val = _cast_value(feature_def.default_value, feature_def.value_type)
                else:
                    val = None
            else:
                val = _cast_value(val, feature_def.value_type)

            ev_ts = ensure_utc(row["event_time"], "event_time")
            av_ts = ensure_utc(row["availability_time"], "availability_time")
            late_sec = (av_ts - ev_ts).total_seconds()

            output_rows.append(
                {
                    "entity_id": str(row["entity_id"]),
                    "feature_name": feature_def.name,
                    "feature_version": feature_def.version,
                    "version_id": feature_def.version_id,
                    "event_time": ev_ts,
                    "availability_time": av_ts,
                    "materialized_at": mat_ts,
                    "value": val,
                    "unit": feature_def.unit,
                    "source_version": feature_def.source.source_version,
                    "run_id": run_id,
                    "is_late_arrival": late_sec > 0.0,
                    "late_arrival_seconds": late_sec,
                }
            )

    schema = {
        "entity_id": pl.String(),
        "feature_name": pl.String(),
        "feature_version": pl.String(),
        "version_id": pl.String(),
        "event_time": pl.Datetime("us", "UTC"),
        "availability_time": pl.Datetime("us", "UTC"),
        "materialized_at": pl.Datetime("us", "UTC"),
        "value": _polars_dtype(feature_def.value_type),
        "unit": pl.String(),
        "source_version": pl.String(),
        "run_id": pl.String(),
        "is_late_arrival": pl.Boolean(),
        "late_arrival_seconds": pl.Float64(),
    }
    if not output_rows:
        return pl.DataFrame(schema=schema)
    return pl.DataFrame(output_rows, schema=schema)
