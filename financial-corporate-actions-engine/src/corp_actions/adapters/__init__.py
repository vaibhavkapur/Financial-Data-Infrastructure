"""Source and integration adapters (JSON fixtures, optional Polars, TDI sibling interfaces)."""

from corp_actions.adapters.json_adapter import (
    load_actions_json,
    load_fixture_bundle,
    load_fx_rates_json,
    load_positions_json,
    load_prices_json,
    parse_action_dict,
    parse_fx_dict,
    parse_position_dict,
    parse_price_dict,
)
from corp_actions.adapters.polars_adapter import (
    adjusted_series_to_polars,
    polars_available,
    position_ledger_to_polars,
)
from corp_actions.adapters.tdi_interfaces import (
    QuantResearchSeriesConsumer,
    SecurityMasterAdapter,
    TickDataPriceAdapter,
)

__all__ = [
    "QuantResearchSeriesConsumer",
    "SecurityMasterAdapter",
    "TickDataPriceAdapter",
    "adjusted_series_to_polars",
    "load_actions_json",
    "load_fixture_bundle",
    "load_fx_rates_json",
    "load_positions_json",
    "load_prices_json",
    "parse_action_dict",
    "parse_fx_dict",
    "parse_position_dict",
    "parse_price_dict",
    "polars_available",
    "position_ledger_to_polars",
]
