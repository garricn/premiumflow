"""Services for roll chain analysis."""

from .analyzer import calculate_breakeven, calculate_pnl
from .cash_flows import CashFlowRow, CashFlowSummary, CashFlowTotals, summarize_cash_flows
from .chain_builder import detect_roll_chains
from .cli_helpers import (
    create_target_label,
    filter_open_chains,
    format_account_label,
    format_expiration_date,
    is_open_chain,
    parse_target_range,
)
from .display import (
    calculate_target_price_range,
    ensure_display_name,
    format_breakeven,
    format_currency,
    format_net_pnl,
    format_option_display,
    format_percent,
    format_price_range,
    format_realized_pnl,
    format_target_close_prices,
    prepare_chain_display,
    prepare_transactions_for_display,
)
from .json_serializer import (
    build_ingest_payload,
    serialize_chain,
    serialize_decimal,
    serialize_normalized_transaction,
    serialize_transaction,
)
from .leg_matching import (
    MatchedLeg,
    MatchedLegLot,
    match_leg_fills,
    match_legs,
)
from .transactions import normalized_to_csv_dicts

__all__ = [
    "detect_roll_chains",
    "calculate_pnl",
    "calculate_breakeven",
    "format_currency",
    "format_breakeven",
    "format_percent",
    "format_price_range",
    "format_target_close_prices",
    "ensure_display_name",
    "format_option_display",
    "prepare_transactions_for_display",
    "prepare_chain_display",
    "format_net_pnl",
    "format_realized_pnl",
    "calculate_target_price_range",
    "serialize_decimal",
    "serialize_transaction",
    "serialize_normalized_transaction",
    "serialize_chain",
    "build_ingest_payload",
    "is_open_chain",
    "parse_target_range",
    "filter_open_chains",
    "format_expiration_date",
    "format_account_label",
    "create_target_label",
    "summarize_cash_flows",
    "CashFlowSummary",
    "CashFlowTotals",
    "CashFlowRow",
    "normalized_to_csv_dicts",
    "match_legs",
    "match_leg_fills",
    "MatchedLeg",
    "MatchedLegLot",
]
