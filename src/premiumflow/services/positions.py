"""Aggregated views of open equity and option positions."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from typing import Dict, Iterable, List, Optional, Tuple

from ..persistence import SQLiteRepository
from .leg_matching import MatchedLegLot
from .stock_lots import fetch_stock_lot_summaries
from .transaction_loader import fetch_normalized_transactions, match_legs_from_transactions


@dataclass(frozen=True)
class EquityPosition:
    """Aggregated equity holdings derived from open stock lots."""

    account_name: str
    account_number: Optional[str]
    symbol: str
    direction: str  # "long" or "short"
    shares: int
    basis_total: Decimal
    basis_per_share: Decimal
    realized_pnl_total: Decimal


@dataclass(frozen=True)
class OptionPosition:
    """Aggregated option holdings derived from open matched lots."""

    account_name: str
    account_number: Optional[str]
    symbol: str
    option_type: str
    strike: Decimal
    expiration: str
    direction: str  # "long" or "short"
    contracts: int
    open_credit: Decimal
    open_fees: Decimal
    credit_remaining: Decimal


@dataclass
class _EquityAccumulator:
    direction: str
    shares: int
    basis_total: Decimal
    realized_total: Decimal


@dataclass
class _OptionAccumulator:
    contracts: int = 0
    open_credit: Decimal = Decimal("0")
    open_fees: Decimal = Decimal("0")
    credit_remaining: Decimal = Decimal("0")


def fetch_equity_positions(
    repository: SQLiteRepository,
    *,
    account_name: Optional[str] = None,
    account_number: Optional[str] = None,
    ticker: Optional[str] = None,
) -> List[EquityPosition]:
    """Aggregate open stock lots into per-symbol equity positions."""
    summaries = fetch_stock_lot_summaries(
        repository,
        account_name=account_name,
        account_number=account_number,
        ticker=ticker,
        status="open",
    )

    aggregates: Dict[Tuple[str, Optional[str], str], _EquityAccumulator] = {}

    for summary in summaries:
        key = (summary.account_name, summary.account_number, summary.symbol)
        quantity = summary.quantity

        entry = aggregates.get(key)
        if entry is None:
            direction = "long" if quantity >= 0 else "short"
            aggregates[key] = _EquityAccumulator(
                direction=direction,
                shares=quantity,
                basis_total=summary.basis_total,
                realized_total=summary.realized_pnl_total,
            )
            continue

        entry.shares += quantity
        entry.basis_total += summary.basis_total
        entry.realized_total += summary.realized_pnl_total

    positions: List[EquityPosition] = []
    for (acct_name, acct_number, symbol), entry in aggregates.items():
        shares = entry.shares
        basis_total = entry.basis_total
        realized_total = entry.realized_total

        share_count = abs(shares)
        basis_per_share = Decimal("0") if share_count == 0 else (basis_total / Decimal(share_count))
        basis_per_share = basis_per_share.quantize(Decimal("0.0001"))

        direction = "long" if shares >= 0 else "short"

        positions.append(
            EquityPosition(
                account_name=acct_name,
                account_number=acct_number,
                symbol=symbol,
                direction=direction,
                shares=shares,
                basis_total=basis_total,
                basis_per_share=basis_per_share,
                realized_pnl_total=realized_total,
            )
        )

    positions.sort(key=lambda pos: (pos.account_name, pos.account_number or "", pos.symbol))
    return positions


def _iter_open_option_lots(lots: Iterable[MatchedLegLot]) -> Iterable[MatchedLegLot]:
    for lot in lots:
        if lot.is_open:
            yield lot


def fetch_option_positions(
    repository: SQLiteRepository,
    *,
    account_name: Optional[str] = None,
    account_number: Optional[str] = None,
    ticker: Optional[str] = None,
) -> List[OptionPosition]:
    """Aggregate open option lots into per-contract positions."""

    account_keys: List[Tuple[str, Optional[str]]]
    if account_name is not None:
        account_keys = [(account_name, account_number)]
    else:
        account_keys = repository.list_accounts()

    positions: List[OptionPosition] = []
    for acct_name, acct_number in account_keys:
        transactions = fetch_normalized_transactions(
            repository,
            account_name=acct_name,
            account_number=acct_number,
            ticker=ticker,
        )
        matched = match_legs_from_transactions(transactions)

        aggregates: Dict[Tuple[str, str, Decimal, str, str], _OptionAccumulator] = defaultdict(
            _OptionAccumulator
        )

        for leg in matched:
            if leg.account_name != acct_name or leg.account_number != acct_number:
                continue
            if ticker is not None and leg.contract.symbol.upper() != ticker.strip().upper():
                continue

            for lot in _iter_open_option_lots(leg.lots):
                key = (
                    lot.contract.symbol,
                    lot.contract.option_type,
                    lot.contract.strike,
                    lot.contract.expiration.isoformat(),
                    lot.direction,
                )
                entry = aggregates[key]
                entry.contracts += lot.quantity
                entry.open_credit += lot.open_premium
                entry.open_fees += lot.open_fees
                entry.credit_remaining += lot.credit_remaining

        for (symbol, option_type, strike, expiration, direction), entry in aggregates.items():
            contracts = entry.contracts
            if contracts == 0:
                continue

            positions.append(
                OptionPosition(
                    account_name=acct_name,
                    account_number=acct_number,
                    symbol=symbol,
                    option_type=option_type,
                    strike=strike,
                    expiration=expiration,
                    direction=direction,
                    contracts=contracts,
                    open_credit=entry.open_credit,
                    open_fees=entry.open_fees,
                    credit_remaining=entry.credit_remaining,
                )
            )

    positions.sort(
        key=lambda pos: (
            pos.account_name,
            pos.account_number or "",
            pos.symbol,
            pos.expiration,
            pos.strike,
            pos.direction,
        )
    )
    return positions


def fetch_positions(
    repository: SQLiteRepository,
    *,
    account_name: Optional[str] = None,
    account_number: Optional[str] = None,
    ticker: Optional[str] = None,
) -> Tuple[List[EquityPosition], List[OptionPosition]]:
    """Return both equity and option positions for the requested filters."""

    equities = fetch_equity_positions(
        repository,
        account_name=account_name,
        account_number=account_number,
        ticker=ticker,
    )
    options = fetch_option_positions(
        repository,
        account_name=account_name,
        account_number=account_number,
        ticker=ticker,
    )
    return equities, options
