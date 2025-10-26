"""Unit tests for target price calculations."""

from decimal import Decimal

from rollchain.services.targets import calculate_target_percents, compute_target_close_prices


def test_calculate_target_percents_includes_midpoint():
    percents = calculate_target_percents((Decimal("0.5"), Decimal("0.7")))
    assert percents == [Decimal("0.5"), Decimal("0.6"), Decimal("0.7")]


def test_calculate_target_percents_single_value():
    percents = calculate_target_percents([Decimal("0.4"), Decimal("0.4")])
    assert percents == [Decimal("0.4")]


def test_compute_target_close_prices_bto_sorted_low_to_high():
    percents = [Decimal("0.5"), Decimal("0.6"), Decimal("0.7")]
    close_prices = compute_target_close_prices("BTO", "$4.50", percents)
    assert close_prices == [Decimal("6.75"), Decimal("7.20"), Decimal("7.65")]


def test_compute_target_close_prices_sto_sorted_high_to_low():
    percents = [Decimal("0.5"), Decimal("0.6"), Decimal("0.7")]
    close_prices = compute_target_close_prices("STO", "$3.00", percents)
    assert close_prices == [Decimal("1.50"), Decimal("1.20"), Decimal("0.90")]


def test_compute_target_close_prices_returns_none_for_invalid_code():
    percents = [Decimal("0.5"), Decimal("0.7")]
    assert compute_target_close_prices("STC", "$3.00", percents) is None


def test_compute_target_close_prices_returns_none_for_unparseable_price():
    percents = [Decimal("0.5"), Decimal("0.7")]
    assert compute_target_close_prices("STO", "", percents) is None
