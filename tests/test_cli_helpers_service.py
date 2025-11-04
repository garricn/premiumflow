"""Tests for CLI helpers service."""

import unittest
from decimal import Decimal

from premiumflow.services.cli_helpers import (
    create_target_label,
    filter_open_chains,
    format_account_label,
    format_expiration_date,
    format_percent,
    is_open_chain,
    parse_target_range,
)


class TestCliHelpers(unittest.TestCase):
    """Test CLI helper functions."""

    def test_is_open_chain_by_status_open(self):
        """Test is_open_chain with OPEN status."""
        chain = {"status": "OPEN"}
        self.assertTrue(is_open_chain(chain))

    def test_is_open_chain_by_status_closed(self):
        """Test is_open_chain with CLOSED status."""
        chain = {"status": "CLOSED"}
        self.assertFalse(is_open_chain(chain))

    def test_is_open_chain_by_last_transaction_sto(self):
        """Test is_open_chain with STO as last transaction."""
        chain = {"transactions": [{"Trans Code": "BTO"}, {"Trans Code": "STO"}]}
        self.assertTrue(is_open_chain(chain))

    def test_is_open_chain_by_last_transaction_bto(self):
        """Test is_open_chain with BTO as last transaction."""
        chain = {"transactions": [{"Trans Code": "STO"}, {"Trans Code": "BTO"}]}
        self.assertTrue(is_open_chain(chain))

    def test_is_open_chain_by_last_transaction_close(self):
        """Test is_open_chain with closing transaction."""
        chain = {"transactions": [{"Trans Code": "STO"}, {"Trans Code": "BTC"}]}
        self.assertFalse(is_open_chain(chain))

    def test_is_open_chain_no_transactions(self):
        """Test is_open_chain with no transactions."""
        chain = {"transactions": []}
        self.assertFalse(is_open_chain(chain))

    def test_is_open_chain_no_status_no_transactions(self):
        """Test is_open_chain with no status and no transactions."""
        chain = {}
        self.assertFalse(is_open_chain(chain))

    def test_parse_target_range_valid(self):
        """Test parsing valid target range."""
        result = parse_target_range("0.5-0.7")
        expected = (Decimal("0.5"), Decimal("0.7"))
        self.assertEqual(result, expected)

    def test_parse_target_range_with_spaces(self):
        """Test parsing target range with spaces."""
        result = parse_target_range(" 0.3 - 0.8 ")
        expected = (Decimal("0.3"), Decimal("0.8"))
        self.assertEqual(result, expected)

    def test_parse_target_range_invalid_format(self):
        """Test parsing invalid target range format."""
        with self.assertRaises(ValueError) as context:
            parse_target_range("0.5")
        self.assertIn("Invalid target range format", str(context.exception))

    def test_parse_target_range_invalid_bounds(self):
        """Test parsing target range with invalid bounds."""
        with self.assertRaises(ValueError) as context:
            parse_target_range("0.8-0.5")
        self.assertIn("Invalid target range format", str(context.exception))

    def test_parse_target_range_malformed(self):
        """Test parsing malformed target range."""
        with self.assertRaises(ValueError) as context:
            parse_target_range("0.5-0.7-0.9")
        self.assertIn("Invalid target range format", str(context.exception))

    def test_format_percent_whole_number(self):
        """Test formatting whole number percentage."""
        result = format_percent(Decimal("0.5"))
        self.assertEqual(result, "50%")

    def test_format_percent_decimal(self):
        """Test formatting decimal percentage."""
        result = format_percent(Decimal("0.1234"))
        self.assertEqual(result, "12.34%")

    def test_format_percent_rounding(self):
        """Test formatting percentage with rounding."""
        result = format_percent(Decimal("0.123456"))
        self.assertEqual(result, "12.35%")

    def test_format_percent_zero(self):
        """Test formatting zero percentage."""
        result = format_percent(Decimal("0"))
        self.assertEqual(result, "0%")

    def test_format_percent_negative(self):
        """Test formatting negative percentage."""
        result = format_percent(Decimal("-0.15"))
        self.assertEqual(result, "-15%")

    def test_filter_open_chains_mixed(self):
        """Test filtering mixed open and closed chains."""
        chains = [
            {"status": "OPEN"},
            {"status": "CLOSED"},
            {"transactions": [{"Trans Code": "STO"}]},
            {"transactions": [{"Trans Code": "BTC"}]},
        ]
        result = filter_open_chains(chains)
        expected = [
            {"status": "OPEN"},
            {"transactions": [{"Trans Code": "STO"}]},
        ]
        self.assertEqual(result, expected)

    def test_filter_open_chains_empty(self):
        """Test filtering empty chain list."""
        result = filter_open_chains([])
        self.assertEqual(result, [])

    def test_format_expiration_date_valid(self):
        """Test formatting valid expiration date."""
        result = format_expiration_date("2025-02-21")
        self.assertEqual(result, "02/21/2025")

    def test_format_expiration_date_single_digits(self):
        """Test formatting expiration date with single digits."""
        result = format_expiration_date("2025-1-5")
        self.assertEqual(result, "01/05/2025")

    def test_format_expiration_date_invalid_format(self):
        """Test formatting invalid expiration date format."""
        result = format_expiration_date("2025/02/21")
        self.assertEqual(result, "2025/02/21")  # Returns original if invalid

    def test_format_expiration_date_malformed(self):
        """Test formatting malformed expiration date."""
        result = format_expiration_date("not-a-date")
        self.assertEqual(result, "not-a-date")  # Returns original if invalid

    def test_create_target_label_single(self):
        """Test creating target label with single percentage."""
        target_percents = [Decimal("0.5")]
        result = create_target_label(target_percents)
        self.assertEqual(result, "Target (50%)")

    def test_create_target_label_multiple(self):
        """Test creating target label with multiple percentages."""
        target_percents = [Decimal("0.5"), Decimal("0.7")]
        result = create_target_label(target_percents)
        self.assertEqual(result, "Target (50%, 70%)")

    def test_create_target_label_decimal(self):
        """Test creating target label with decimal percentages."""
        target_percents = [Decimal("0.25"), Decimal("0.75")]
        result = create_target_label(target_percents)
        self.assertEqual(result, "Target (25%, 75%)")

    def test_create_target_label_empty(self):
        """Test creating target label with empty list."""
        target_percents = []
        result = create_target_label(target_percents)
        self.assertEqual(result, "Target ()")

    def test_format_account_label_with_number(self):
        """Format includes account number when provided."""
        self.assertEqual(
            format_account_label("Robinhood IRA", "RH-12345"),
            "Robinhood IRA (RH-12345)",
        )

    def test_format_account_label_without_number(self):
        """Format returns name when number is missing or None."""
        self.assertEqual(format_account_label("Brokerage", None), "Brokerage")
        self.assertEqual(format_account_label("Brokerage", ""), "Brokerage")


if __name__ == "__main__":
    unittest.main()
