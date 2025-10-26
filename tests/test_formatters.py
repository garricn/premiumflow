"""Unit tests for position specification formatting and parsing."""

import unittest
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from premiumflow.core.parser import (
    format_position_spec,
    parse_lookup_input,
)


class TestPositionFormatting(unittest.TestCase):
    """Test position specification formatting and parsing."""

    def test_format_position_spec_call(self):
        formatted = format_position_spec("TSLA", 550, "CALL", "11/21/2025")
        self.assertEqual(formatted, "TSLA $550 CALL 11/21/2025")

    def test_format_position_spec_put(self):
        formatted = format_position_spec("AAPL", 150, "PUT", "12/19/2025")
        self.assertEqual(formatted, "AAPL $150 PUT 12/19/2025")

    def test_format_position_spec_decimal_strike(self):
        formatted = format_position_spec("IREN", 70, "CALL", "2/20/2026")
        self.assertEqual(formatted, "IREN $70 CALL 2/20/2026")

    def test_parse_lookup_input_valid(self):
        lookup_str = "TSLA $550 C 2025-11-21"
        symbol, strike, option_type, expiration = parse_lookup_input(lookup_str)

        self.assertEqual(symbol, 'TSLA')
        self.assertEqual(strike, 550.0)
        self.assertEqual(option_type, 'C')
        self.assertEqual(expiration, '2025-11-21')

    def test_parse_lookup_input_lowercase(self):
        lookup_str = "tsla $550 c 2025-11-21"
        with self.assertRaises(ValueError) as context:
            parse_lookup_input(lookup_str)

        self.assertIn("invalid lookup format", str(context.exception).lower())

    def test_parse_lookup_input_invalid_format(self):
        lookup_str = "INVALID FORMAT"

        with self.assertRaises(ValueError) as context:
            parse_lookup_input(lookup_str)

        self.assertIn("invalid lookup format", str(context.exception).lower())

    def test_parse_lookup_input_missing_dollar(self):
        lookup_str = "TSLA 550 C 2025-11-21"

        with self.assertRaises(ValueError) as context:
            parse_lookup_input(lookup_str)

        self.assertIn("invalid lookup format", str(context.exception).lower())


if __name__ == '__main__':
    unittest.main(verbosity=2)
