"""Unit tests for position specification formatting and parsing."""

import unittest
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from src.roll import (
    format_position_spec,
    parse_lookup_input,
)


class TestPositionFormatting(unittest.TestCase):
    """Test position specification formatting and parsing."""
    
    def test_format_position_spec_call(self):
        """Test formatting a call option description."""
        desc = "TSLA 11/21/2025 Call $550.00"
        formatted = format_position_spec(desc)
        self.assertEqual(formatted, "TSLA $550 CALL 11/21/2025")
    
    def test_format_position_spec_put(self):
        """Test formatting a put option description."""
        desc = "AAPL 12/19/2025 Put $150.00"
        formatted = format_position_spec(desc)
        self.assertEqual(formatted, "AAPL $150 PUT 12/19/2025")
    
    def test_format_position_spec_decimal_strike(self):
        """Test formatting with decimal strike price."""
        desc = "IREN 2/20/2026 Call $70.00"
        formatted = format_position_spec(desc)
        self.assertEqual(formatted, "IREN $70 CALL 2/20/2026")
    
    def test_parse_lookup_input_valid(self):
        """Test parsing valid lookup input."""
        lookup_str = "TSLA $550 CALL 11/21/2025"
        spec = parse_lookup_input(lookup_str)
        
        self.assertEqual(spec['ticker'], 'TSLA')
        self.assertEqual(spec['strike'], '550')
        self.assertEqual(spec['option_type'], 'CALL')
        self.assertEqual(spec['expiration'], '11/21/2025')
    
    def test_parse_lookup_input_lowercase(self):
        """Test parsing with lowercase input."""
        lookup_str = "tsla $550 call 11/21/2025"
        spec = parse_lookup_input(lookup_str)
        
        self.assertEqual(spec['ticker'], 'TSLA')
        self.assertEqual(spec['option_type'], 'CALL')
    
    def test_parse_lookup_input_invalid_format(self):
        """Test parsing with invalid format."""
        lookup_str = "INVALID FORMAT"
        
        with self.assertRaises(ValueError) as context:
            parse_lookup_input(lookup_str)
        
        self.assertIn("format must be", str(context.exception).lower())
    
    def test_parse_lookup_input_missing_dollar(self):
        """Test parsing with missing $ sign."""
        lookup_str = "TSLA 550 CALL 11/21/2025"
        
        with self.assertRaises(ValueError) as context:
            parse_lookup_input(lookup_str)
        
        self.assertIn("must start with $", str(context.exception).lower())


if __name__ == '__main__':
    unittest.main(verbosity=2)
