"""Unit tests for CSV parsing and options transaction detection."""

import unittest
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from premiumflow.core.parser import (
    is_options_transaction,
    is_call_option,
    is_put_option,
)


class TestCSVParsing(unittest.TestCase):
    """Test CSV parsing and options transaction detection."""
    
    def test_is_options_transaction_with_trans_code(self):
        """Test options detection via transaction code."""
        row = {'Trans Code': 'BTC', 'Description': 'Some description'}
        self.assertTrue(is_options_transaction(row))
        
        row = {'Trans Code': 'STO', 'Description': 'Some description'}
        self.assertTrue(is_options_transaction(row))
        
        row = {'Trans Code': 'OASGN', 'Description': 'Some description'}
        self.assertTrue(is_options_transaction(row))
    
    def test_is_options_transaction_with_description(self):
        """Test options detection via description."""
        row = {'Trans Code': 'OTHER', 'Description': 'TSLA Call $500'}
        self.assertTrue(is_options_transaction(row))
        
        row = {'Trans Code': 'OTHER', 'Description': 'AAPL Put $150'}
        self.assertTrue(is_options_transaction(row))
    
    def test_is_options_transaction_non_options(self):
        """Test that non-options transactions are rejected."""
        row = {'Trans Code': 'BUY', 'Description': 'TSLA Common Stock'}
        self.assertFalse(is_options_transaction(row))
    
    def test_is_options_transaction_null_handling(self):
        """Test handling of None values."""
        row = {'Trans Code': None, 'Description': None}
        self.assertFalse(is_options_transaction(row))
    
    def test_is_call_option(self):
        """Test call option detection."""
        self.assertTrue(is_call_option("TSLA 11/21/2025 Call $550.00"))
        self.assertFalse(is_call_option("TSLA 11/21/2025 Put $550.00"))
        self.assertFalse(is_call_option(""))
    
    def test_is_put_option(self):
        """Test put option detection."""
        self.assertTrue(is_put_option("TSLA 11/21/2025 Put $550.00"))
        self.assertFalse(is_put_option("TSLA 11/21/2025 Call $550.00"))
        self.assertFalse(is_put_option(""))


if __name__ == '__main__':
    unittest.main(verbosity=2)
