"""Unit tests for chain lookup functionality."""

import unittest
import tempfile
import os
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from src.roll import find_chain_by_position


class TestLookupFunctionality(unittest.TestCase):
    """Test chain lookup functionality."""
    
    def setUp(self):
        """Create a temporary CSV file for testing."""
        self.csv_content = '''Activity Date,Process Date,Settle Date,Instrument,Description,Trans Code,Quantity,Price,Amount
9/12/2025,9/12/2025,9/15/2025,TSLA,TSLA 10/17/2025 Call $515.00,STO,1,$3.00,$299.95
9/22/2025,9/22/2025,9/23/2025,TSLA,TSLA 10/17/2025 Call $515.00,BTC,1,$7.30,($730.04)
9/22/2025,9/22/2025,9/23/2025,TSLA,TSLA 11/21/2025 Call $550.00,STO,1,$15.75,$1,574.95
10/8/2025,10/8/2025,10/9/2025,TSLA,TSLA 11/21/2025 Call $550.00,BTC,1,$8.75,($875.04)
'''
        self.temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv')
        self.temp_file.write(self.csv_content)
        self.temp_file.close()
    
    def tearDown(self):
        """Clean up temporary file."""
        os.unlink(self.temp_file.name)
    
    def test_find_chain_by_position_found(self):
        """Test finding a chain by position specification."""
        lookup_spec = {
            'ticker': 'TSLA',
            'strike': '550',
            'option_type': 'CALL',
            'expiration': '11/21/2025'
        }
        
        chain = find_chain_by_position(self.temp_file.name, lookup_spec)
        
        self.assertIsNotNone(chain, "Should find the chain")
        self.assertEqual(chain['ticker'], 'TSLA')
        self.assertEqual(chain['status'], 'CLOSED')
    
    def test_find_chain_by_position_not_found(self):
        """Test when position is not found."""
        lookup_spec = {
            'ticker': 'AAPL',
            'strike': '150',
            'option_type': 'CALL',
            'expiration': '12/19/2025'
        }
        
        chain = find_chain_by_position(self.temp_file.name, lookup_spec)
        
        self.assertIsNone(chain, "Should not find a chain")
    
    def test_find_chain_with_different_strike(self):
        """Test finding chain with a different strike from same chain."""
        # First position in the chain
        lookup_spec = {
            'ticker': 'TSLA',
            'strike': '515',
            'option_type': 'CALL',
            'expiration': '10/17/2025'
        }
        
        chain = find_chain_by_position(self.temp_file.name, lookup_spec)
        
        self.assertIsNotNone(chain, "Should find the chain via first position")
        self.assertEqual(chain['ticker'], 'TSLA')


if __name__ == '__main__':
    unittest.main(verbosity=2)
