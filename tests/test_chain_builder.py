"""Unit tests for chain detection and roll chain building."""

import unittest
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from src.roll import detect_roll_chains


class TestRollChainDetection(unittest.TestCase):
    """Test roll chain building and status detection."""
    
    def setUp(self):
        """Create test data for a closed roll chain."""
        self.closed_chain_txns = [
            {'Activity Date': '9/12/2025', 'Trans Code': 'STO', 'Quantity': '1', 
             'Description': 'TSLA 10/17/2025 Call $515.00', 'Instrument': 'TSLA',
             'Amount': '$299.95', 'Price': '$3.00'},
            {'Activity Date': '9/22/2025', 'Trans Code': 'BTC', 'Quantity': '1',
             'Description': 'TSLA 10/17/2025 Call $515.00', 'Instrument': 'TSLA',
             'Amount': '($730.04)', 'Price': '$7.30'},
            {'Activity Date': '9/22/2025', 'Trans Code': 'STO', 'Quantity': '1',
             'Description': 'TSLA 11/21/2025 Call $550.00', 'Instrument': 'TSLA',
             'Amount': '$1,574.95', 'Price': '$15.75'},
            {'Activity Date': '10/8/2025', 'Trans Code': 'BTC', 'Quantity': '1',
             'Description': 'TSLA 11/21/2025 Call $550.00', 'Instrument': 'TSLA',
             'Amount': '($875.04)', 'Price': '$8.75'},
        ]
        
        # Open chain (remove last closing transaction)
        self.open_chain_txns = self.closed_chain_txns[:-1]
    
    def test_detect_closed_chain(self):
        """Test detection of a closed roll chain."""
        chains = detect_roll_chains(self.closed_chain_txns)
        
        self.assertEqual(len(chains), 1, "Should detect exactly 1 chain")
        
        chain = chains[0]
        self.assertEqual(chain['status'], 'CLOSED', "Chain should be CLOSED")
        self.assertEqual(chain['ticker'], 'TSLA')
        self.assertEqual(chain['start_date'], '9/12/2025')
        self.assertEqual(chain['end_date'], '10/8/2025')
        self.assertEqual(chain['roll_count'], 1, "Should have 1 roll")
        self.assertEqual(len(chain['transactions']), 4, "Should have 4 transactions")
    
    def test_detect_open_chain(self):
        """Test detection of an open roll chain."""
        chains = detect_roll_chains(self.open_chain_txns)
        
        self.assertEqual(len(chains), 1, "Should detect exactly 1 chain")
        
        chain = chains[0]
        self.assertEqual(chain['status'], 'OPEN', "Chain should be OPEN")
        self.assertEqual(chain['ticker'], 'TSLA')
        self.assertEqual(len(chain['transactions']), 3, "Should have 3 transactions")
    
    def test_chain_transaction_order(self):
        """Test that chain transactions are in chronological order."""
        from datetime import datetime
        
        chains = detect_roll_chains(self.closed_chain_txns)
        chain = chains[0]
        
        txns = chain['transactions']
        for i in range(len(txns) - 1):
            current_date = datetime.strptime(txns[i]['Activity Date'], '%m/%d/%Y')
            next_date = datetime.strptime(txns[i + 1]['Activity Date'], '%m/%d/%Y')
            # Dates should be in ascending order (or same day for rolls)
            self.assertLessEqual(current_date, next_date)


if __name__ == '__main__':
    unittest.main(verbosity=2)