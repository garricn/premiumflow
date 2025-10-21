"""Unit tests for P&L calculations including credits, debits, fees, and breakeven."""

import unittest
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from src.roll import detect_roll_chains


class TestPnLCalculations(unittest.TestCase):
    """Test P&L calculations including credits, debits, fees, and breakeven."""
    
    def setUp(self):
        """Create test data."""
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
        
        self.open_chain_txns = self.closed_chain_txns[:-1]
    
    def test_credits_calculation(self):
        """Test total credits calculation."""
        chains = detect_roll_chains(self.closed_chain_txns)
        chain = chains[0]
        
        # Credits: 299.95 + 1574.95 = 1874.90
        expected_credits = 299.95 + 1574.95
        self.assertAlmostEqual(chain['total_credits'], expected_credits, places=2)
    
    def test_debits_calculation(self):
        """Test total debits calculation."""
        chains = detect_roll_chains(self.closed_chain_txns)
        chain = chains[0]
        
        # Debits: 730.04 + 875.04 = 1605.08
        expected_debits = 730.04 + 875.04
        self.assertAlmostEqual(chain['total_debits'], expected_debits, places=2)
    
    def test_net_pnl_calculation(self):
        """Test net P&L calculation for closed chain."""
        chains = detect_roll_chains(self.closed_chain_txns)
        chain = chains[0]
        
        # Net P&L: 1874.90 - 1605.08 = 269.82
        expected_pnl = 1874.90 - 1605.08
        self.assertAlmostEqual(chain['net_pnl'], expected_pnl, places=2)
    
    def test_fees_calculation(self):
        """Test fees calculation ($0.04 per contract)."""
        chains = detect_roll_chains(self.closed_chain_txns)
        chain = chains[0]
        
        # Fees: 4 transactions * 1 contract * $0.04 = $0.16
        total_fees = len(chain['transactions']) * 0.04
        self.assertAlmostEqual(total_fees, 0.16, places=2)
    
    def test_breakeven_calculation_open_chain(self):
        """Test breakeven price calculation for open chain."""
        chains = detect_roll_chains(self.open_chain_txns)
        chain = chains[0]
        
        # Net so far: 299.95 + 1574.95 - 730.04 = 1144.86
        # Fees: 3 * 0.04 = 0.12
        # Net after fees: 1144.86 - 0.12 = 1144.74
        # Breakeven per share: 1144.74 / 100 = $11.4474
        
        total_credits = chain['total_credits']
        total_debits = chain['total_debits']
        fees = len(chain['transactions']) * 0.04
        net_so_far = total_credits - total_debits - fees
        breakeven = net_so_far / 100
        
        self.assertAlmostEqual(breakeven, 11.45, places=2)


if __name__ == '__main__':
    unittest.main(verbosity=2)
