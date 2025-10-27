"""Unit tests for P&L calculations including credits, debits, fees, and breakeven."""

import sys
import unittest
from decimal import Decimal
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from premiumflow.services.chain_builder import detect_roll_chains


class TestPnLCalculations(unittest.TestCase):
    """Test P&L calculations including credits, debits, fees, and breakeven."""

    def setUp(self):
        """Create test data."""
        self.closed_chain_txns = [
            {
                "Activity Date": "9/12/2025",
                "Trans Code": "STO",
                "Quantity": "1",
                "Description": "TSLA 10/17/2025 Call $515.00",
                "Instrument": "TSLA",
                "Amount": "$299.95",
                "Price": "$3.00",
            },
            {
                "Activity Date": "9/22/2025",
                "Trans Code": "BTC",
                "Quantity": "1",
                "Description": "TSLA 10/17/2025 Call $515.00",
                "Instrument": "TSLA",
                "Amount": "($730.04)",
                "Price": "$7.30",
            },
            {
                "Activity Date": "9/22/2025",
                "Trans Code": "STO",
                "Quantity": "1",
                "Description": "TSLA 11/21/2025 Call $550.00",
                "Instrument": "TSLA",
                "Amount": "$1,574.95",
                "Price": "$15.75",
            },
            {
                "Activity Date": "10/8/2025",
                "Trans Code": "BTC",
                "Quantity": "1",
                "Description": "TSLA 11/21/2025 Call $550.00",
                "Instrument": "TSLA",
                "Amount": "($875.04)",
                "Price": "$8.75",
            },
        ]

        self.open_chain_txns = self.closed_chain_txns[:-1]

    def test_credits_calculation(self):
        """Test total credits calculation."""
        chains = detect_roll_chains(self.closed_chain_txns)
        chain = chains[0]

        # Credits: 299.95 + 1574.95 = 1874.90
        expected_credits = Decimal("299.95") + Decimal("1574.95")
        self.assertEqual(Decimal(str(chain["total_credits"])), expected_credits)

    def test_debits_calculation(self):
        """Test total debits calculation."""
        chains = detect_roll_chains(self.closed_chain_txns)
        chain = chains[0]

        # Debits: 730.04 + 875.04 = 1605.08
        expected_debits = Decimal("730.04") + Decimal("875.04")
        self.assertEqual(Decimal(str(chain["total_debits"])), expected_debits)

    def test_net_pnl_calculation(self):
        """Test net P&L calculation for closed chain."""
        chains = detect_roll_chains(self.closed_chain_txns)
        chain = chains[0]

        # Net P&L: 1874.90 - 1605.08 = 269.82
        expected_pnl = Decimal("1874.90") - Decimal("1605.08")
        actual_pnl = Decimal(str(chain["net_pnl"])).quantize(Decimal("0.01"))
        self.assertEqual(actual_pnl, expected_pnl)

    def test_fees_calculation(self):
        """Test fees calculation ($0.04 per contract)."""
        chains = detect_roll_chains(self.closed_chain_txns)
        chain = chains[0]

        # Fees: 4 transactions * 1 contract * $0.04 = $0.16
        total_fees = len(chain["transactions"]) * Decimal("0.04")
        self.assertEqual(total_fees, Decimal("0.16"))

    def test_breakeven_calculation_open_chain(self):
        """Test breakeven price calculation for open chain."""
        chains = detect_roll_chains(self.open_chain_txns)
        chain = chains[0]

        # Net so far: 299.95 + 1574.95 - 730.04 = 1144.86
        # Fees: 3 * 0.04 = 0.12
        # Net after fees: 1144.86 - 0.12 = 1144.74
        # Breakeven per share: 1144.74 / 100 = $11.4474

        total_credits = Decimal(str(chain["total_credits"]))
        total_debits = Decimal(str(chain["total_debits"]))
        fees = len(chain["transactions"]) * Decimal("0.04")
        net_so_far = total_credits - total_debits - fees
        breakeven = net_so_far / Decimal("100")

        self.assertEqual(breakeven, Decimal("11.4474"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
