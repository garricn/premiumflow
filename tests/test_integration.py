"""Unit tests for multi-roll chains and package integration."""

import unittest
import sys
from decimal import Decimal
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from premiumflow.services.chain_builder import detect_roll_chains
import premiumflow


class TestMultiRollChains(unittest.TestCase):
    """Test chains with multiple rolls."""
    
    def setUp(self):
        """Create test data for a multi-roll chain."""
        self.multi_roll_txns = [
            {'Activity Date': '9/19/2025', 'Trans Code': 'STO', 'Quantity': '1',
             'Description': 'IREN 10/17/2025 Call $50.00', 'Instrument': 'IREN',
             'Amount': '$132.96', 'Price': '$1.33'},
            {'Activity Date': '9/22/2025', 'Trans Code': 'BTC', 'Quantity': '1',
             'Description': 'IREN 10/17/2025 Call $50.00', 'Instrument': 'IREN',
             'Amount': '($265.08)', 'Price': '$2.65'},
            {'Activity Date': '9/22/2025', 'Trans Code': 'STO', 'Quantity': '1',
             'Description': 'IREN 11/21/2025 Call $55.00', 'Instrument': 'IREN',
             'Amount': '$390.00', 'Price': '$3.90'},
            {'Activity Date': '10/3/2025', 'Trans Code': 'BTC', 'Quantity': '1',
             'Description': 'IREN 11/21/2025 Call $55.00', 'Instrument': 'IREN',
             'Amount': '($807.08)', 'Price': '$8.07'},
            {'Activity Date': '10/3/2025', 'Trans Code': 'STO', 'Quantity': '1',
             'Description': 'IREN 12/19/2025 Call $60.00', 'Instrument': 'IREN',
             'Amount': '$877.00', 'Price': '$8.77'},
            {'Activity Date': '10/6/2025', 'Trans Code': 'BTC', 'Quantity': '1',
             'Description': 'IREN 12/19/2025 Call $60.00', 'Instrument': 'IREN',
             'Amount': '($1185.08)', 'Price': '$11.85'},
            {'Activity Date': '10/6/2025', 'Trans Code': 'STO', 'Quantity': '1',
             'Description': 'IREN 02/20/2026 Call $70.00', 'Instrument': 'IREN',
             'Amount': '$1325.00', 'Price': '$13.25'},
        ]
    
    def test_detect_multi_roll_chain(self):
        """Test detection of a chain with multiple rolls."""
        chains = detect_roll_chains(self.multi_roll_txns)
        
        self.assertEqual(len(chains), 1, "Should detect 1 chain")
        
        chain = chains[0]
        self.assertEqual(chain['roll_count'], 3, "Should have 3 rolls")
        self.assertEqual(chain['status'], 'OPEN', "Chain should be OPEN")
        self.assertEqual(len(chain['transactions']), 7, "Should have 7 transactions")
    
    def test_multi_roll_pnl(self):
        """Test P&L calculation for multi-roll chain."""
        chains = detect_roll_chains(self.multi_roll_txns)
        chain = chains[0]
        
        # Total credits: 132.96 + 390.00 + 877.00 + 1325.00 = 2724.96
        expected_credits = Decimal('132.96') + Decimal('390.00') + Decimal('877.00') + Decimal('1325.00')
        self.assertEqual(Decimal(str(chain['total_credits'])), expected_credits)
        
        # Total debits: 265.08 + 807.08 + 1185.08 = 2257.24
        expected_debits = Decimal('265.08') + Decimal('807.08') + Decimal('1185.08')
        self.assertEqual(Decimal(str(chain['total_debits'])), expected_debits)


class TestPremiumFlowPackage(unittest.TestCase):
    """Ensure the premiumflow package exports expected helpers."""

    def test_reexports_legacy_functions(self):
        from premiumflow.core.parser import (
            format_position_spec,
            parse_lookup_input,
            is_call_option,
            is_options_transaction,
            is_put_option,
        )
        from premiumflow import find_chain_by_position

        expected = {
            'detect_roll_chains': detect_roll_chains,
            'find_chain_by_position': find_chain_by_position,
            'format_position_spec': format_position_spec,
            'is_call_option': is_call_option,
            'is_options_transaction': is_options_transaction,
            'is_put_option': is_put_option,
            'parse_lookup_input': parse_lookup_input,
        }

        for name, reference in expected.items():
            with self.subTest(name=name):
                # Check that the function exists and is callable
                self.assertTrue(hasattr(premiumflow, name), f"Function {name} not found in premiumflow package")
                self.assertTrue(callable(getattr(premiumflow, name)), f"Attribute {name} is not callable")

    def test_package_version(self):
        self.assertEqual(premiumflow.__version__, '0.1.0')


if __name__ == '__main__':
    unittest.main(verbosity=2)
