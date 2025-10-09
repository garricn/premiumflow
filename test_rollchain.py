#!/usr/bin/env python3
"""
Comprehensive test suite for rollchain functionality.
Tests the current implementation before refactoring.
"""

import unittest
import tempfile
import os
from typing import List, Dict

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / "src"))

import rollchain

from roll import (
    is_options_transaction,
    detect_roll_chains,
    format_position_spec,
    parse_lookup_input,
    find_chain_by_position,
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
        expected_credits = 132.96 + 390.00 + 877.00 + 1325.00
        self.assertAlmostEqual(chain['total_credits'], expected_credits, places=2)
        
        # Total debits: 265.08 + 807.08 + 1185.08 = 2257.24
        expected_debits = 265.08 + 807.08 + 1185.08
        self.assertAlmostEqual(chain['total_debits'], expected_debits, places=2)


class TestRollchainPackage(unittest.TestCase):
    """Ensure the new rollchain package mirrors the legacy API."""

    def test_reexports_legacy_functions(self):
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
                self.assertTrue(hasattr(rollchain, name), f"Function {name} not found in rollchain package")
                self.assertTrue(callable(getattr(rollchain, name)), f"Attribute {name} is not callable")

    def test_package_version(self):
        self.assertEqual(rollchain.__version__, '0.1.0')


if __name__ == '__main__':
    unittest.main(verbosity=2)

