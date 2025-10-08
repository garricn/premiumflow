#!/usr/bin/env python3
"""
Unit tests for the roll detection logic
"""

import unittest
import tempfile
import os
import sys

# Add current directory to path so we can import roll module
sys.path.insert(0, os.getcwd())

import roll
from roll import (
    get_options_transactions,
    detect_rolls,
    deduplicate_transactions,
    is_call_option,
    is_put_option
)


class TestRollDetection(unittest.TestCase):
    
    def setUp(self):
        """Set up test data."""
        # Create sample CSV content for testing
        self.sample_csv_content = '''"Activity Date","Process Date","Settle Date","Instrument","Description","Trans Code","Quantity","Price","Amount"
"10/7/2025","10/7/2025","10/8/2025","TSLA","TSLA 11/21/2025 Call $550.00","BTC","1","$8.75","($875.04)"
"10/7/2025","10/7/2025","10/8/2025","TSLA","TSLA 2/20/2026 Call $600.00","STO","1","$12.00","$1,199.10"
"10/6/2025","10/6/2025","10/7/2025","NVDA","NVDA 11/21/2025 Put $170.00","STO","2","$5.54","$1,107.91"
"10/6/2025","10/6/2025","10/7/2025","NVDA","NVDA 10/31/2025 Put $165.00","BTC","2","$4.00","($800.16)"
"9/22/2025","9/22/2025","9/23/2025","PLTR","PLTR 10/17/2025 Call $185.00","BTC","1","$8.39","($839.04)"
"9/22/2025","9/22/2025","9/23/2025","PLTR","PLTR 11/21/2025 Call $200.00","STO","1","$11.32","$1,131.95"
"9/22/2025","9/22/2025","9/23/2025","PLTR","PLTR 10/17/2025 Call $185.00","BTC","1","$8.39","($839.04)"
"9/22/2025","9/22/2025","9/23/2025","PLTR","PLTR 11/21/2025 Call $200.00","STO","1","$11.32","$1,131.95"
"9/5/2025","9/5/2025","9/8/2025","HOOD","HOOD 9/5/2025 Call $104.00","OASGN","1","",""
"9/5/2025","9/5/2025","9/8/2025","HOOD","HOOD 9/5/2025 Call $104.00","STO","1","$1.08","$107.95"
"9/5/2025","9/5/2025","9/8/2025","HOOD","HOOD 9/5/2025 Put $96.00","BTC","1","$0.50","($50.04)"
"9/5/2025","9/5/2025","9/8/2025","HOOD","HOOD 9/5/2025 Put $96.00","STO","1","$1.21","$120.95"
"8/29/2025","8/29/2025","9/2/2025","TMC","TMC 11/21/2025 Call $11.00","BTC","20","$0.94","($1,880.84)"
"8/29/2025","8/29/2025","9/2/2025","TMC","TMC 11/21/2025 Call $11.00","BTC","20","$0.94","($1,880.84)"
"8/29/2025","8/29/2025","9/2/2025","TMC","TMC 2/20/2026 Call $14.00","STO","20","$1.20","$2,399.10"
"8/29/2025","8/29/2025","9/2/2025","TMC","TMC 2/20/2026 Call $14.00","STO","20","$1.20","$2,399.10"
"8/28/2025","8/28/2025","9/2/2025","AMD","AMD 10/17/2025 Call $170.00","STO","1","$4.25","$424.95"
"8/28/2025","8/28/2025","9/2/2025","AMD","AMD 10/17/2025 Put $140.00","BTC","1","$1.23","($123.04)"
'''
        
        # Create temporary CSV file
        self.temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv')
        self.temp_file.write(self.sample_csv_content)
        self.temp_file.close()
    
    def tearDown(self):
        """Clean up temporary file."""
        os.unlink(self.temp_file.name)
    
    def test_get_options_transactions(self):
        """Test that options transactions are correctly identified."""
        transactions = get_options_transactions(self.temp_file.name)
        
        # Should find all options transactions
        self.assertEqual(len(transactions), 17)  # 18 lines - 1 header = 17 transactions
        
        # Check that only options transactions are included
        for txn in transactions:
            desc = txn.get('Description', '')
            trans_code = txn.get('Trans Code', '')
            self.assertTrue(
                'Call' in desc or 'Put' in desc or trans_code in ['BTC', 'STO', 'OASGN'],
                f"Transaction should be options-related: {txn}"
            )
    
    def test_is_call_option(self):
        """Test call option detection."""
        self.assertTrue(is_call_option("TSLA 11/21/2025 Call $550.00"))
        self.assertFalse(is_call_option("TSLA 11/21/2025 Put $550.00"))
        self.assertFalse(is_call_option("TSLA Common Stock"))
        self.assertFalse(is_call_option(""))
    
    def test_is_put_option(self):
        """Test put option detection."""
        self.assertTrue(is_put_option("TSLA 11/21/2025 Put $550.00"))
        self.assertFalse(is_put_option("TSLA 11/21/2025 Call $550.00"))
        self.assertFalse(is_put_option("TSLA Common Stock"))
        self.assertFalse(is_put_option(""))
    
    def test_deduplicate_transactions(self):
        """Test transaction deduplication."""
        transactions = get_options_transactions(self.temp_file.name)
        unique_txns = deduplicate_transactions(transactions)
        
        # Should have fewer transactions after deduplication
        self.assertLess(len(unique_txns), len(transactions))
        
        # Check that duplicates are removed
        seen_keys = set()
        for txn in unique_txns:
            key = (
                txn.get('Activity Date', ''),
                txn.get('Instrument', ''),
                txn.get('Trans Code', ''),
                txn.get('Quantity', ''),
                txn.get('Price', ''),
                txn.get('Description', '')
            )
            self.assertNotIn(key, seen_keys, f"Duplicate transaction found: {txn}")
            seen_keys.add(key)
    
    def test_detect_rolls_basic(self):
        """Test basic roll detection."""
        transactions = get_options_transactions(self.temp_file.name)
        unique_txns = deduplicate_transactions(transactions)
        rolls = detect_rolls(unique_txns)
        
        # Should detect some rolls
        self.assertGreater(len(rolls), 0)
        
        # Check roll structure
        for roll in rolls:
            self.assertIn('type', roll)
            self.assertEqual(roll['type'], 'ROLL')
            self.assertIn('ticker', roll)
            self.assertIn('date', roll)
            self.assertIn('quantity', roll)
            self.assertIn('btc_desc', roll)
            self.assertIn('sto_desc', roll)
            self.assertIn('net_credit', roll)
    
    def test_detect_rolls_tsla(self):
        """Test TSLA roll detection specifically."""
        transactions = get_options_transactions(self.temp_file.name)
        unique_txns = deduplicate_transactions(transactions)
        rolls = detect_rolls(unique_txns)
        
        # Find TSLA rolls
        tsla_rolls = [roll for roll in rolls if roll['ticker'] == 'TSLA']
        
        # Should detect the TSLA roll: $550 Call -> $600 Call
        self.assertEqual(len(tsla_rolls), 1)
        
        tsla_roll = tsla_rolls[0]
        self.assertEqual(tsla_roll['date'], '10/7/2025')
        self.assertEqual(tsla_roll['quantity'], 1)
        self.assertIn('550.00', tsla_roll['btc_desc'])
        self.assertIn('600.00', tsla_roll['sto_desc'])
        
        # Net credit should be positive (profitable roll)
        self.assertGreater(tsla_roll['net_credit'], 0)
    
    def test_detect_rolls_nvda(self):
        """Test NVDA roll detection (Put roll)."""
        transactions = get_options_transactions(self.temp_file.name)
        unique_txns = deduplicate_transactions(transactions)
        rolls = detect_rolls(unique_txns)
        
        # Find NVDA rolls
        nvda_rolls = [roll for roll in rolls if roll['ticker'] == 'NVDA']
        
        # Should detect the NVDA put roll: $165 Put -> $170 Put
        self.assertEqual(len(nvda_rolls), 1)
        
        nvda_roll = nvda_rolls[0]
        self.assertEqual(nvda_roll['date'], '10/6/2025')
        self.assertEqual(nvda_roll['quantity'], 2)
        self.assertIn('165.00', nvda_roll['btc_desc'])
        self.assertIn('170.00', nvda_roll['sto_desc'])
    
    def test_detect_rolls_pltr_deduplication(self):
        """Test PLTR roll detection with deduplication."""
        transactions = get_options_transactions(self.temp_file.name)
        unique_txns = deduplicate_transactions(transactions)
        rolls = detect_rolls(unique_txns)
        
        # Find PLTR rolls
        pltr_rolls = [roll for roll in rolls if roll['ticker'] == 'PLTR']
        
        # Should detect only 1 roll despite duplicate transactions
        self.assertEqual(len(pltr_rolls), 1)
        
        pltr_roll = pltr_rolls[0]
        self.assertEqual(pltr_roll['date'], '9/22/2025')
        self.assertEqual(pltr_roll['quantity'], 1)
    
    def test_detect_rolls_tmc_deduplication(self):
        """Test TMC roll detection with multiple duplicates."""
        transactions = get_options_transactions(self.temp_file.name)
        unique_txns = deduplicate_transactions(transactions)
        rolls = detect_rolls(unique_txns)
        
        # Find TMC rolls
        tmc_rolls = [roll for roll in rolls if roll['ticker'] == 'TMC']
        
        # Should detect only 1 roll despite 4 duplicate transactions (2 BTC + 2 STO)
        self.assertEqual(len(tmc_rolls), 1)
        
        tmc_roll = tmc_rolls[0]
        self.assertEqual(tmc_roll['date'], '8/29/2025')
        self.assertEqual(tmc_roll['quantity'], 20)
    
    def test_detect_rolls_hood_put_roll(self):
        """Test HOOD put roll detection."""
        transactions = get_options_transactions(self.temp_file.name)
        unique_txns = deduplicate_transactions(transactions)
        rolls = detect_rolls(unique_txns)
        
        # Find HOOD rolls
        hood_rolls = [roll for roll in rolls if roll['ticker'] == 'HOOD']
        
        # Should detect 1 put roll (Call roll has OASGN which doesn't match BTC+STO pattern)
        self.assertEqual(len(hood_rolls), 1)
        
        hood_roll = hood_rolls[0]
        self.assertEqual(hood_roll['date'], '9/5/2025')
        self.assertIn('Put', hood_roll['btc_desc'])
        self.assertIn('Put', hood_roll['sto_desc'])
    
    def test_no_false_positives(self):
        """Test that non-roll transactions are not detected as rolls."""
        transactions = get_options_transactions(self.temp_file.name)
        unique_txns = deduplicate_transactions(transactions)
        rolls = detect_rolls(unique_txns)
        
        # AMD should not have any rolls (mismatched quantities/types)
        amd_rolls = [roll for roll in rolls if roll['ticker'] == 'AMD']
        self.assertEqual(len(amd_rolls), 0)
    
    def test_roll_calculation_accuracy(self):
        """Test that roll credit/debit calculations are accurate."""
        transactions = get_options_transactions(self.temp_file.name)
        unique_txns = deduplicate_transactions(transactions)
        rolls = detect_rolls(unique_txns)
        
        # Test TSLA roll calculation
        tsla_rolls = [roll for roll in rolls if roll['ticker'] == 'TSLA']
        self.assertEqual(len(tsla_rolls), 1)
        
        tsla_roll = tsla_rolls[0]
        expected_btc_amount = -875.04  # BTC is negative
        expected_sto_amount = 1199.10   # STO is positive
        expected_net = expected_sto_amount + expected_btc_amount
        
        self.assertAlmostEqual(tsla_roll['net_credit'], expected_net, places=2)


if __name__ == '__main__':
    # Run the tests
    unittest.main(verbosity=2)
