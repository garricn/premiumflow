#!/usr/bin/env python3
"""
Unit test for the specific TSLA multi-leg roll chain
"""

import unittest
import tempfile
import os
import csv
from typing import List, Dict

# Import the same functions
def is_options_transaction(row: Dict[str, str]) -> bool:
    trans_code = row.get('Trans Code', '').strip()
    description = row.get('Description', '').strip()
    options_codes = {'BTC', 'STO', 'OASGN'}
    if trans_code in options_codes:
        return True
    if 'Call' in description or 'Put' in description:
        return True
    return False

def get_options_transactions(csv_file: str) -> List[Dict[str, str]]:
    options_txns = []
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not row.get('Activity Date'):
                continue
            if is_options_transaction(row):
                options_txns.append(row)
    return options_txns

def deduplicate_transactions(transactions: List[Dict[str, str]]) -> List[Dict[str, str]]:
    seen = set()
    unique_txns = []
    for txn in transactions:
        key = (
            txn.get('Activity Date', ''),
            txn.get('Instrument', ''),
            txn.get('Trans Code', ''),
            txn.get('Quantity', ''),
            txn.get('Price', ''),
            txn.get('Description', '')
        )
        if key not in seen:
            seen.add(key)
            unique_txns.append(txn)
    return unique_txns

# Import the real detect_rolls function from the roll module
from roll import detect_rolls


class TestTSLARollChain(unittest.TestCase):
    
    def setUp(self):
        """Set up the specific TSLA roll chain data."""
        self.tsla_roll_chain_csv = '''"Activity Date","Process Date","Settle Date","Instrument","Description","Trans Code","Quantity","Price","Amount"
"9/12/2025","9/12/2025","9/15/2025","TSLA","TSLA 10/17/2025 Call $515.00","STO","1","$3.00","$299.96"
"9/22/2025","9/22/2025","9/23/2025","TSLA","TSLA 10/17/2025 Call $515.00","BTC","1","$7.30","($730.04)"
"9/22/2025","9/22/2025","9/23/2025","TSLA","TSLA 11/21/2025 Call $550.00","STO","1","$15.75","$1,574.96"
"10/8/2025","10/8/2025","10/9/2025","TSLA","TSLA 11/21/2025 Call $550.00","BTC","1","$8.75","($875.04)"
'''
        
        self.temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv')
        self.temp_file.write(self.tsla_roll_chain_csv)
        self.temp_file.close()
    
    def tearDown(self):
        """Clean up temporary file."""
        os.unlink(self.temp_file.name)
    
    def test_tsla_roll_chain_analysis(self):
        """Test analysis of the complete TSLA roll chain - SHOULD FAIL."""
        transactions = get_options_transactions(self.temp_file.name)
        unique_txns = deduplicate_transactions(transactions)
        rolls = detect_rolls(unique_txns)
        
        print("\n=== TSLA Roll Chain Analysis ===")
        print(f"Total transactions: {len(transactions)}")
        print(f"Unique transactions: {len(unique_txns)}")
        print(f"Detected rolls: {len(rolls)}")
        
        print("\nAll transactions:")
        for txn in unique_txns:
            date = txn.get('Activity Date', '')
            action = txn.get('Trans Code', '')
            desc = txn.get('Description', '')
            amount = txn.get('Amount', '')
            print(f"  {date}: {action} - {desc} - {amount}")
        
        print("\nDetected rolls:")
        for roll in rolls:
            if roll['type'] == 'ROLL':
                print(f"  {roll['date']}: {roll['quantity']} contracts - {roll['btc_desc']} → {roll['sto_desc']} (${roll['net_credit']:.2f})")
            elif roll['type'] == 'STRATEGY':
                print(f"  STRATEGY {roll['date']}-{roll['end_date']}: {roll['quantity']} contracts - {roll['initial_desc']} → {roll['final_desc']} (${roll['net_credit']:.2f})")
        
        # SUCCESS: Enhanced logic now detects both same-day rolls AND complete strategies
        self.assertEqual(len(rolls), 2, "Should detect 2 rolls: same-day roll + complete strategy")
        
        # Check for the complete roll chain (9/12 → 9/22 → 10/8)
        roll_dates = [roll['date'] for roll in rolls]
        strategy_rolls = [roll for roll in rolls if roll['type'] == 'STRATEGY']
        
        self.assertIn('9/22/2025', roll_dates, "Should detect same-day roll")
        self.assertEqual(len(strategy_rolls), 1, "Should detect 1 complete strategy roll")
        
        if strategy_rolls:
            strategy = strategy_rolls[0]
            self.assertEqual(strategy['date'], '9/12/2025', "Strategy should start on 9/12/2025")
            self.assertEqual(strategy['end_date'], '10/8/2025', "Strategy should end on 10/8/2025")
        
        # Check for complete strategy P&L
        total_strategy_pnl = 299.96 + 1574.96 - 730.04 - 875.04  # $269.84
        strategy_roll = [roll for roll in rolls if roll.get('type') == 'STRATEGY']
        self.assertEqual(len(strategy_roll), 1, "BUG: Should detect complete strategy roll")
        
        if strategy_roll:
            strategy = strategy_roll[0]
            self.assertAlmostEqual(strategy['net_credit'], total_strategy_pnl, places=2, 
                                 msg="BUG: Strategy P&L calculation incorrect")
    
    def test_roll_chain_manual_verification(self):
        """Manually verify the roll chain components."""
        transactions = get_options_transactions(self.temp_file.name)
        
        # Group by date for analysis
        by_date = {}
        for txn in transactions:
            date = txn.get('Activity Date', '')
            if date not in by_date:
                by_date[date] = []
            by_date[date].append(txn)
        
        print("\n=== Manual Roll Chain Verification ===")
        
        # 9/12/2025: Initial covered call write
        sept_12 = by_date.get('9/12/2025', [])
        self.assertEqual(len(sept_12), 1)
        self.assertEqual(sept_12[0]['Trans Code'], 'STO')
        self.assertIn('515.00', sept_12[0]['Description'])
        print("✓ 9/12: STO $515 Call - Initial position opened")
        
        # 9/22/2025: Roll up and out
        sept_22 = by_date.get('9/22/2025', [])
        self.assertEqual(len(sept_22), 2)
        
        btc_txn = [txn for txn in sept_22 if txn['Trans Code'] == 'BTC'][0]
        sto_txn = [txn for txn in sept_22 if txn['Trans Code'] == 'STO'][0]
        
        self.assertIn('515.00', btc_txn['Description'])
        self.assertIn('550.00', sto_txn['Description'])
        print("✓ 9/22: BTC $515 Call + STO $550 Call - Rolled up and out")
        
        # 10/8/2025: Final close
        oct_8 = by_date.get('10/8/2025', [])
        self.assertEqual(len(oct_8), 1)
        self.assertEqual(oct_8[0]['Trans Code'], 'BTC')
        self.assertIn('550.00', oct_8[0]['Description'])
        print("✓ 10/8: BTC $550 Call - Final position closed")
        
        # Calculate total P&L
        total_credit = 299.96 + 1574.96  # Initial STO + Roll STO
        total_debit = 730.04 + 875.04    # Roll BTC + Final BTC
        net_pnl = total_credit - total_debit
        
        print(f"\n=== Total P&L Analysis ===")
        print(f"Total Credits: ${total_credit:,.2f}")
        print(f"Total Debits:  ${total_debit:,.2f}")
        print(f"Net P&L:       ${net_pnl:,.2f}")
        
        # Should be profitable
        self.assertGreater(net_pnl, 0, "Roll chain should be profitable")
    
    def test_roll_chain_limitations(self):
        """Test what our current logic misses."""
        transactions = get_options_transactions(self.temp_file.name)
        unique_txns = deduplicate_transactions(transactions)
        rolls = detect_rolls(unique_txns)
        
        print("\n=== Current Logic Limitations ===")
        print("✓ Detects: Same-day rolls (9/22/2025)")
        print("✗ Misses: Multi-day roll strategy (9/12 → 9/22 → 10/8)")
        print("✗ Misses: Complete position lifecycle")
        print("✗ Misses: Total strategy P&L")
        
        # Enhanced logic now works correctly
        self.assertEqual(len(rolls), 2, "Now detects 2 rolls: same-day + strategy")
        
        # We should ideally detect the complete roll strategy
        print(f"\nIdeal detection would show:")
        print(f"  - Complete roll chain: $515 (9/12) → $550 (9/22) → Close (10/8)")
        print(f"  - Total strategy P&L: ${269.84:.2f}")


if __name__ == '__main__':
    unittest.main(verbosity=2)
