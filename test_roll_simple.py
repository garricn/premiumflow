#!/usr/bin/env python3
"""
Simple unit tests for roll detection logic
"""

import unittest
import tempfile
import os
import sys
import csv
from datetime import datetime
from typing import List, Dict

# Copy the functions we need to test directly
def parse_date(date_str: str) -> datetime:
    """Parse date string in M/D/YYYY format."""
    return datetime.strptime(date_str, '%m/%d/%Y')

def is_options_transaction(row: Dict[str, str]) -> bool:
    """Determine if a transaction is options-related."""
    trans_code = row.get('Trans Code', '').strip()
    description = row.get('Description', '').strip()
    
    options_codes = {'BTC', 'STO', 'OASGN'}
    if trans_code in options_codes:
        return True
    
    if 'Call' in description or 'Put' in description:
        return True
    
    return False

def get_options_transactions(csv_file: str) -> List[Dict[str, str]]:
    """Extract all options transactions from CSV file."""
    options_txns = []
    
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not row.get('Activity Date'):
                continue
                
            if is_options_transaction(row):
                options_txns.append(row)
    
    return options_txns

def is_call_option(description: str) -> bool:
    """Check if the option is a Call."""
    return 'Call' in description

def is_put_option(description: str) -> bool:
    """Check if the option is a Put."""
    return 'Put' in description

def deduplicate_transactions(transactions: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """Remove duplicate transactions with identical details."""
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

def detect_rolls(transactions: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """Detect potential rolls based on Robinhood pattern."""
    rolls = []
    
    # Deduplicate transactions first
    unique_txns = deduplicate_transactions(transactions)
    
    # Group by ticker and date
    by_ticker_date = {}
    for txn in unique_txns:
        ticker = txn.get('Instrument', '').strip()
        date = txn.get('Activity Date', '')
        key = (ticker, date)
        if key not in by_ticker_date:
            by_ticker_date[key] = []
        by_ticker_date[key].append(txn)
    
    # Look for BTC+STO patterns
    for (ticker, date), txns in by_ticker_date.items():
        if len(txns) < 2:
            continue
            
        txns.sort(key=lambda x: x.get('Trans Code', ''))
        
        btc_txns = [txn for txn in txns if txn.get('Trans Code') == 'BTC']
        sto_txns = [txn for txn in txns if txn.get('Trans Code') == 'STO']
        
        # Look for matching quantities
        for btc in btc_txns:
            btc_qty = int(btc.get('Quantity', '0'))
            btc_desc = btc.get('Description', '')
            
            for sto in sto_txns:
                sto_qty = int(sto.get('Quantity', '0'))
                sto_desc = sto.get('Description', '')
                
                # Check if quantities match and both are same option type
                if (btc_qty == sto_qty and btc_qty > 0 and 
                    (('Call' in btc_desc and 'Call' in sto_desc) or 
                     ('Put' in btc_desc and 'Put' in sto_desc))):
                    
                    # Calculate net credit/debit
                    btc_amount = float(btc.get('Amount', '0').replace('$', '').replace(',', '').replace('(', '-').replace(')', ''))
                    sto_amount = float(sto.get('Amount', '0').replace('$', '').replace(',', '').replace('(', '-').replace(')', ''))
                    net_credit = sto_amount + btc_amount
                    
                    roll_info = {
                        'type': 'ROLL',
                        'ticker': ticker,
                        'date': date,
                        'quantity': btc_qty,
                        'btc_desc': btc_desc,
                        'sto_desc': sto_desc,
                        'btc_price': btc.get('Price', ''),
                        'sto_price': sto.get('Price', ''),
                        'net_credit': net_credit,
                        'btc_amount': btc_amount,
                        'sto_amount': sto_amount
                    }
                    rolls.append(roll_info)
    
    return rolls


class TestRollDetection(unittest.TestCase):
    
    def setUp(self):
        """Set up test data."""
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
        
        self.temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv')
        self.temp_file.write(self.sample_csv_content)
        self.temp_file.close()
    
    def tearDown(self):
        """Clean up temporary file."""
        os.unlink(self.temp_file.name)
    
    def test_detect_rolls_tmc_deduplication(self):
        """Test TMC roll detection with multiple duplicates - this is the bug we're looking for."""
        transactions = get_options_transactions(self.temp_file.name)
        unique_txns = deduplicate_transactions(transactions)
        rolls = detect_rolls(unique_txns)
        
        # Find TMC rolls
        tmc_rolls = [roll for roll in rolls if roll['ticker'] == 'TMC']
        
        print(f"TMC rolls detected: {len(tmc_rolls)}")
        for roll in tmc_rolls:
            print(f"  {roll['date']}: {roll['quantity']} contracts - {roll['btc_desc']} → {roll['sto_desc']} (${roll['net_credit']:.2f})")
        
        # Should detect only 1 roll despite 4 duplicate transactions (2 BTC + 2 STO)
        self.assertEqual(len(tmc_rolls), 1, f"Expected 1 TMC roll, got {len(tmc_rolls)}")
        
        tmc_roll = tmc_rolls[0]
        self.assertEqual(tmc_roll['date'], '8/29/2025')
        self.assertEqual(tmc_roll['quantity'], 20)
    
    def test_detect_rolls_pltr_deduplication(self):
        """Test PLTR roll detection with duplicates."""
        transactions = get_options_transactions(self.temp_file.name)
        unique_txns = deduplicate_transactions(transactions)
        rolls = detect_rolls(unique_txns)
        
        # Find PLTR rolls
        pltr_rolls = [roll for roll in rolls if roll['ticker'] == 'PLTR']
        
        print(f"PLTR rolls detected: {len(pltr_rolls)}")
        for roll in pltr_rolls:
            print(f"  {roll['date']}: {roll['quantity']} contracts - {roll['btc_desc']} → {roll['sto_desc']} (${roll['net_credit']:.2f})")
        
        # Should detect only 1 roll despite duplicate transactions
        self.assertEqual(len(pltr_rolls), 1, f"Expected 1 PLTR roll, got {len(pltr_rolls)}")


if __name__ == '__main__':
    unittest.main(verbosity=2)
