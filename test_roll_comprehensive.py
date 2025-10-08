#!/usr/bin/env python3
"""
Comprehensive tests for roll detection logic
"""

import unittest
import tempfile
import os
import csv
from datetime import datetime
from typing import List, Dict

# Import the same functions
def parse_date(date_str: str) -> datetime:
    return datetime.strptime(date_str, '%m/%d/%Y')

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

def detect_rolls(transactions: List[Dict[str, str]]) -> List[Dict[str, str]]:
    rolls = []
    unique_txns = deduplicate_transactions(transactions)
    
    by_ticker_date = {}
    for txn in unique_txns:
        ticker = txn.get('Instrument', '').strip()
        date = txn.get('Activity Date', '')
        key = (ticker, date)
        if key not in by_ticker_date:
            by_ticker_date[key] = []
        by_ticker_date[key].append(txn)
    
    for (ticker, date), txns in by_ticker_date.items():
        if len(txns) < 2:
            continue
            
        txns.sort(key=lambda x: x.get('Trans Code', ''))
        btc_txns = [txn for txn in txns if txn.get('Trans Code') == 'BTC']
        sto_txns = [txn for txn in txns if txn.get('Trans Code') == 'STO']
        
        for btc in btc_txns:
            btc_qty = int(btc.get('Quantity', '0'))
            btc_desc = btc.get('Description', '')
            
            for sto in sto_txns:
                sto_qty = int(sto.get('Quantity', '0'))
                sto_desc = sto.get('Description', '')
                
                if (btc_qty == sto_qty and btc_qty > 0 and 
                    (('Call' in btc_desc and 'Call' in sto_desc) or 
                     ('Put' in btc_desc and 'Put' in sto_desc))):
                    
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
                        'net_credit': net_credit,
                        'btc_amount': btc_amount,
                        'sto_amount': sto_amount
                    }
                    rolls.append(roll_info)
    
    return rolls


class TestRollDetectionComprehensive(unittest.TestCase):
    
    def test_real_data_tsla_calls(self):
        """Test with real TSLA call data from the CSV."""
        # Use the actual CSV file
        csv_file = '03669093-403a-582f-a7e8-785ca4624477.csv'
        
        if not os.path.exists(csv_file):
            self.skipTest(f"CSV file {csv_file} not found")
        
        transactions = get_options_transactions(csv_file)
        
        # Filter for TSLA calls only
        tsla_calls = [txn for txn in transactions 
                     if txn.get('Instrument') == 'TSLA' and 'Call' in txn.get('Description', '')]
        
        print(f"Found {len(tsla_calls)} TSLA call transactions")
        
        # Test roll detection
        unique_txns = deduplicate_transactions(tsla_calls)
        rolls = detect_rolls(unique_txns)
        
        print(f"Detected {len(rolls)} TSLA call rolls:")
        for roll in rolls:
            print(f"  {roll['date']}: {roll['quantity']} contracts - {roll['btc_desc']} → {roll['sto_desc']} (${roll['net_credit']:.2f})")
        
        # Should detect 2 rolls based on our earlier analysis
        self.assertEqual(len(rolls), 2, f"Expected 2 TSLA call rolls, got {len(rolls)}")
        
        # Check specific rolls
        dates = [roll['date'] for roll in rolls]
        self.assertIn('9/22/2025', dates)
        
        # Check that all rolls are profitable
        for roll in rolls:
            self.assertGreater(roll['net_credit'], 0, f"Roll should be profitable: {roll}")
    
    def test_real_data_tmc_rolls(self):
        """Test with real TMC data to check deduplication."""
        csv_file = '03669093-403a-582f-a7e8-785ca4624477.csv'
        
        if not os.path.exists(csv_file):
            self.skipTest(f"CSV file {csv_file} not found")
        
        transactions = get_options_transactions(csv_file)
        
        # Filter for TMC calls only
        tmc_calls = [txn for txn in transactions 
                    if txn.get('Instrument') == 'TMC' and 'Call' in txn.get('Description', '')]
        
        print(f"Found {len(tmc_calls)} TMC call transactions")
        
        # Show raw transactions
        print("Raw TMC transactions:")
        for txn in tmc_calls:
            print(f"  {txn['Activity Date']}: {txn['Trans Code']} {txn['Quantity']} @ {txn['Price']} - {txn['Description']}")
        
        # Test deduplication
        unique_txns = deduplicate_transactions(tmc_calls)
        print(f"After deduplication: {len(unique_txns)} transactions")
        
        # Test roll detection
        rolls = detect_rolls(unique_txns)
        
        print(f"Detected {len(rolls)} TMC call rolls:")
        for roll in rolls:
            print(f"  {roll['date']}: {roll['quantity']} contracts - {roll['btc_desc']} → {roll['sto_desc']} (${roll['net_credit']:.2f})")
        
        # Should detect 1 roll despite multiple duplicate transactions
        self.assertEqual(len(rolls), 1, f"Expected 1 TMC call roll, got {len(rolls)}")
        
        if rolls:
            roll = rolls[0]
            self.assertEqual(roll['date'], '10/7/2025')
            self.assertEqual(roll['quantity'], 20)


if __name__ == '__main__':
    unittest.main(verbosity=2)
