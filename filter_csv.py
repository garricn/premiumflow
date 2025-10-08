#!/usr/bin/env python3
"""
Filter CSV to extract only TSLA call transactions
"""

import csv
import sys

def filter_tsla_calls(input_file, output_file):
    """Filter CSV to only include TSLA call transactions."""
    tsla_calls = []
    
    # Read the input CSV
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        
        for row in reader:
            # Check if it's a TSLA call transaction
            instrument = row.get('Instrument', '') or ''
            instrument = instrument.strip().upper()
            description = row.get('Description', '') or ''
            
            if instrument == 'TSLA' and 'Call' in description:
                tsla_calls.append(row)
    
    # Write the filtered data to output file
    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(tsla_calls)
    
    print(f"Filtered {len(tsla_calls)} TSLA call transactions to {output_file}")

if __name__ == '__main__':
    input_file = 'all_transactions.csv'
    output_file = 'tsla_calls.csv'
    
    try:
        filter_tsla_calls(input_file, output_file)
    except FileNotFoundError:
        print(f"Error: File '{input_file}' not found")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
