"""Unit tests for analysis service functions."""

import unittest
from decimal import Decimal

from src.rollchain.services.analysis import (
    is_open_chain,
    calculate_realized_pnl,
    calculate_target_price_range,
    filter_open_chains,
)


class TestAnalysisService(unittest.TestCase):
    """Test chain analysis functions."""

    def test_is_open_chain_by_status_open(self):
        """Test open chain detection by status."""
        chain = {'status': 'OPEN'}
        self.assertTrue(is_open_chain(chain))

    def test_is_open_chain_by_status_closed(self):
        """Test closed chain detection by status."""
        chain = {'status': 'CLOSED'}
        self.assertFalse(is_open_chain(chain))

    def test_is_open_chain_by_last_transaction_sto(self):
        """Test open chain detection by last transaction code."""
        chain = {
            'status': 'UNKNOWN',
            'transactions': [
                {'Trans Code': 'STO'}
            ]
        }
        self.assertTrue(is_open_chain(chain))

    def test_is_open_chain_by_last_transaction_bto(self):
        """Test open chain detection by last transaction code."""
        chain = {
            'status': 'UNKNOWN',
            'transactions': [
                {'Trans Code': 'BTO'}
            ]
        }
        self.assertTrue(is_open_chain(chain))

    def test_is_open_chain_by_last_transaction_btc(self):
        """Test closed chain detection by last transaction code."""
        chain = {
            'status': 'UNKNOWN',
            'transactions': [
                {'Trans Code': 'BTC'}
            ]
        }
        self.assertFalse(is_open_chain(chain))

    def test_is_open_chain_no_transactions(self):
        """Test chain with no transactions."""
        chain = {'status': 'UNKNOWN', 'transactions': []}
        self.assertFalse(is_open_chain(chain))

    def test_calculate_realized_pnl(self):
        """Test realized P&L calculation."""
        chain = {
            'total_credits': Decimal('500.00'),
            'total_debits': Decimal('400.00'),
            'total_fees': Decimal('0.16')
        }
        result = calculate_realized_pnl(chain)
        expected = Decimal('500.00') - Decimal('400.00') - Decimal('0.16')
        self.assertEqual(result, expected)

    def test_calculate_realized_pnl_missing_values(self):
        """Test realized P&L calculation with missing values."""
        chain = {}
        result = calculate_realized_pnl(chain)
        self.assertEqual(result, Decimal('0'))

    def test_calculate_target_price_range_valid(self):
        """Test target price range calculation."""
        chain = {
            'breakeven_price': Decimal('100.00'),
            'net_contracts': 1,
            'total_credits': Decimal('500.00'),
            'total_debits': Decimal('400.00'),
            'total_fees': Decimal('0.16')
        }
        bounds = (Decimal('0.5'), Decimal('0.7'))
        
        result = calculate_target_price_range(chain, bounds)
        
        # Expected: breakeven + (realized_pnl/100) * bounds
        # realized_pnl = 500 - 400 - 0.16 = 99.84
        # per_share = 99.84 / 100 = 0.9984
        # lower = 100 + (0.9984 * 0.5) = 100.50
        # upper = 100 + (0.9984 * 0.7) = 100.70
        self.assertIsNotNone(result)
        low, high = result
        self.assertAlmostEqual(float(low), 100.50, places=1)
        self.assertAlmostEqual(float(high), 100.70, places=1)

    def test_calculate_target_price_range_no_breakeven(self):
        """Test target price range with no breakeven."""
        chain = {
            'net_contracts': 1,
            'total_credits': Decimal('500.00'),
            'total_debits': Decimal('400.00')
        }
        bounds = (Decimal('0.5'), Decimal('0.7'))
        
        result = calculate_target_price_range(chain, bounds)
        self.assertIsNone(result)

    def test_calculate_target_price_range_no_contracts(self):
        """Test target price range with no contracts."""
        chain = {
            'breakeven_price': Decimal('100.00'),
            'net_contracts': 0,
            'total_credits': Decimal('500.00'),
            'total_debits': Decimal('400.00')
        }
        bounds = (Decimal('0.5'), Decimal('0.7'))
        
        result = calculate_target_price_range(chain, bounds)
        self.assertIsNone(result)

    def test_calculate_target_price_range_negative_realized(self):
        """Test target price range with negative realized P&L."""
        chain = {
            'breakeven_price': Decimal('100.00'),
            'net_contracts': 1,
            'total_credits': Decimal('300.00'),
            'total_debits': Decimal('400.00'),
            'total_fees': Decimal('0.16')
        }
        bounds = (Decimal('0.5'), Decimal('0.7'))
        
        result = calculate_target_price_range(chain, bounds)
        self.assertIsNone(result)

    def test_calculate_target_price_range_short_position(self):
        """Test target price range for short position."""
        chain = {
            'breakeven_price': Decimal('100.00'),
            'net_contracts': -1,  # Short position
            'total_credits': Decimal('500.00'),
            'total_debits': Decimal('400.00'),
            'total_fees': Decimal('0.16')
        }
        bounds = (Decimal('0.5'), Decimal('0.7'))
        
        result = calculate_target_price_range(chain, bounds)
        
        # For short positions, prices should be below breakeven
        self.assertIsNotNone(result)
        low, high = result
        self.assertLess(low, Decimal('100.00'))
        self.assertLess(high, Decimal('100.00'))

    def test_filter_open_chains(self):
        """Test filtering chains to open positions only."""
        chains = [
            {'status': 'OPEN'},
            {'status': 'CLOSED'},
            {'status': 'UNKNOWN', 'transactions': [{'Trans Code': 'STO'}]},
            {'status': 'UNKNOWN', 'transactions': [{'Trans Code': 'BTC'}]},
        ]
        
        result = filter_open_chains(chains)
        
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['status'], 'OPEN')
        self.assertEqual(result[1]['status'], 'UNKNOWN')

    def test_filter_open_chains_empty(self):
        """Test filtering empty chain list."""
        result = filter_open_chains([])
        self.assertEqual(len(result), 0)


if __name__ == '__main__':
    unittest.main()
