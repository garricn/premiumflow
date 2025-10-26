"""Unit tests for display service functions."""

import unittest
from decimal import Decimal
from unittest.mock import patch

from src.rollchain.services.display import (
    format_currency,
    format_breakeven,
    format_percent,
    format_price_range,
    format_target_close_prices,
    ensure_display_name,
    format_option_display,
    prepare_transactions_for_display,
    prepare_chain_display,
    format_net_pnl,
    format_realized_pnl,
    calculate_target_price_range,
)


class TestDisplayService(unittest.TestCase):
    """Test display formatting functions."""

    def test_format_currency_positive(self):
        """Test formatting positive currency values."""
        self.assertEqual(format_currency(Decimal('123.45')), '$123.45')
        self.assertEqual(format_currency(Decimal('1000.00')), '$1,000.00')
        self.assertEqual(format_currency(Decimal('0.01')), '$0.01')

    def test_format_currency_negative(self):
        """Test formatting negative currency values."""
        self.assertEqual(format_currency(Decimal('-123.45')), '-$123.45')
        self.assertEqual(format_currency(Decimal('-1000.00')), '-$1,000.00')

    def test_format_currency_none(self):
        """Test formatting None currency values."""
        self.assertEqual(format_currency(None), '--')

    def test_format_breakeven_open_chain(self):
        """Test formatting breakeven for open chain."""
        chain = {
            'status': 'OPEN',
            'breakeven_price': Decimal('100.50'),
            'breakeven_direction': 'above'
        }
        self.assertEqual(format_breakeven(chain), '$100.50 above')

    def test_format_breakeven_closed_chain(self):
        """Test formatting breakeven for closed chain."""
        chain = {'status': 'CLOSED'}
        self.assertEqual(format_breakeven(chain), '--')

    def test_format_breakeven_no_price(self):
        """Test formatting breakeven when no price available."""
        chain = {'status': 'OPEN', 'breakeven_price': None}
        self.assertEqual(format_breakeven(chain), '--')

    def test_format_percent(self):
        """Test formatting percentage values."""
        self.assertEqual(format_percent(Decimal('0.5')), '50%')
        self.assertEqual(format_percent(Decimal('0.25')), '25%')
        self.assertEqual(format_percent(Decimal('0.1234')), '12.34%')
        self.assertEqual(format_percent(Decimal('1.0')), '100%')

    def test_format_price_range(self):
        """Test formatting price range."""
        range_tuple = (Decimal('100.00'), Decimal('150.00'))
        self.assertEqual(format_price_range(range_tuple), '$100.00 - $150.00')
        self.assertEqual(format_price_range(None), '--')

    def test_format_target_close_prices(self):
        """Test formatting target close prices."""
        prices = [Decimal('100.00'), Decimal('150.00'), Decimal('200.00')]
        self.assertEqual(format_target_close_prices(prices), '$100.00, $150.00, $200.00')
        self.assertEqual(format_target_close_prices(None), '--')
        self.assertEqual(format_target_close_prices([]), '--')

    def test_ensure_display_name_with_display_name(self):
        """Test display name when already present."""
        chain = {'display_name': 'TSLA $500 CALL'}
        self.assertEqual(ensure_display_name(chain), 'TSLA $500 CALL')

    def test_ensure_display_name_with_symbol_strike(self):
        """Test display name generation from symbol and strike."""
        chain = {
            'symbol': 'TSLA',
            'strike': Decimal('500'),
            'option_label': 'CALL'
        }
        self.assertEqual(ensure_display_name(chain), 'TSLA $500 CALL')

    def test_ensure_display_name_decimal_strike(self):
        """Test display name with decimal strike."""
        chain = {
            'symbol': 'TSLA',
            'strike': Decimal('500.50'),
            'option_label': 'CALL'
        }
        self.assertEqual(ensure_display_name(chain), 'TSLA $500.50 CALL')

    def test_ensure_display_name_fallback_to_symbol(self):
        """Test display name fallback to symbol."""
        chain = {'symbol': 'TSLA'}
        self.assertEqual(ensure_display_name(chain), 'TSLA')

    def test_format_option_display_with_parsed(self):
        """Test option display formatting with parsed descriptor."""
        from src.rollchain.services.options import OptionDescriptor
        
        parsed = OptionDescriptor(
            symbol='TSLA',
            expiration='11/21/2025',
            option_type='Call',
            strike=Decimal('500.00')
        )
        formatted, expiration = format_option_display(parsed, 'fallback')
        
        self.assertEqual(formatted, 'TSLA $500.00 Call')
        self.assertEqual(expiration, '11/21/2025')

    def test_format_option_display_without_parsed(self):
        """Test option display formatting without parsed descriptor."""
        formatted, expiration = format_option_display(None, 'fallback description')
        
        self.assertEqual(formatted, 'fallback description')
        self.assertEqual(expiration, '')

    @patch('src.rollchain.services.targets.compute_target_close_prices')
    @patch('src.rollchain.services.options.parse_option_description')
    def test_prepare_transactions_for_display(self, mock_parse, mock_compute):
        """Test transaction display preparation."""
        from src.rollchain.services.options import OptionDescriptor
        
        # Mock the dependencies
        mock_parse.return_value = OptionDescriptor(
            symbol='TSLA',
            expiration='11/21/2025',
            option_type='Call',
            strike=Decimal('500.00')
        )
        mock_compute.return_value = [Decimal('100.00'), Decimal('150.00')]
        
        transactions = [
            {
                'Activity Date': '2025-01-01',
                'Instrument': 'TSLA',
                'Description': 'TSLA 11/21/2025 Call $500.00',
                'Trans Code': 'STO',
                'Quantity': '1',
                'Price': '$5.00'
            }
        ]
        target_percents = [Decimal('0.5'), Decimal('0.7')]
        
        result = prepare_transactions_for_display(transactions, target_percents)
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['date'], '2025-01-01')
        self.assertEqual(result[0]['symbol'], 'TSLA')
        self.assertEqual(result[0]['expiration'], '11/21/2025')
        self.assertEqual(result[0]['code'], 'STO')
        self.assertEqual(result[0]['quantity'], '1')
        self.assertEqual(result[0]['price'], '$5.00')
        self.assertEqual(result[0]['description'], 'TSLA $500.00 Call')
        self.assertEqual(result[0]['target_close'], '$100.00, $150.00')

    def test_prepare_chain_display(self):
        """Test chain display preparation."""
        
        chain = {
            'symbol': 'TSLA',
            'strike': Decimal('500'),
            'option_label': 'CALL',
            'expiration': '11/21/2025',
            'status': 'OPEN',
            'total_credits': Decimal('500.00'),
            'total_debits': Decimal('400.00'),
            'total_fees': Decimal('0.16'),
            'net_pnl_after_fees': Decimal('99.84'),
            'breakeven_price': Decimal('450.00'),
            'net_contracts': 1
        }
        target_bounds = (Decimal('0.5'), Decimal('0.7'))
        
        result = prepare_chain_display(chain, target_bounds)
        
        self.assertEqual(result['display_name'], 'TSLA $500 CALL')
        self.assertEqual(result['expiration'], '11/21/2025')
        self.assertEqual(result['status'], 'OPEN')
        self.assertEqual(result['credits'], '$500.00')
        self.assertEqual(result['debits'], '$400.00')
        self.assertEqual(result['fees'], '$0.16')
        # Target price should now be calculated: $450.00 + ($99.84/100 * 0.5) to ($450.00 + $99.84/100 * 0.7)
        self.assertEqual(result['target_price'], '$450.50 - $450.70')

    def test_format_net_pnl_closed_chain(self):
        """Test net P&L formatting for closed chain."""
        chain = {
            'status': 'CLOSED',
            'net_pnl_after_fees': Decimal('100.00')
        }
        self.assertEqual(format_net_pnl(chain), '$100.00')

    def test_format_net_pnl_open_chain(self):
        """Test net P&L formatting for open chain."""
        chain = {
            'status': 'OPEN',
            'total_credits': Decimal('500.00'),
            'total_debits': Decimal('450.00'),
            'total_fees': Decimal('0.16')
        }
        self.assertEqual(format_net_pnl(chain), '$49.84')

    def test_format_realized_pnl(self):
        """Test realized P&L formatting."""
        chain = {
            'total_credits': Decimal('500.00'),
            'total_debits': Decimal('425.00'),
            'total_fees': Decimal('0.16')
        }
        self.assertEqual(format_realized_pnl(chain), '$74.84')

    def test_calculate_target_price_range_valid(self):
        """Test target price range calculation with valid data."""
        chain = {
            'breakeven_price': Decimal('100.00'),
            'net_contracts': 1,
            'total_credits': Decimal('1000.00'),
            'total_debits': Decimal('500.00'),
            'total_fees': Decimal('10.00')
        }
        bounds = (Decimal('0.5'), Decimal('0.7'))
        result = calculate_target_price_range(chain, bounds)
        
        # Expected: realized = 1000 - 500 - 10 = 490
        # per_share = 490 / (1 * 100) = 4.9
        # lower_shift = 4.9 * 0.5 = 2.45
        # upper_shift = 4.9 * 0.7 = 3.43
        # Since net_contracts > 0: low = 100 + 2.45 = 102.45, high = 100 + 3.43 = 103.43
        expected = (Decimal('102.45'), Decimal('103.43'))
        self.assertEqual(result, expected)

    def test_calculate_target_price_range_negative_contracts(self):
        """Test target price range calculation with negative contracts."""
        chain = {
            'breakeven_price': Decimal('100.00'),
            'net_contracts': -1,
            'total_credits': Decimal('1000.00'),
            'total_debits': Decimal('500.00'),
            'total_fees': Decimal('10.00')
        }
        bounds = (Decimal('0.5'), Decimal('0.7'))
        result = calculate_target_price_range(chain, bounds)
        
        # Expected: realized = 1000 - 500 - 10 = 490
        # per_share = 490 / (1 * 100) = 4.9
        # lower_shift = 4.9 * 0.5 = 2.45
        # upper_shift = 4.9 * 0.7 = 3.43
        # Since net_contracts < 0: low = 100 - 3.43 = 96.57, high = 100 - 2.45 = 97.55
        expected = (Decimal('96.57'), Decimal('97.55'))
        self.assertEqual(result, expected)

    def test_calculate_target_price_range_no_breakeven(self):
        """Test target price range calculation with no breakeven."""
        chain = {
            'net_contracts': 1,
            'total_credits': Decimal('1000.00'),
            'total_debits': Decimal('500.00'),
            'total_fees': Decimal('10.00')
        }
        bounds = (Decimal('0.5'), Decimal('0.7'))
        result = calculate_target_price_range(chain, bounds)
        self.assertIsNone(result)

    def test_calculate_target_price_range_no_contracts(self):
        """Test target price range calculation with no contracts."""
        chain = {
            'breakeven_price': Decimal('100.00'),
            'net_contracts': 0,
            'total_credits': Decimal('1000.00'),
            'total_debits': Decimal('500.00'),
            'total_fees': Decimal('10.00')
        }
        bounds = (Decimal('0.5'), Decimal('0.7'))
        result = calculate_target_price_range(chain, bounds)
        self.assertIsNone(result)

    def test_calculate_target_price_range_negative_realized(self):
        """Test target price range calculation with negative realized P&L."""
        chain = {
            'breakeven_price': Decimal('100.00'),
            'net_contracts': 1,
            'total_credits': Decimal('400.00'),
            'total_debits': Decimal('500.00'),
            'total_fees': Decimal('10.00')
        }
        bounds = (Decimal('0.5'), Decimal('0.7'))
        result = calculate_target_price_range(chain, bounds)
        self.assertIsNone(result)


if __name__ == '__main__':
    unittest.main()
