"""
Tests for CLI utilities module.

This module tests the shared utilities used across CLI commands.
"""

import pytest
from decimal import Decimal
from unittest.mock import patch, MagicMock
from click import BadParameter

from premiumflow.cli.utils import (
    parse_target_range,
    prepare_transactions_for_display,
    create_transactions_table,
)


class TestParseTargetRange:
    """Test parse_target_range function."""

    def test_valid_range(self):
        """Test parsing valid target range."""
        result = parse_target_range("0.5-0.7")
        assert result == (Decimal("0.5"), Decimal("0.7"))

    def test_valid_single_value(self):
        """Test parsing single value raises error (only ranges supported)."""
        with pytest.raises(BadParameter, match="Invalid target range format"):
            parse_target_range("0.6")

    def test_decimal_precision(self):
        """Test decimal precision handling."""
        result = parse_target_range("0.25-0.75")
        assert result == (Decimal("0.25"), Decimal("0.75"))

    def test_invalid_format(self):
        """Test invalid format raises BadParameter."""
        with pytest.raises(BadParameter, match="Invalid target range format"):
            parse_target_range("invalid")

    def test_invalid_bounds(self):
        """Test invalid bounds raises BadParameter."""
        with pytest.raises(BadParameter, match="Invalid target range format"):
            parse_target_range("0.8-0.5")

    def test_empty_string(self):
        """Test empty string raises BadParameter."""
        with pytest.raises(BadParameter, match="Invalid target range format"):
            parse_target_range("")

    def test_negative_values(self):
        """Test negative values raise error (not supported)."""
        with pytest.raises(BadParameter, match="Invalid target range format"):
            parse_target_range("-0.1-0.5")


class TestPrepareTransactionsForDisplay:
    """Test prepare_transactions_for_display function."""

    def test_empty_transactions(self):
        """Test with empty transaction list."""
        result = prepare_transactions_for_display([], [Decimal("0.5"), Decimal("0.7")])
        assert result == []

    def test_single_transaction(self):
        """Test with single transaction."""
        transactions = [{
            'Activity Date': '2023-01-01',
            'Instrument': 'TSLA',
            'Description': 'TSLA $500 CALL 01/20/23',
            'Trans Code': 'STO',
            'Quantity': '1',
            'Price': '5.00',
        }]
        
        with patch('premiumflow.cli.utils.parse_option_description') as mock_parse, \
             patch('premiumflow.cli.utils.format_option_display') as mock_format, \
             patch('premiumflow.cli.utils.compute_target_close_prices') as mock_compute, \
             patch('premiumflow.cli.utils.format_target_close_prices') as mock_format_target:
            
            # Setup mocks
            mock_option = MagicMock()
            mock_parse.return_value = mock_option
            mock_format.return_value = ("TSLA $500 CALL 01/20/23", "01/20/23")
            mock_compute.return_value = [Decimal("2.50"), Decimal("3.50")]
            mock_format_target.return_value = "$2.50-$3.50"
            
            result = prepare_transactions_for_display(transactions, [Decimal("0.5"), Decimal("0.7")])
            
            assert len(result) == 1
            assert result[0]["date"] == "2023-01-01"
            assert result[0]["symbol"] == "TSLA"
            assert result[0]["expiration"] == "01/20/23"
            assert result[0]["code"] == "STO"
            assert result[0]["quantity"] == "1"
            assert result[0]["price"] == "5.00"
            assert result[0]["description"] == "TSLA $500 CALL 01/20/23"
            assert result[0]["target_close"] == "$2.50-$3.50"

    def test_multiple_transactions(self):
        """Test with multiple transactions."""
        transactions = [
            {
                'Activity Date': '2023-01-01',
                'Instrument': 'TSLA',
                'Description': 'TSLA $500 CALL 01/20/23',
                'Trans Code': 'STO',
                'Quantity': '1',
                'Price': '5.00',
            },
            {
                'Activity Date': '2023-01-02',
                'Instrument': 'AAPL',
                'Description': 'AAPL $150 PUT 02/17/23',
                'Trans Code': 'BTO',
                'Quantity': '2',
                'Price': '3.50',
            }
        ]
        
        with patch('premiumflow.cli.utils.parse_option_description') as mock_parse, \
             patch('premiumflow.cli.utils.format_option_display') as mock_format, \
             patch('premiumflow.cli.utils.compute_target_close_prices') as mock_compute, \
             patch('premiumflow.cli.utils.format_target_close_prices') as mock_format_target:
            
            # Setup mocks
            mock_option = MagicMock()
            mock_parse.return_value = mock_option
            mock_format.return_value = ("Formatted Description", "Expiration")
            mock_compute.return_value = [Decimal("2.50"), Decimal("3.50")]
            mock_format_target.return_value = "$2.50-$3.50"
            
            result = prepare_transactions_for_display(transactions, [Decimal("0.5"), Decimal("0.7")])
            
            assert len(result) == 2
            assert result[0]["symbol"] == "TSLA"
            assert result[1]["symbol"] == "AAPL"

    def test_missing_fields(self):
        """Test handling of missing fields in transactions."""
        transactions = [{
            'Activity Date': '2023-01-01',
            # Missing other fields
        }]
        
        with patch('premiumflow.cli.utils.parse_option_description') as mock_parse, \
             patch('premiumflow.cli.utils.format_option_display') as mock_format, \
             patch('premiumflow.cli.utils.compute_target_close_prices') as mock_compute, \
             patch('premiumflow.cli.utils.format_target_close_prices') as mock_format_target:
            
            # Setup mocks
            mock_option = MagicMock()
            mock_parse.return_value = mock_option
            mock_format.return_value = ("", "")
            mock_compute.return_value = []
            mock_format_target.return_value = ""
            
            result = prepare_transactions_for_display(transactions, [Decimal("0.5")])
            
            assert len(result) == 1
            assert result[0]["date"] == "2023-01-01"
            assert result[0]["symbol"] == ""
            assert result[0]["expiration"] == ""
            assert result[0]["code"] == ""
            assert result[0]["quantity"] == ""
            assert result[0]["price"] == ""
            assert result[0]["description"] == ""
            assert result[0]["target_close"] == ""

    def test_none_instrument_handling(self):
        """Test handling of None instrument field."""
        transactions = [{
            'Activity Date': '2023-01-01',
            'Instrument': None,
            'Description': 'Test',
            'Trans Code': 'STO',
            'Quantity': '1',
            'Price': '5.00',
        }]
        
        with patch('premiumflow.cli.utils.parse_option_description') as mock_parse, \
             patch('premiumflow.cli.utils.format_option_display') as mock_format, \
             patch('premiumflow.cli.utils.compute_target_close_prices') as mock_compute, \
             patch('premiumflow.cli.utils.format_target_close_prices') as mock_format_target:
            
            # Setup mocks
            mock_option = MagicMock()
            mock_parse.return_value = mock_option
            mock_format.return_value = ("Test", "")
            mock_compute.return_value = []
            mock_format_target.return_value = ""
            
            result = prepare_transactions_for_display(transactions, [Decimal("0.5")])
            
            assert result[0]["symbol"] == ""


class TestCreateTransactionsTable:
    """Test create_transactions_table function."""

    def test_empty_transactions(self):
        """Test with empty transaction list."""
        table = create_transactions_table([])
        assert table.row_count == 0

    def test_single_transaction(self):
        """Test with single transaction."""
        transactions = [{
            "date": "2023-01-01",
            "symbol": "TSLA",
            "expiration": "01/20/23",
            "code": "STO",
            "quantity": "1",
            "price": "5.00",
            "description": "TSLA $500 CALL 01/20/23",
            "target_close": "$2.50-$3.50",
        }]
        
        table = create_transactions_table(transactions)
        assert table.row_count == 1

    def test_multiple_transactions(self):
        """Test with multiple transactions."""
        transactions = [
            {
                "date": "2023-01-01",
                "symbol": "TSLA",
                "expiration": "01/20/23",
                "code": "STO",
                "quantity": "1",
                "price": "5.00",
                "description": "TSLA $500 CALL 01/20/23",
                "target_close": "$2.50-$3.50",
            },
            {
                "date": "2023-01-02",
                "symbol": "AAPL",
                "expiration": "02/17/23",
                "code": "BTO",
                "quantity": "2",
                "price": "3.50",
                "description": "AAPL $150 PUT 02/17/23",
                "target_close": "$1.75-$2.45",
            }
        ]
        
        table = create_transactions_table(transactions)
        assert table.row_count == 2

    def test_table_structure(self):
        """Test table has correct columns."""
        table = create_transactions_table([])
        columns = [col.header for col in table.columns]
        expected_columns = ["Date", "Symbol", "Expiration", "Code", "Quantity", "Price", "Description", "Target Close"]
        assert columns == expected_columns
