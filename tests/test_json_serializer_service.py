"""Tests for JSON serializer service."""

import unittest
from decimal import Decimal

from src.premiumflow.services.json_serializer import (
    build_ingest_payload,
    is_open_chain,
    serialize_chain,
    serialize_decimal,
    serialize_transaction,
)


class TestJsonSerializer(unittest.TestCase):
    """Test JSON serialization functions."""

    def test_serialize_decimal_positive(self):
        """Test serializing positive Decimal values."""
        value = Decimal("123.45")
        result = serialize_decimal(value)
        self.assertEqual(result, "123.45")

    def test_serialize_decimal_negative(self):
        """Test serializing negative Decimal values."""
        value = Decimal("-67.89")
        result = serialize_decimal(value)
        self.assertEqual(result, "-67.89")

    def test_serialize_decimal_zero(self):
        """Test serializing zero Decimal values."""
        value = Decimal("0")
        result = serialize_decimal(value)
        self.assertEqual(result, "0")

    def test_serialize_decimal_none(self):
        """Test serializing None values."""
        result = serialize_decimal(None)
        self.assertIsNone(result)

    def test_serialize_decimal_string(self):
        """Test serializing string values."""
        result = serialize_decimal("test")
        self.assertEqual(result, "test")

    def test_serialize_decimal_int(self):
        """Test serializing integer values."""
        result = serialize_decimal(42)
        self.assertEqual(result, 42)

    def test_is_open_chain_by_status_open(self):
        """Test is_open_chain with OPEN status."""
        chain = {"status": "OPEN"}
        self.assertTrue(is_open_chain(chain))

    def test_is_open_chain_by_status_closed(self):
        """Test is_open_chain with CLOSED status."""
        chain = {"status": "CLOSED"}
        self.assertFalse(is_open_chain(chain))

    def test_is_open_chain_by_last_transaction_sto(self):
        """Test is_open_chain with STO as last transaction."""
        chain = {"transactions": [{"Trans Code": "BTO"}, {"Trans Code": "STO"}]}
        self.assertTrue(is_open_chain(chain))

    def test_is_open_chain_by_last_transaction_bto(self):
        """Test is_open_chain with BTO as last transaction."""
        chain = {"transactions": [{"Trans Code": "STO"}, {"Trans Code": "BTO"}]}
        self.assertTrue(is_open_chain(chain))

    def test_is_open_chain_by_last_transaction_close(self):
        """Test is_open_chain with closing transaction."""
        chain = {"transactions": [{"Trans Code": "STO"}, {"Trans Code": "BTC"}]}
        self.assertFalse(is_open_chain(chain))

    def test_is_open_chain_no_transactions(self):
        """Test is_open_chain with no transactions."""
        chain = {"transactions": []}
        self.assertFalse(is_open_chain(chain))

    def test_is_open_chain_no_status_no_transactions(self):
        """Test is_open_chain with no status and no transactions."""
        chain = {}
        self.assertFalse(is_open_chain(chain))

    def test_serialize_transaction_complete(self):
        """Test serializing a complete transaction."""
        txn = {
            "Activity Date": "2025-01-15",
            "Instrument": "TSLA",
            "Description": "TSLA $500 Call",
            "Trans Code": "STO",
            "Quantity": "1",
            "Price": "$5.00",
            "Amount": "$500.00",
        }
        result = serialize_transaction(txn)

        expected = {
            "activity_date": "2025-01-15",
            "instrument": "TSLA",
            "description": "TSLA $500 Call",
            "trans_code": "STO",
            "quantity": "1",
            "price": "$5.00",
            "amount": "$500.00",
        }
        self.assertEqual(result, expected)

    def test_serialize_transaction_minimal(self):
        """Test serializing a transaction with minimal data."""
        txn = {}
        result = serialize_transaction(txn)

        expected = {
            "activity_date": "",
            "instrument": "",
            "description": "",
            "trans_code": "",
            "quantity": "",
            "price": "",
            "amount": "",
        }
        self.assertEqual(result, expected)

    def test_serialize_transaction_with_none_values(self):
        """Test serializing a transaction with None values."""
        txn = {
            "Instrument": None,
            "Trans Code": None,
        }
        result = serialize_transaction(txn)

        self.assertEqual(result["instrument"], "")
        self.assertEqual(result["trans_code"], "")

    def test_serialize_chain_complete(self):
        """Test serializing a complete chain."""
        chain = {
            "symbol": "TSLA",
            "status": "OPEN",
            "start_date": "2025-01-15",
            "end_date": None,
            "roll_count": 2,
            "strike": Decimal("500.00"),
            "option_label": "CALL",
            "expiration": "2025-02-21",
            "total_credits": Decimal("1000.00"),
            "total_debits": Decimal("500.00"),
            "total_fees": Decimal("10.00"),
            "net_pnl": Decimal("490.00"),
            "net_pnl_after_fees": Decimal("480.00"),
            "breakeven_price": Decimal("450.00"),
            "breakeven_direction": "UP",
            "net_contracts": 1,
            "transactions": [
                {
                    "Activity Date": "2025-01-15",
                    "Trans Code": "STO",
                    "Quantity": "1",
                    "Price": "$5.00",
                    "Amount": "$500.00",
                    "Description": "TSLA $500 Call",
                }
            ],
        }
        result = serialize_chain(chain, "chain-1")

        self.assertEqual(result["chain_id"], "chain-1")
        self.assertEqual(result["display_name"], "TSLA $500 CALL")
        self.assertEqual(result["symbol"], "TSLA")
        self.assertEqual(result["status"], "OPEN")
        self.assertEqual(result["strike"], "500")
        self.assertEqual(result["total_credits"], "1000")
        self.assertEqual(result["total_debits"], "500")
        self.assertEqual(result["net_contracts"], 1)
        self.assertEqual(len(result["transactions"]), 1)
        self.assertEqual(result["transactions"][0]["trans_code"], "STO")

    def test_serialize_chain_minimal(self):
        """Test serializing a chain with minimal data."""
        chain = {}
        result = serialize_chain(chain, "chain-1")

        self.assertEqual(result["chain_id"], "chain-1")
        self.assertEqual(result["symbol"], None)
        self.assertEqual(result["status"], None)
        self.assertEqual(result["strike"], None)
        self.assertEqual(result["transactions"], [])

    def test_build_ingest_payload_complete(self):
        """Test building complete ingest payload."""
        transactions = [
            {
                "Activity Date": "2025-01-15",
                "Instrument": "TSLA",
                "Description": "TSLA $500 Call",
                "Trans Code": "STO",
                "Quantity": "1",
                "Price": "$5.00",
                "Amount": "$500.00",
            }
        ]
        display_rows = [
            {
                "target_close": "$100.00, $150.00",
            }
        ]
        chains = [
            {
                "symbol": "TSLA",
                "status": "OPEN",
                "strike": Decimal("500.00"),
                "transactions": [],
            }
        ]
        target_percents = [Decimal("0.5"), Decimal("0.7")]

        result = build_ingest_payload(
            csv_file="test.csv",
            transactions=transactions,
            display_rows=display_rows,
            chains=chains,
            target_percents=target_percents,
            options_only=True,
            ticker="TSLA",
            strategy="calls",
            open_only=False,
        )

        self.assertEqual(result["source_file"], "test.csv")
        self.assertEqual(result["filters"]["options_only"], True)
        self.assertEqual(result["filters"]["ticker"], "TSLA")
        self.assertEqual(result["filters"]["strategy"], "calls")
        self.assertEqual(result["filters"]["open_only"], False)
        self.assertEqual(result["target_percents"], ["0.5", "0.7"])
        self.assertEqual(len(result["transactions"]), 1)
        self.assertEqual(len(result["chains"]), 1)
        self.assertEqual(result["transactions"][0]["targets"], "$100.00, $150.00")

    def test_build_ingest_payload_open_only(self):
        """Test building ingest payload with open_only filter."""
        transactions = []
        display_rows = []
        chains = [
            {"symbol": "TSLA", "status": "OPEN", "transactions": []},
            {"symbol": "AAPL", "status": "CLOSED", "transactions": []},
        ]
        target_percents = [Decimal("0.5")]

        result = build_ingest_payload(
            csv_file="test.csv",
            transactions=transactions,
            display_rows=display_rows,
            chains=chains,
            target_percents=target_percents,
            options_only=False,
            ticker=None,
            strategy=None,
            open_only=True,
        )

        # Should only include the OPEN chain
        self.assertEqual(len(result["chains"]), 1)
        self.assertEqual(result["chains"][0]["symbol"], "TSLA")
        self.assertEqual(result["chains"][0]["status"], "OPEN")

    def test_build_ingest_payload_minimal(self):
        """Test building ingest payload with minimal data."""
        result = build_ingest_payload(
            csv_file="test.csv",
            transactions=[],
            display_rows=[],
            chains=[],
            target_percents=[],
            options_only=False,
            ticker=None,
            strategy=None,
            open_only=False,
        )

        self.assertEqual(result["source_file"], "test.csv")
        self.assertEqual(result["filters"]["options_only"], False)
        self.assertEqual(result["filters"]["ticker"], None)
        self.assertEqual(result["filters"]["strategy"], None)
        self.assertEqual(result["filters"]["open_only"], False)
        self.assertEqual(result["target_percents"], [])
        self.assertEqual(result["transactions"], [])
        self.assertEqual(result["chains"], [])


if __name__ == "__main__":
    unittest.main()
