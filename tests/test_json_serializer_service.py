# file-length-ignore
"""Tests for JSON serializer service."""

import unittest
from datetime import date
from decimal import Decimal

from premiumflow.core.legs import build_leg_fills
from premiumflow.core.parser import NormalizedOptionTransaction
from premiumflow.services.json_serializer import (
    build_ingest_payload,
    is_open_chain,
    serialize_chain,
    serialize_decimal,
    serialize_leg,
    serialize_leg_lot,
    serialize_leg_portion,
    serialize_normalized_transaction,
    serialize_transaction,
)
from premiumflow.services.leg_matching import match_leg_fills


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
            "net_pnl": Decimal("500.00"),
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
        self.assertEqual(result["net_pnl"], "500")
        self.assertEqual(result["net_contracts"], 1)
        self.assertEqual(len(result["transactions"]), 1)
        self.assertEqual(result["transactions"][0]["trans_code"], "STO")
        self.assertNotIn("total_fees", result)
        self.assertNotIn("net_pnl_after_fees", result)

    def test_serialize_chain_minimal(self):
        """Test serializing a chain with minimal data."""
        chain = {}
        result = serialize_chain(chain, "chain-1")

        self.assertEqual(result["chain_id"], "chain-1")
        self.assertEqual(result["symbol"], None)
        self.assertEqual(result["status"], None)
        self.assertEqual(result["strike"], None)
        self.assertEqual(result["transactions"], [])

    def test_serialize_normalized_transaction_complete(self):
        """Serialize a full NormalizedOptionTransaction."""
        txn = NormalizedOptionTransaction(
            activity_date=date(2025, 1, 15),
            process_date=date(2025, 1, 16),
            settle_date=None,
            instrument="TSLA",
            description="TSLA $500 Call",
            trans_code="STO",
            quantity=1,
            price=Decimal("5.00"),
            amount=Decimal("500.00"),
            strike=Decimal("500.00"),
            option_type="CALL",
            expiration=date(2025, 2, 21),
            action="SELL",
            raw={},
        )

        result = serialize_normalized_transaction(txn)

        self.assertEqual(result["activity_date"], "2025-01-15")
        self.assertEqual(result["process_date"], "2025-01-16")
        self.assertEqual(result["instrument"], "TSLA")
        self.assertEqual(result["amount"], "500")
        self.assertEqual(result["option_type"], "CALL")

    def test_build_ingest_payload_complete(self):
        """Test building complete ingest payload with canonical fields."""
        txn = NormalizedOptionTransaction(
            activity_date=date(2025, 1, 15),
            process_date=None,
            settle_date=None,
            instrument="TSLA",
            description="TSLA $500 Call",
            trans_code="STO",
            quantity=1,
            price=Decimal("5.00"),
            amount=Decimal("500.00"),
            strike=Decimal("500.00"),
            option_type="CALL",
            expiration=date(2025, 2, 21),
            action="SELL",
            raw={},
        )
        chains = [
            {
                "symbol": "TSLA",
                "status": "OPEN",
                "strike": Decimal("500.00"),
                "transactions": [],
            }
        ]

        from premiumflow.services.json_serializer import IngestPayloadOptions

        result = build_ingest_payload(
            options=IngestPayloadOptions(
                csv_file="test.csv",
                account_name="Test Account",
                account_number="ACCT-123",
                options_only=True,
                ticker="TSLA",
                strategy="calls",
                open_only=False,
            ),
            transactions=[txn],
            chains=chains,
        )

        self.assertEqual(result["source_file"], "test.csv")
        self.assertEqual(result["filters"]["options_only"], True)
        self.assertEqual(result["filters"]["ticker"], "TSLA")
        self.assertEqual(result["filters"]["strategy"], "calls")
        self.assertEqual(result["filters"]["open_only"], False)
        self.assertEqual(result["account"]["name"], "Test Account")
        self.assertEqual(result["account"]["number"], "ACCT-123")
        self.assertEqual(len(result["transactions"]), 1)
        self.assertEqual(len(result["chains"]), 1)
        txn_payload = result["transactions"][0]
        self.assertEqual(txn_payload["activity_date"], "2025-01-15")
        self.assertEqual(txn_payload["price"], "5")
        self.assertEqual(txn_payload["amount"], "500")
        self.assertIn("strike", txn_payload)
        self.assertNotIn("cash_flow", result)

    def test_build_ingest_payload_open_only(self):
        """Test building ingest payload with open_only filter."""
        chains = [
            {"symbol": "TSLA", "status": "OPEN", "transactions": []},
            {"symbol": "AAPL", "status": "CLOSED", "transactions": []},
        ]

        from premiumflow.services.json_serializer import IngestPayloadOptions

        result = build_ingest_payload(
            options=IngestPayloadOptions(
                csv_file="test.csv",
                account_name="Test Account",
                account_number=None,
                options_only=False,
                ticker=None,
                strategy=None,
                open_only=True,
            ),
            transactions=[],
            chains=chains,
        )

        # Should only include the OPEN chain
        self.assertEqual(len(result["chains"]), 1)
        self.assertEqual(result["chains"][0]["symbol"], "TSLA")
        self.assertEqual(result["chains"][0]["status"], "OPEN")

    def test_build_ingest_payload_minimal(self):
        """Test building ingest payload with minimal data."""
        from premiumflow.services.json_serializer import IngestPayloadOptions

        result = build_ingest_payload(
            options=IngestPayloadOptions(
                csv_file="test.csv",
                account_name="Test Account",
                account_number=None,
                options_only=False,
                ticker=None,
                strategy=None,
                open_only=False,
            ),
            transactions=[],
            chains=[],
        )

        self.assertEqual(result["source_file"], "test.csv")
        self.assertEqual(result["filters"]["options_only"], False)
        self.assertIsNone(result["filters"]["ticker"])
        self.assertIsNone(result["filters"]["strategy"])
        self.assertEqual(result["filters"]["open_only"], False)
        self.assertEqual(result["transactions"], [])
        self.assertEqual(result["chains"], [])

    def test_serialize_leg_portion(self):
        """Test serializing a LotFillPortion."""
        transactions = [
            NormalizedOptionTransaction(
                activity_date=date(2025, 10, 1),
                process_date=date(2025, 10, 1),
                settle_date=date(2025, 10, 3),
                instrument="TMC",
                description="TMC 10/17/2025 Call $7.00",
                trans_code="STO",
                quantity=2,
                price=Decimal("1.00"),
                amount=Decimal("200.00"),
                strike=Decimal("7.00"),
                option_type="CALL",
                expiration=date(2025, 10, 17),
                action="SELL",
                raw={},
            )
        ]
        fills = build_leg_fills(transactions, account_name="Test Account", account_number="12345")
        matched = match_leg_fills(fills)
        lot = matched.lots[0]
        portion = lot.open_portions[0]

        result = serialize_leg_portion(portion)

        self.assertEqual(result["quantity"], 2)
        self.assertEqual(result["premium"], "200.00")
        self.assertEqual(result["fees"], "0.00")
        self.assertEqual(result["activity_date"], "2025-10-01")
        self.assertEqual(result["trans_code"], "STO")
        self.assertEqual(result["description"], "TMC 10/17/2025 Call $7.00")

    def test_serialize_leg_lot_open(self):
        """Test serializing an open MatchedLegLot."""
        transactions = [
            NormalizedOptionTransaction(
                activity_date=date(2025, 10, 1),
                process_date=date(2025, 10, 1),
                settle_date=date(2025, 10, 3),
                instrument="TMC",
                description="TMC 10/17/2025 Call $7.00",
                trans_code="STO",
                quantity=2,
                price=Decimal("1.00"),
                amount=Decimal("200.00"),
                strike=Decimal("7.00"),
                option_type="CALL",
                expiration=date(2025, 10, 17),
                action="SELL",
                raw={},
            )
        ]
        fills = build_leg_fills(transactions, account_name="Test Account", account_number="12345")
        matched = match_leg_fills(fills)
        lot = matched.lots[0]

        result = serialize_leg_lot(lot)

        self.assertEqual(result["contract"]["symbol"], "TMC")
        self.assertEqual(result["contract"]["strike"], "7.00")
        self.assertEqual(result["account_name"], "Test Account")
        self.assertEqual(result["account_number"], "12345")
        self.assertEqual(result["direction"], "short")
        self.assertEqual(result["quantity"], 2)
        self.assertEqual(result["opened_at"], "2025-10-01")
        self.assertIsNone(result["closed_at"])
        self.assertEqual(result["status"], "open")
        self.assertEqual(result["open_premium"], "200.00")
        self.assertEqual(result["close_premium"], "0.00")
        self.assertIsNone(result["realized_pnl"])
        self.assertIsNone(result["net_pnl"])
        self.assertEqual(result["quantity_remaining"], 2)
        self.assertEqual(result["credit_remaining"], "200.00")

    def test_serialize_leg_lot_closed(self):
        """Test serializing a closed MatchedLegLot."""
        transactions = [
            NormalizedOptionTransaction(
                activity_date=date(2025, 10, 1),
                process_date=date(2025, 10, 1),
                settle_date=date(2025, 10, 3),
                instrument="TMC",
                description="TMC 10/17/2025 Call $7.00",
                trans_code="STO",
                quantity=2,
                price=Decimal("1.00"),
                amount=Decimal("200.00"),
                strike=Decimal("7.00"),
                option_type="CALL",
                expiration=date(2025, 10, 17),
                action="SELL",
                raw={},
            ),
            NormalizedOptionTransaction(
                activity_date=date(2025, 10, 5),
                process_date=date(2025, 10, 5),
                settle_date=date(2025, 10, 7),
                instrument="TMC",
                description="TMC 10/17/2025 Call $7.00",
                trans_code="BTC",
                quantity=2,
                price=Decimal("0.50"),
                amount=Decimal("-100.00"),
                strike=Decimal("7.00"),
                option_type="CALL",
                expiration=date(2025, 10, 17),
                action="BUY",
                raw={},
            ),
        ]
        fills = build_leg_fills(transactions, account_name="Test Account", account_number="12345")
        matched = match_leg_fills(fills)
        lot = matched.lots[0]

        result = serialize_leg_lot(lot)

        self.assertEqual(result["status"], "closed")
        self.assertEqual(result["opened_at"], "2025-10-01")
        self.assertEqual(result["closed_at"], "2025-10-05")
        self.assertEqual(result["realized_pnl"], "100.00")
        self.assertEqual(result["net_pnl"], "100.00")
        self.assertEqual(result["quantity_remaining"], 0)
        self.assertEqual(result["credit_remaining"], "0.00")
        self.assertEqual(len(result["open_portions"]), 1)
        self.assertEqual(len(result["close_portions"]), 1)

    def test_serialize_leg_open(self):
        """Test serializing an open MatchedLeg."""
        transactions = [
            NormalizedOptionTransaction(
                activity_date=date(2025, 10, 1),
                process_date=date(2025, 10, 1),
                settle_date=date(2025, 10, 3),
                instrument="TMC",
                description="TMC 10/17/2025 Call $7.00",
                trans_code="STO",
                quantity=2,
                price=Decimal("1.00"),
                amount=Decimal("200.00"),
                strike=Decimal("7.00"),
                option_type="CALL",
                expiration=date(2025, 10, 17),
                action="SELL",
                raw={},
            )
        ]
        fills = build_leg_fills(transactions, account_name="Test Account", account_number="12345")
        matched = match_leg_fills(fills)

        result = serialize_leg(matched)

        self.assertEqual(result["contract"]["symbol"], "TMC")
        self.assertEqual(result["account_name"], "Test Account")
        self.assertEqual(result["account_number"], "12345")
        self.assertEqual(result["open_quantity"], 2)
        self.assertEqual(result["net_contracts"], -2)
        self.assertEqual(result["open_premium"], "200.00")
        self.assertEqual(result["is_open"], True)
        self.assertEqual(result["opened_at"], "2025-10-01")
        self.assertIsNone(result["closed_at"])
        self.assertEqual(result["opened_quantity"], 2)
        self.assertEqual(result["closed_quantity"], 0)
        self.assertEqual(result["open_credit_gross"], "200.00")
        self.assertIsNone(result["resolution"])
        self.assertEqual(len(result["lots"]), 1)

    def test_serialize_leg_closed(self):
        """Test serializing a closed MatchedLeg."""
        transactions = [
            NormalizedOptionTransaction(
                activity_date=date(2025, 10, 1),
                process_date=date(2025, 10, 1),
                settle_date=date(2025, 10, 3),
                instrument="TMC",
                description="TMC 10/17/2025 Call $7.00",
                trans_code="STO",
                quantity=2,
                price=Decimal("1.00"),
                amount=Decimal("200.00"),
                strike=Decimal("7.00"),
                option_type="CALL",
                expiration=date(2025, 10, 17),
                action="SELL",
                raw={},
            ),
            NormalizedOptionTransaction(
                activity_date=date(2025, 10, 5),
                process_date=date(2025, 10, 5),
                settle_date=date(2025, 10, 7),
                instrument="TMC",
                description="TMC 10/17/2025 Call $7.00",
                trans_code="BTC",
                quantity=2,
                price=Decimal("0.50"),
                amount=Decimal("-100.00"),
                strike=Decimal("7.00"),
                option_type="CALL",
                expiration=date(2025, 10, 17),
                action="BUY",
                raw={},
            ),
        ]
        fills = build_leg_fills(transactions, account_name="Test Account", account_number="12345")
        matched = match_leg_fills(fills)

        result = serialize_leg(matched)

        self.assertEqual(result["is_open"], False)
        self.assertEqual(result["open_quantity"], 0)
        self.assertEqual(result["net_contracts"], 0)
        self.assertEqual(result["opened_at"], "2025-10-01")
        self.assertEqual(result["closed_at"], "2025-10-05")
        self.assertEqual(result["opened_quantity"], 2)
        self.assertEqual(result["closed_quantity"], 2)
        self.assertEqual(result["realized_pnl"], "100.00")
        self.assertEqual(result["resolution"], "BTC")

    def test_serialize_leg_lot_with_multiple_portions(self):
        """Test serializing a lot with multiple open and close portions."""
        transactions = [
            NormalizedOptionTransaction(
                activity_date=date(2025, 10, 1),
                process_date=date(2025, 10, 1),
                settle_date=date(2025, 10, 3),
                instrument="TMC",
                description="TMC 10/17/2025 Call $7.00",
                trans_code="STO",
                quantity=1,
                price=Decimal("1.00"),
                amount=Decimal("100.00"),
                strike=Decimal("7.00"),
                option_type="CALL",
                expiration=date(2025, 10, 17),
                action="SELL",
                raw={},
            ),
            NormalizedOptionTransaction(
                activity_date=date(2025, 10, 2),
                process_date=date(2025, 10, 2),
                settle_date=date(2025, 10, 4),
                instrument="TMC",
                description="TMC 10/17/2025 Call $7.00",
                trans_code="STO",
                quantity=1,
                price=Decimal("1.20"),
                amount=Decimal("120.00"),
                strike=Decimal("7.00"),
                option_type="CALL",
                expiration=date(2025, 10, 17),
                action="SELL",
                raw={},
            ),
            NormalizedOptionTransaction(
                activity_date=date(2025, 10, 5),
                process_date=date(2025, 10, 5),
                settle_date=date(2025, 10, 7),
                instrument="TMC",
                description="TMC 10/17/2025 Call $7.00",
                trans_code="BTC",
                quantity=1,
                price=Decimal("0.50"),
                amount=Decimal("-50.00"),
                strike=Decimal("7.00"),
                option_type="CALL",
                expiration=date(2025, 10, 17),
                action="BUY",
                raw={},
            ),
            NormalizedOptionTransaction(
                activity_date=date(2025, 10, 6),
                process_date=date(2025, 10, 6),
                settle_date=date(2025, 10, 8),
                instrument="TMC",
                description="TMC 10/17/2025 Call $7.00",
                trans_code="BTC",
                quantity=1,
                price=Decimal("0.60"),
                amount=Decimal("-60.00"),
                strike=Decimal("7.00"),
                option_type="CALL",
                expiration=date(2025, 10, 17),
                action="BUY",
                raw={},
            ),
        ]
        fills = build_leg_fills(transactions, account_name="Test Account", account_number="12345")
        matched = match_leg_fills(fills)
        # FIFO matching creates separate lots: STO1 -> BTC1, STO2 -> BTC2
        # So we get 2 closed lots, each with 1 open portion and 1 close portion
        self.assertEqual(len(matched.lots), 2)

        # Serialize both lots and verify they're correctly serialized
        lot1_result = serialize_leg_lot(matched.lots[0])
        lot2_result = serialize_leg_lot(matched.lots[1])

        # Each lot should have 1 open portion and 1 close portion
        self.assertEqual(len(lot1_result["open_portions"]), 1)
        self.assertEqual(len(lot1_result["close_portions"]), 1)
        self.assertEqual(len(lot2_result["open_portions"]), 1)
        self.assertEqual(len(lot2_result["close_portions"]), 1)

        # Verify quantities
        self.assertEqual(lot1_result["quantity"], 1)
        self.assertEqual(lot2_result["quantity"], 1)

        # Verify premiums (each lot should have one of the STO/BTC pairs)
        total_open = Decimal(lot1_result["open_premium"]) + Decimal(lot2_result["open_premium"])
        total_close = Decimal(lot1_result["close_premium"]) + Decimal(lot2_result["close_premium"])
        self.assertEqual(total_open, Decimal("220.00"))  # 100 + 120
        self.assertEqual(total_close, Decimal("-110.00"))  # -50 + -60

    def test_serialize_leg_portion_negative_premium(self):
        """Test serializing a portion with negative premium (debit)."""
        transactions = [
            NormalizedOptionTransaction(
                activity_date=date(2025, 10, 1),
                process_date=date(2025, 10, 1),
                settle_date=date(2025, 10, 3),
                instrument="TMC",
                description="TMC 10/17/2025 Call $7.00",
                trans_code="BTO",
                quantity=2,
                price=Decimal("1.00"),
                amount=Decimal("-200.00"),
                strike=Decimal("7.00"),
                option_type="CALL",
                expiration=date(2025, 10, 17),
                action="BUY",
                raw={},
            )
        ]
        fills = build_leg_fills(transactions, account_name="Test Account", account_number="12345")
        matched = match_leg_fills(fills)
        lot = matched.lots[0]
        portion = lot.open_portions[0]

        result = serialize_leg_portion(portion)

        self.assertEqual(result["premium"], "-200.00")  # Negative for debit
        self.assertEqual(result["fees"], "0.00")

    def test_serialize_leg_lot_with_empty_portions(self):
        """Test serializing a lot with empty portions arrays (edge case)."""
        transactions = [
            NormalizedOptionTransaction(
                activity_date=date(2025, 10, 1),
                process_date=date(2025, 10, 1),
                settle_date=date(2025, 10, 3),
                instrument="TMC",
                description="TMC 10/17/2025 Call $7.00",
                trans_code="STO",
                quantity=2,
                price=Decimal("1.00"),
                amount=Decimal("200.00"),
                strike=Decimal("7.00"),
                option_type="CALL",
                expiration=date(2025, 10, 17),
                action="SELL",
                raw={},
            )
        ]
        fills = build_leg_fills(transactions, account_name="Test Account", account_number="12345")
        matched = match_leg_fills(fills)
        lot = matched.lots[0]

        result = serialize_leg_lot(lot)

        # Open lot should have open_portions but empty close_portions
        self.assertEqual(len(result["open_portions"]), 1)
        self.assertEqual(len(result["close_portions"]), 0)
        self.assertEqual(result["close_premium"], "0.00")
        self.assertEqual(result["close_fees"], "0.00")


if __name__ == "__main__":
    unittest.main()
