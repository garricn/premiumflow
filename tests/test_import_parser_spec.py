from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from premiumflow.core.parser import ImportValidationError, load_option_transactions


FIXTURE_DIR = Path(__file__).parent / "fixtures"


def test_load_option_transactions_parses_fixture():
    csv_path = FIXTURE_DIR / "tsla_rc-001-closed.csv"

    results = load_option_transactions(csv_path, regulatory_fee=Decimal("0.04"))

    assert len(results) == 8

    first = results[0]
    assert first.activity_date.isoformat() == "2025-10-08"
    assert first.process_date.isoformat() == "2025-10-08"
    assert first.settle_date.isoformat() == "2025-10-09"
    assert first.trans_code == "BTC"
    assert first.action == "BUY"
    assert first.quantity == 1
    assert first.price == Decimal("8.75")
    assert first.option_type == "CALL"
    assert first.expiration.isoformat() == "2025-11-21"
    assert first.fees == Decimal("0.04")

    # Ensure sell-side rows get SELL action and still use default fees.
    sell_row = next(item for item in results if item.trans_code == "STO")
    assert sell_row.action == "SELL"
    assert sell_row.fees == Decimal("0.04")


def test_load_option_transactions_uses_commission_override(tmp_path):
    csv_content = """Activity Date,Process Date,Settle Date,Instrument,Description,Trans Code,Quantity,Price,Amount,Commission
10/7/2025,10/7/2025,10/8/2025,TSLA,TSLA 10/25/2025 Call $200.00,STO,2,$1.25,$250.00,($1.50)
"""
    csv_path = tmp_path / "commission.csv"
    csv_path.write_text(csv_content, encoding="utf-8")

    results = load_option_transactions(csv_path, regulatory_fee=Decimal("0.04"))

    assert len(results) == 1
    assert results[0].fees == Decimal("1.50")


def test_load_option_transactions_skips_non_option_rows(tmp_path):
    csv_content = (
        "Activity Date,Process Date,Settle Date,Instrument,Description,Trans Code,Quantity,Price,Amount\n"
        "10/7/2025,10/7/2025,10/8/2025,AMZN,\"Amazon\n"
        "CUSIP: 023135106\",Buy,10,$220.70,\n"
    )
    csv_path = tmp_path / "equity.csv"
    csv_path.write_text(csv_content, encoding="utf-8")

    results = load_option_transactions(csv_path, regulatory_fee=Decimal("0.04"))

    assert results == []


@pytest.mark.parametrize(
    "field,value,error",
    [
        ("Price", "xyz", 'Invalid decimal in "Price"'),
        ("Activity Date", "2025-10-07", 'Invalid date in "Activity Date"'),
    ],
)
def test_load_option_transactions_reports_first_validation_error(tmp_path, field, value, error):
    base_row = {
        "Activity Date": "10/7/2025",
        "Process Date": "10/7/2025",
        "Settle Date": "10/8/2025",
        "Instrument": "TSLA",
        "Description": "TSLA 10/25/2025 Call $200.00",
        "Trans Code": "STO",
        "Quantity": "1",
        "Price": "$1.25",
        "Amount": "$125.00",
    }
    base_row[field] = value

    headers = ",".join(base_row.keys())
    row_values = ",".join(base_row.values())
    csv_path = tmp_path / "invalid.csv"
    csv_path.write_text(f"{headers}\n{row_values}\n", encoding="utf-8")

    with pytest.raises(ImportValidationError) as excinfo:
        load_option_transactions(csv_path, regulatory_fee=Decimal("0.04"))

    assert "Row 2" in str(excinfo.value)
    assert error in str(excinfo.value)


def test_load_option_transactions_requires_option_details(tmp_path):
    csv_content = """Activity Date,Process Date,Settle Date,Instrument,Description,Trans Code,Quantity,Price,Amount
10/7/2025,10/7/2025,10/8/2025,TSLA,TSLA Option Missing Strike,BTO,1,$1.25,$125.00
"""
    csv_path = tmp_path / "bad_description.csv"
    csv_path.write_text(csv_content, encoding="utf-8")

    with pytest.raises(ImportValidationError) as excinfo:
        load_option_transactions(csv_path, regulatory_fee=Decimal("0.04"))

    assert "Row 2" in str(excinfo.value)
    assert "Description must include 'Call' or 'Put'" in str(excinfo.value)
