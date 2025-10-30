from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from premiumflow.core.parser import ImportValidationError, load_option_transactions

FIXTURE_DIR = Path(__file__).parent / "fixtures"


def test_load_option_transactions_parses_fixture():
    csv_path = FIXTURE_DIR / "options_sample.csv"

    result = load_option_transactions(
        csv_path,
        account_name="Robinhood IRA",
        account_number=" RH-12345 ",
        regulatory_fee=Decimal("0.04"),
    )

    assert result.account_name == "Robinhood IRA"
    assert result.account_number == "RH-12345"
    assert result.regulatory_fee == Decimal("0.00")
    assert len(result.transactions) == 3

    # First row is the STO entry (SELL).
    first = result.transactions[0]
    assert first.activity_date.isoformat() == "2025-10-07"
    assert first.process_date.isoformat() == "2025-10-07"
    assert first.settle_date.isoformat() == "2025-10-08"
    assert first.trans_code == "STO"
    assert first.action == "SELL"
    assert first.quantity == 2
    assert first.price == Decimal("1.20")
    assert first.option_type == "CALL"
    assert first.expiration.isoformat() == "2025-10-25"
    assert first.fees == Decimal("0.00")

    # Second row ensures BUY/STC branches.
    second = result.transactions[1]
    assert second.trans_code == "BTC"
    assert second.action == "BUY"
    assert second.fees == Decimal("0.00")


def test_load_option_transactions_rejects_parenthesized_price(tmp_path):
    csv_content = """Activity Date,Process Date,Settle Date,Instrument,Description,Trans Code,Quantity,Price,Amount
10/7/2025,10/7/2025,10/8/2025,TSLA,TSLA 10/25/2025 Call $200.00,STO,2,(1.25),$250.00
"""
    csv_path = tmp_path / "parenthesized_price.csv"
    csv_path.write_text(csv_content, encoding="utf-8")

    with pytest.raises(ImportValidationError) as excinfo:
        load_option_transactions(
            csv_path,
            account_name="Test Account",
            regulatory_fee=Decimal("0.04"),
        )

    assert 'Row 2: Invalid decimal in "Price"' in str(
        excinfo.value
    ) or "must be non-negative" in str(excinfo.value)


def test_load_option_transactions_skips_non_option_rows(tmp_path):
    csv_content = (
        "Activity Date,Process Date,Settle Date,Instrument,Description,Trans Code,Quantity,Price,Amount\n"
        '10/7/2025,10/7/2025,10/8/2025,AMZN,"Amazon\n'
        'CUSIP: 023135106",Buy,10,$220.70,\n'
    )
    csv_path = tmp_path / "equity.csv"
    csv_path.write_text(csv_content, encoding="utf-8")

    result = load_option_transactions(
        csv_path,
        account_name="Test Account",
        regulatory_fee=Decimal("0.04"),
    )

    assert result.transactions == []


def test_load_option_transactions_skips_incomplete_non_option_rows(tmp_path):
    csv_content = (
        "Activity Date,Process Date,Settle Date,Instrument,Description,Trans Code,Quantity,Price,Amount\n"
        ",,,,,Buy,,,\n"
    )
    csv_path = tmp_path / "incomplete.csv"
    csv_path.write_text(csv_content, encoding="utf-8")

    result = load_option_transactions(
        csv_path,
        account_name="Test Account",
        regulatory_fee=Decimal("0.04"),
    )

    assert result.transactions == []


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
        load_option_transactions(
            csv_path,
            account_name="Test Account",
            regulatory_fee=Decimal("0.04"),
        )

    assert "Row 2" in str(excinfo.value)
    assert error in str(excinfo.value)


def test_load_option_transactions_requires_option_details(tmp_path):
    csv_content = """Activity Date,Process Date,Settle Date,Instrument,Description,Trans Code,Quantity,Price,Amount
10/7/2025,10/7/2025,10/8/2025,TSLA,TSLA Option Missing Strike,BTO,1,$1.25,$125.00
"""
    csv_path = tmp_path / "bad_description.csv"
    csv_path.write_text(csv_content, encoding="utf-8")

    with pytest.raises(ImportValidationError) as excinfo:
        load_option_transactions(
            csv_path,
            account_name="Test Account",
            regulatory_fee=Decimal("0.04"),
        )

    assert "Row 2" in str(excinfo.value)
    assert "Description must include 'Call' or 'Put'" in str(excinfo.value)


def test_load_option_transactions_requires_account_name(tmp_path):
    csv_content = """Activity Date,Process Date,Settle Date,Instrument,Description,Trans Code,Quantity,Price,Amount
10/7/2025,10/7/2025,10/8/2025,TSLA,TSLA 10/25/2025 Call $200.00,STO,1,$1.25,$125.00
"""
    csv_path = tmp_path / "account.csv"
    csv_path.write_text(csv_content, encoding="utf-8")

    with pytest.raises(ImportValidationError) as excinfo:
        load_option_transactions(csv_path, account_name=" ", regulatory_fee=Decimal("0.04"))

    assert str(excinfo.value) == "--account-name is required."


def test_load_option_transactions_rejects_blank_account_number(tmp_path):
    csv_content = """Activity Date,Process Date,Settle Date,Instrument,Description,Trans Code,Quantity,Price,Amount
10/7/2025,10/7/2025,10/8/2025,TSLA,TSLA 10/25/2025 Call $200.00,STO,1,$1.25,$125.00
"""
    csv_path = tmp_path / "account_blank_number.csv"
    csv_path.write_text(csv_content, encoding="utf-8")

    with pytest.raises(ImportValidationError) as excinfo:
        load_option_transactions(
            csv_path,
            account_name="Test Account",
            account_number="   ",
            regulatory_fee=Decimal("0.04"),
        )

    assert str(excinfo.value) == "--account-number cannot be blank."
