# Import Cash-Flow Workflow

PremiumFlow’s hardened import flow (see [Issue #83](https://github.com/garricn/premiumflow/issues/83) and [Issue #8](https://github.com/garricn/premiumflow/issues/8)) validates option CSV exports and normalizes them into canonical transaction rows, ready for persistence or later analysis. This guide explains how to prepare data, run the CLI, and interpret the results.

## CSV Requirements & Validation

PremiumFlow expects Robinhood-style CSVs with the following headers:

```
Activity Date, Process Date, Settle Date, Instrument, Description,
Trans Code, Quantity, Price, Amount, Commission (optional)
```

During import the parser:

- Requires `--account-name` and trims both the account name and optional `--account-number`.
- Accepts commissions and prices in either `$1.23` or `(1.23)` format, and infers missing prices from `Amount` when possible (assignments default to `$0.00`).
- Filters to supported option transaction codes (`STO`, `STC`, `BTO`, `BTC`, `OASGN`) and reports row numbers in every `ImportValidationError`.
- Sorts transactions chronologically (Activity Date → Process Date → Settle Date) so downstream consumers get a stable ordering.

## Running the CLI

### Table Output

```bash
uv run premiumflow import \
  --file /tmp/import-doc-sample.csv \
  --account-name "Sample IRA" \
  --account-number "RH-123"
```

Example (abbreviated) output:

```
Account: Sample IRA (RH-123)
┏━━━━━━━━━━━━━━┳────────┳━━━━━━━━━━━━━━┳─────────┳───────┳────────┳──────┳──────────┳─────────┳──────────┳────────────────────────────────────────────┓
┃ Date         ┃ Symbol ┃ Expiration   ┃ Strike  ┃ Type  ┃ Action ┃ Code ┃ Quantity ┃ Price   ┃ Amount   ┃ Description                                ┃
┡━━━━━━━━━━━━━━╇────────╇━━━━━━━━━━━━━━╇─────────╇───────╇────────╇──────╇──────────╇─────────╇──────────╇────────────────────────────────────────────┩
│ 2025-09-01   │ TSLA   │ 2025-10-17   │ $515.00 │ CALL  │ SELL   │ STO  │ 1        │ $3.00   │ $300.00  │ TSLA 10/17/2025 Call $515.00               │
│ 2025-09-02   │ AAPL   │ 2025-10-17   │ $150.00 │ PUT   │ BUY    │ BTO  │ 1        │ $2.00   │ -$200.00 │ AAPL 10/17/2025 Put $150.00                │
│ 2025-09-15   │ AAPL   │ 2025-10-17   │ $150.00 │ PUT   │ SELL   │ STC  │ 1        │ $1.00   │ $100.00  │ AAPL 10/17/2025 Put $150.00                │
└━━━━━━━━━━━━━━┴────────┴━━━━━━━━━━━━━━┴─────────┴───────┴────────┴──────┴──────────┴─────────┴──────────┴────────────────────────────────────────────┘
```

### JSON Output

```bash
uv run premiumflow import \
  --file /tmp/import-doc-sample.csv \
  --account-name "Sample IRA" \
  --account-number "RH-123" \
  --json-output
```

Key fields in the payload:

```json
{
  "filters": { "options_only": true, "ticker": null, "strategy": null, "open_only": false },
  "account": { "name": "Sample IRA", "number": "RH-123" },
  "transactions": [
    {
      "activity_date": "2025-09-01",
      "instrument": "TSLA",
      "description": "TSLA 10/17/2025 Call $515.00",
      "trans_code": "STO",
      "action": "SELL",
      "quantity": 1,
      "price": "3",
      "amount": "300",
      "expiration": "2025-10-17",
      "strike": "515"
    },
    …
  ],
  "chains": []
}
```

Decimal-backed fields (price, amount, strike) are stringified to preserve precision, while integer
fields such as `quantity` remain numeric. The CLI table still formats values for readability
(currency symbols, comma separators, parentheses to denote negatives). When a CSV field such as
`Amount` is blank, the table displays `--` while the JSON payload emits `null` so downstream
consumers can distinguish missing data.

## Persistence

Every successful run of `premiumflow import` (and the deprecated `premiumflow ingest`) now writes the
canonical rows to a SQLite database located at `~/.premiumflow/premiumflow.db`. Set the
`PREMIUMFLOW_DB_PATH` environment variable to override the location or delete the file to reset the
stored history during development. Future CLI features and the web UI will read from the same store so
imports only need to happen once.

## Troubleshooting

- `Missing option '--account-name'`: the flag is required for every import.
- `Row N: Column "Price" cannot be blank.`: supply the broker price or confirm the row is an option assignment; PremiumFlow will infer a zero price for `OASGN` entries when both `Price` and `Amount` are empty.
- Broker commissions are currently ignored. Downstream tooling should compute fees if they are required for reporting.

## Future Enhancements

Persistence of cash-flow reports, MCP integration, and a web dashboard remain out of scope for this milestone. See the discussion in [Issue #8](https://github.com/garricn/premiumflow/issues/8) for roadmap context.
