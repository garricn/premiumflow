# Import Cash-Flow Workflow

PremiumFlow’s hardened import flow (see [Issue #83](https://github.com/garricn/premiumflow/issues/83) and [Issue #8](https://github.com/garricn/premiumflow/issues/8)) validates option CSV exports, normalizes the data, and surfaces cash-flow metrics in both table and JSON form. This guide explains how to prepare data, run the CLI, and interpret the results.

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
- Sorts transactions chronologically (Activity Date → Process Date → Settle Date) before computing running cash-flow totals.

## Running the CLI

### Table Output

```bash
uv run premiumflow import \
  --file /tmp/import-doc-sample.csv \
  --account-name "Sample IRA" \
  --account-number "RH-123" \
  --regulatory-fee 0.04
```

Example (abbreviated) output:

```
Account: Sample IRA (RH-123) · Reg Fee: $0.04
┏━━━━━┳━━━━━━┳━━━━━━━━━━━━┳━━━━━┳━━━┳━━━━━┳━━━━━━━┳━━━━━━┳━━━━┳━━━━━━━┳━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
│ Date│Symbol│ Expiration │Code │Qty│ Price│ Credit│ Debit│ Fee│ Net Pr.│ Net P&L│ Target Close │ Description                                  │
├─────┼──────┼────────────┼─────┼───┼──────┼───────┼──────┼────┼────────┼────────┼──────────────┼──────────────────────────────────────────────┤
│2025-09-01│TSLA│2025-10-17│STO│1│$3.00│$300.00│$0.00│$0.04│$300.00│$299.96│$1.50,$1.20,$0.90│TSLA 10/17/2025 Call $515.00│
│2025-09-02│AAPL│2025-10-17│BTO│1│$2.00│$0.00 │$200.00│$0.04│$100.00│$99.92 │$3.00,$3.20,$3.40│AAPL 10/17/2025 Put $150.00 │
│2025-09-15│AAPL│2025-10-17│STC│1│$1.00│$100.00│$0.00│$0.04│$200.00│$199.88│--              │AAPL 10/17/2025 Put $150.00 │
└─────┴──────┴────────────┴─────┴───┴──────┴───────┴──────┴────┴────────┴────────┴──────────────┴──────────────────────────────────────────────┘
Totals: Credits $400.00 · Debits $200.00 · Fees $0.12 · Net Premium $200.00 · Net P&L $199.88
```

### JSON Output

```bash
uv run premiumflow import \
  --file /tmp/import-doc-sample.csv \
  --account-name "Sample IRA" \
  --account-number "RH-123" \
  --regulatory-fee 0.04 \
  --json-output
```

Key fields in the payload:

```json
{
  "filters": { "options_only": true, "ticker": null, "strategy": null, "open_only": false },
  "account": { "name": "Sample IRA", "number": "RH-123" },
  "regulatory_fee": "0.04",
  "cash_flow": { "credits": "400", "debits": "200", "fees": "0.12", "net_premium": "200", "net_pnl": "199.88" },
  "transactions": [
    {
      "trans_code": "STO",
      "credit": "300",
      "fee": "0.04",
      "running": { "net_premium": "300", "net_pnl": "299.96" },
      "targets": ["1.5", "1.2", "0.9"],
      "expiration": "2025-10-17",
      "strike": "515"
    },
    …
  ],
  "chains": []
}
```

## Troubleshooting

- `Missing option '--account-name'`: the flag is required for every import.
- `--regulatory-fee must be a decimal value.`: pass numeric values like `0.04`.
- `Row N: Column "Price" cannot be blank.`: supply the broker price or confirm the row is an option assignment; PremiumFlow will infer a zero price for `OASGN` entries when both `Price` and `Amount` are empty.
- Commissions supplied in the CSV always override the regulatory fee and accept parenthesized syntax `(1.50)`.

## Future Enhancements

Persistence of cash-flow reports, MCP integration, and a web dashboard remain out of scope for this milestone. See the discussion in [Issue #8](https://github.com/garricn/premiumflow/issues/8) for roadmap context.
