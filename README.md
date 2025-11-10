# PremiumFlow

A Python tool for analyzing options trading roll chains from CSV transaction data.

## Code Review Process

### Automated Reviews

- All PRs get automated Codex reviews
- Look for **P1/P2/P3 priority badges** in comments
- Address all **P1 issues** before merging
- Use `gh pr view {PR} --json reviews,comments` to see all feedback

### Checking PR Feedback

Use the provided script to comprehensively check all feedback:

```bash
./scripts/check-pr-feedback.sh {PR_NUMBER}
```

Or manually check:

```bash
# Get all reviews
gh api repos/garricn/premiumflow/pulls/{PR}/reviews

# Get review comments for each review
gh api repos/garricn/premiumflow/pulls/{PR}/reviews/{REVIEW_ID}/comments
```

### Priority Levels

- **P1**: Critical functionality changes that could break existing behavior
- **P2**: Important improvements or optimizations
- **P3**: Minor suggestions or style improvements

## Development Guidelines

### Service Extraction

- Each service should be self-contained when possible
- Branch from `main` for each new service
- Include comprehensive unit tests
- Export functions in `services/__init__.py`
- Address code review comments before merging

### Code Formatting & Linting

- Python formatting uses [Black](https://black.readthedocs.io/) with a 100-character line length target.
- Linting is handled by [Ruff](https://docs.astral.sh/ruff/) for import order, bugbear checks, and general hygiene.
- Markdown uses [mdformat](https://mdformat.readthedocs.io/) with the same 100-character wrap; run `uv run mdformat README.md` to keep docs tidy.
- Shell scripts use [shfmt](https://github.com/mvdan/sh) (v3.12.0); run `shfmt -w scripts` after edits (`brew install shfmt` on macOS, or download from the releases page).
- YAML files use [yamlfmt](https://github.com/google/yamlfmt) (v0.20.0); run `yamlfmt -conf .yamlfmt.yml .github/workflows` (Homebrew: `brew install yamlfmt`).
- TOML files use [taplo](https://taplo.tamasfe.dev/) (v0.10.0); run `taplo fmt --config .taplo.toml pyproject.toml` (Homebrew: `brew install taplo`).
- Run `uv run black src tests`, `uv run ruff check src tests --fix`, `uv run mdformat README.md`, `shfmt -w scripts`, `yamlfmt -conf .yamlfmt.yml .github/workflows`, `taplo fmt --config .taplo.toml pyproject.toml`, and `uv run mypy --config-file mypy.ini src/premiumflow tests` before sending a PR; CI enforces the same formatting checks.
- Other file types have their own formatter issues tracked separately.

### Pre-commit Hooks

- Install hooks after syncing dependencies: `uv run pre-commit install --hook-type pre-commit --hook-type pre-push`.
- Run `uv run pre-commit run --all-files` to warm caches and verify the checkout before pushing.
- Hooks call `black`, `ruff`, `mypy`, and `mdformat` via `uv`, plus locally installed `shfmt`, `yamlfmt`, and `taplo` binaries (see commands above for installation).
- A pre-push hook runs `uv run pytest` so CI failures are caught early.

### Financial Calculations

- Use `Decimal` for all financial calculations
- Maintain backward compatibility when refactoring
- Test coverage is critical for financial calculations
- Always preserve fallback logic when refactoring

## Environment Setup

PremiumFlow targets Python **3.11** and uses [uv](https://github.com/astral-sh/uv) for Python and dependency management.

1. [Install `uv`](https://github.com/astral-sh/uv?tab=readme-ov-file#installation) (single static binary).

1. Sync the project environment (installs dependencies and the package in editable mode):

   ```bash
   uv sync --extra dev
   ```

   The repository includes a committed `uv.lock`; use `uv sync --locked --extra dev` in CI or when you want to ensure the lockfile stays unchanged.

1. Run commands inside the project environment with `uv run`, for example:

   ```bash
   uv run pytest
   ```

### Testing & Coverage

`uv run pytest` now runs tests with coverage instrumentation (`pytest-cov` is included in the dev extras).
The terminal output highlights the total coverage percentage and shows missing lines per file so you can quickly detect gaps,
and an HTML report lands at `htmlcov/index.html` for a browsable module-by-module breakdown. Coverage must stay at or above
**85% overall**—`pytest` fails locally and in CI when coverage falls beneath that bar—so inspect the terminal summary and
open `htmlcov/index.html` after a run to verify the lines you just added are exercised.

To add or upgrade dependencies, edit `pyproject.toml` and regenerate the lockfile:

```bash
uv lock --upgrade-package <package-name>     # upgrade specific packages
uv lock                                      # refresh everything after edits
```

## Usage

```bash
uv run premiumflow analyze transactions.csv
uv run premiumflow import --json-output
uv run premiumflow import list
uv run premiumflow import delete 42 --yes
uv run premiumflow lookup "TSLA 500C 2025-02-21"
uv run premiumflow trace "TSLA $550 Call" all_transactions.csv
uv run premiumflow legs --status open --lots
uv run premiumflow cashflow --account-name "Robinhood" --account-number 1234 --assignment-handling exclude
```

### Matched Legs CLI

Run `premiumflow legs` after importing data to inspect FIFO-matched option legs stored in the SQLite
database. Filters combine with AND semantics:

- `--account-name` / `--account-number` narrow to a specific account label.
- `--ticker` restricts to a single underlying symbol.
- `--since` / `--until` accept `YYYY-MM-DD` activity dates.
- `--status` chooses `open`, `closed`, or `all` legs (default `all`).
- `--format` toggles between `table` and `json`; add `--lots` to include lot-level detail beneath
  the summary table.

Table output includes opened/closed dates, a resolution summary ("Expiration", "Assignment",
"Buy to close", etc.), Robinhood-style cash columns (`Credit at Open`, `Cost at Close`,
`Realized P/L`, `Credit Remaining`), separate open/close fee columns, and a net-cash figure that
matches trade confirmations. Totals rows summarize quantity, cash, and fees at both the leg and lot
levels. JSON output mirrors the per-leg metadata and exposes the complete lot payload for
downstream tooling.

### Cashflow Assignment Handling

Use `premiumflow cashflow` to summarize realized P&L, fees, and cashflow by day/week/month. Set
`--assignment-handling include` (default) to keep assignment premium in the realized columns, or
`--assignment-handling exclude` to defer those credits so the totals line up with broker “options
only” reports. The table (CLI, JSON, and web UI) always shows an `Assignment Premium (After Fees)`
column so you can reconcile the difference regardless of the selected mode.

### Cash-Flow Import Guide

The hardened `premiumflow import` workflow (required flags, validation behavior, cash-flow metrics, sample
output, and troubleshooting) is documented in [docs/import-cash-flow.md](docs/import-cash-flow.md).

### Parser API

If you need to work directly with the import parser, call `premiumflow.core.parser.load_option_transactions`
with explicit account metadata. The function returns a `ParsedImportResult` object that bundles the
trimmed account name/number and the list of normalized option rows.

```python
from decimal import Decimal
from premiumflow.core.parser import load_option_transactions

result = load_option_transactions(
    "all_transactions.csv",
    account_name="Robinhood IRA",
    account_number="RH-12345",
)

for txn in result.transactions:
    ...
```

Both `account_name` and `account_number` are required and must contain non-whitespace characters.

### Persistence

Running `premiumflow import` writes the normalized rows to a SQLite database so that subsequent tools and
the forthcoming web UI can read from a durable source. By default the database lives at
`~/.premiumflow/premiumflow.db`; set the `PREMIUMFLOW_DB_PATH` environment variable to point the CLI at a
different location. Deleting the file removes all stored imports. Use `premiumflow import list` to review
persisted ingests and `premiumflow import delete <id>` to remove one without touching the entire database.

- Use `--skip-existing` to leave the persisted data untouched when importing the same file again, or
  `--replace-existing` to overwrite the stored copy. Without either flag, duplicate imports terminate with
  a helpful message. Duplicate detection is keyed by account and file path.
