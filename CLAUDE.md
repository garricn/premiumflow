# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Quick Start: Common Commands

All commands use `uv run` to execute within the project environment.

### Setup

```bash
uv sync --extra dev                    # Install dependencies (Python 3.11+)
uv run pre-commit install --hook-type pre-commit --hook-type pre-push
```

### Testing & Coverage

```bash
uv run pytest                          # Run all tests (85% coverage required)
uv run pytest tests/test_file.py       # Run a specific test file
uv run pytest tests/test_file.py::TestClass::test_method  # Run single test
```

### Code Quality

```bash
uv run black src tests                 # Format Python code (100-char line)
uv run ruff check src tests --fix      # Lint and auto-fix
uv run mypy --config-file mypy.ini src/premiumflow tests  # Type check
uv run mdformat README.md              # Format markdown
shfmt -w scripts                       # Format shell scripts (install: brew install shfmt)
yamlfmt -conf .yamlfmt.yml .github/workflows  # Format YAML (install: brew install yamlfmt)
taplo fmt --config .taplo.toml pyproject.toml # Format TOML (install: brew install taplo)
uv run pre-commit run --all-files      # Run all pre-commit checks
```

### Running the Application

```bash
uv run premiumflow analyze transactions.csv
uv run premiumflow import --json-output
uv run premiumflow import list
uv run premiumflow legs --status open --lots
uv run premiumflow cashflow --account-name "Robinhood"
uv run premiumflow lookup "TSLA 500C 2025-02-21"
uv run premiumflow trace "TSLA $550 Call" all_transactions.csv
uv run premiumflow shares
```

## Project Overview

**PremiumFlow** analyzes options trading transactions to generate roll chain analysis, matched leg reports, and cash flow/P&L summaries. It consists of a Click-based CLI and a FastAPI web UI, backed by SQLite persistence.

### Technology Stack

- **Language & Version**: Python 3.11+
- **CLI**: Click 8.1.0+
- **Web**: FastAPI 0.115.0+ with Jinja2 templating
- **Data Validation**: Pydantic 2.0.0+
- **Database**: SQLite (built-in)
- **Financial Math**: Decimal (required for accuracy)
- **Code Tools**: Black, Ruff, mypy, pytest
- **Dependency Manager**: uv (Astral)

## Architecture

PremiumFlow uses a **layered architecture** with clear separation of concerns:

```
┌─────────────────────────────────┐
│ CLI & Web UI Layer              │
│ (cli/, web/)                    │
└────────────────┬────────────────┘
                 │
┌────────────────▼────────────────┐
│ Services Layer                  │
│ (services/)                     │
│ - Financial calculations        │
│ - Data aggregation & matching   │
│ - Formatting                    │
└────────────────┬────────────────┘
                 │
┌────────────────▼────────────────┐
│ Core Layer                      │
│ (core/)                         │
│ - Models, CSV parsing           │
│ - Leg processing                │
└────────────────┬────────────────┘
                 │
┌────────────────▼────────────────┐
│ Persistence Layer               │
│ (persistence/)                  │
│ - SQLite storage                │
└─────────────────────────────────┘
```

### Core Layer: `src/premiumflow/core/`

- **`models.py`**: Pydantic models (`Transaction`, `RollChain`)
- **`parser.py`**: CSV parsing with support for options (BTC, STO, OASGN, OEXP codes), stock transfers, and ACH transactions. Returns `ParsedImportResult` with normalized `NormalizedOptionTransaction` and `NormalizedStockTransaction` objects
- **`legs.py`**: Options leg contract and fill classes (`LegContract`, `LegFill`, `OptionLeg`) with utilities like `build_leg_fills()` and `aggregate_legs()`

### Persistence Layer: `src/premiumflow/persistence/`

- **`storage.py`**: `SQLiteStorage` class manages database lifecycle, transaction persistence via `store_import_result()`, and detects duplicates (raises `DuplicateImportError`). Database path: `~/.premiumflow/premiumflow.db` or `PREMIUMFLOW_DB_PATH` env var. Uses singleton pattern via `get_storage()`
- **`repository.py`**: `SQLiteRepository` (singleton pattern via `get_repository()`) provides query interface. Data classes: `StoredTransaction`, `StoredImport`, `StoredStockLot`

### Services Layer: `src/premiumflow/services/` (Core Business Logic)

- **`leg_matching.py`**: FIFO matching algorithm (`match_legs()`, `match_leg_fills()`). Tracks lot-level fills with fees and premiums in `MatchedLeg` and `MatchedLegLot` objects
- **`cash_flow.py`**: Public dataclasses (`PeriodMetrics`, `CashFlowPnlReport`) and the thin wrapper that delegates to the heavy implementation.
- **`cash_flow_report.py`**: P&L reports by period (`generate_cash_flow_pnl_report()`). Supports `PeriodType` (daily/weekly/monthly/total) and `AssignmentHandling` enum. Handles contract multiplier (100 for options)
- **`chain_builder.py`**: Detects roll chains from transactions (`detect_roll_chains()`)
- **`stock_lot_builder.py`**: Reconstructs stock lots from option assignments (`rebuild_assignment_stock_lots()`)
- **`stock_lots.py`**: Fetches stock holding summaries (`fetch_stock_lot_summaries()`)
- **`analyzer.py`**: Financial calculations—`calculate_pnl()` and `calculate_breakeven()`
- **`display.py`**: Terminal formatting helpers (`format_currency()`, `format_percent()`, `prepare_chain_display()`)
- **`json_serializer.py`**: JSON conversion and `build_ingest_payload()` for API integration
- **`cli_helpers.py`**: CLI utilities for filters and labels
- **`transaction_loader.py`**: Load persisted transactions
- **`options.py`** & **`transactions.py`**: Helper utilities

### CLI Layer: `src/premiumflow/cli/`

- **`commands.py`**: Main Click group and command registration
- **`analyze.py`**, **`cashflow.py`**, **`legs.py`**, **`lookup.py`**, **`import_command.py`**, **`trace.py`**, **`shares.py`**: Individual command handlers
- **`utils.py`**: CLI utility functions

### Web Layer: `src/premiumflow/web/`

- **`app.py`**: FastAPI app factory with routes for `/`, `/cashflow`, `/imports`, `/imports/{import_id}`, `/legs`, `/stock-lots`
- **`dependencies.py`**: FastAPI dependency injection (exposes `SQLiteRepository`)
- **`templates/`** & **`static/`**: Jinja2 templates and CSS/JS assets

## Key Data Patterns

### CSV Import Pipeline

1. `core/parser.load_option_transactions()` parses CSV and returns `ParsedImportResult`
1. `persistence/storage.store_import_result()` writes to SQLite with duplicate detection
1. Services query via `SQLiteRepository` singleton

### Leg Matching & P&L Workflow

1. `transaction_loader.fetch_normalized_transactions()` retrieves stored data
1. `leg_matching.match_legs_from_transactions()` runs FIFO matching → returns `MatchedLeg` list
1. `cash_flow_report.generate_cash_flow_pnl_report()` aggregates by period (daily/weekly/monthly/total)
1. `json_serializer.serialize_*()` converts to JSON or terminal format

### Web UI Form Submission

1. User uploads CSV with account metadata
1. FastAPI `/imports` endpoint parses, validates, and stores
1. Redirect to `/imports/{import_id}` displays normalized rows, leg matches, stock lots via Jinja2 template

## Important Architectural Notes

### Financial Calculations

- **Always use `Decimal`** for money calculations—never float
- Contract multiplier = 100 (options contracts = 100 shares per contract)
- Amount values preferred from broker CSV (already include multiplier)
- Test coverage is critical for financial calculations

### Database Singleton Pattern

- Both `SQLiteRepository` and `SQLiteStorage` use singleton patterns
- Web UI tests clear singleton caches between test runs for isolation
- Tests use `PREMIUMFLOW_DB_PATH` env var to point to temp databases

### Testing & Coverage

- **Coverage requirement: 85% minimum** enforced by pytest
- Web smoke tests in `tests/test_web_app.py` boot real FastAPI app with temp database
- Use `_persist_import()` and `_seed_assignment_stock_lots()` fixtures to seed data
- Always clear singleton caches in tests to preserve database isolation (`tmp_path` + cleared caches)
- Run `uv run pytest` to generate HTML coverage report at `htmlcov/index.html`

### Code Quality Standards

- Black: 100-character line length
- Ruff: pycodestyle, pyflakes, isort, bugbear checks; max McCabe complexity 10
- mypy: strict type checking with custom config
- Pre-commit hooks validate before push; pre-push hook runs full test suite

### Service Development

- Each service should be self-contained
- Branch from `main` per new service
- Include comprehensive unit tests
- Export public functions in `services/__init__.py`
- Address code review comments (watch for P1/P2/P3 priority badges) before merging

## Code Review Process

- All PRs get automated Codex reviews with **P1/P2/P3 priority badges**
- Address all **P1 issues** before merging (critical functionality changes)
- Use `gh pr view {PR} --json reviews,comments` or run:
  ```bash
  ./scripts/check-pr-feedback.sh {PR_NUMBER}
  ```

### Priority Levels

- **P1**: Critical functionality changes that could break existing behavior
- **P2**: Important improvements or optimizations
- **P3**: Minor suggestions or style improvements

## Dependencies & Environment Variables

- **`PREMIUMFLOW_DB_PATH`**: Override default SQLite database location (~/.premiumflow/premiumflow.db)
- **`uv` manager**: Use `uv lock` to manage lockfile when adding dependencies

## File Organization Highlights

- **Tests**: Organized under `tests/` with unit tests per service, fixtures in `tests/fixtures/`, smoke tests in `test_web_app.py`
- **Docs**: User guides in `docs/users/`, developer guides in `docs/developers/`
- **Scripts**: Utility scripts in `scripts/` (formatted with shfmt)
- **Config**: `pyproject.toml` (project metadata), `mypy.ini`, `.pre-commit-config.yaml`, `.github/workflows/`
