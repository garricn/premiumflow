# Options Project Refactoring Plan

## ✅ Phase 1: Test Suite (COMPLETE)

**Status**: 26 tests passing in `test_rollchain.py` (legacy)

### What We Accomplished
- Created comprehensive test suite (`test_rollchain.py` (legacy)) with 26 passing tests
- All core functions tested and working in `roll.py`:
  - `format_position_spec` - Convert descriptions to lookup format
  - `parse_lookup_input` - Parse position specifications
  - `find_chain_by_position` - Lookup chains by position
  - `detect_roll_chains` - Build roll chains from transactions
- Set up virtual environment and pytest
- Adopted `uv` for dependency management with a committed `uv.lock`
- Removed redundant old test files

### Test Coverage
1. **CSV Parsing** (6 tests)
   - Options transaction detection
   - Call/Put option detection  
   - Null handling

2. **Roll Chain Detection** (3 tests)
   - Closed chain detection
   - Open chain detection
   - Transaction ordering

3. **P&L Calculations** (5 tests)
   - Credits calculation
   - Debits calculation
   - Net P&L calculation
   - Fees calculation ($0.04/contract)
   - Breakeven price calculation

4. **Position Formatting** (7 tests)
   - Format position specs (TICKER $STRIKE TYPE DATE)
   - Parse lookup input
   - Validation and error handling

5. **Lookup Functionality** (3 tests)
   - Find chains by position
   - Handle not found cases
   - Different strikes in same chain

6. **Multi-Roll Chains** (2 tests)
   - Detect chains with 3+ rolls
   - Calculate multi-roll P&L

## 📋 Phase 2: Project Structure Setup (NEXT)

### Goals
- Create proper Python package structure
- Set up `pyproject.toml` for modern Python packaging
- Organize code into modules

### Proposed Structure
```
options/
├── pyproject.toml          # Project config & dependencies
├── README.md
├── .gitignore
├── uv.lock                 # uv dependency lockfile (committed)
├── src/
│   └── options/
│       ├── __init__.py
│       ├── __main__.py     # CLI entry point
│       ├── cli/
│       │   ├── __init__.py
│       │   ├── commands.py  # registers subcommands
│       │   ├── analyze.py
│       │   ├── ingest.py
│       │   ├── lookup.py
│       │   └── trace.py
│       ├── core/
│       │   ├── __init__.py
│       │   ├── models.py   # Transaction, Options models
│       │   └── parser.py   # CSV parsing
│       ├── services/
│       │   ├── __init__.py
│       │   ├── chain_builder.py  # Chain detection
│       │   └── analyzer.py       # P&L calculations
│       └── formatters/
│           ├── __init__.py
│           └── output.py   # Display logic
├── scripts/
│   └── migrate_cli_shim.py  # Temporary: preserve old CLI behavior during migration
├── tests/
│   ├── __init__.py
│   ├── conftest.py
│   ├── test_parser.py
│   ├── test_chain_builder.py
│   ├── test_analyzer.py
│   ├── test_formatters.py
│   └── fixtures/
│       ├── tsla_rc-001-closed.csv
│       └── tsla_rc-001-open.csv
└── examples/
    └── sample_transactions.csv
```

### Technology Stack
- **Package Management**: `pyproject.toml` (PEP 517/518)
- **CLI Framework**: `click`
- **Data Validation**: `pydantic`
- **Terminal UI**: `rich`
- **Testing**: `pytest` ✅
 - **Numeric precision**: `decimal.Decimal` for prices/amounts/P&L

## 📋 Phase 3: Extract Core Models

### Data Classes to Create
1. **Transaction** (pydantic model)
   - activity_date: datetime
   - instrument: str
   - description: str
   - trans_code: str (BTC, STO, BTC, STC, OASGN)
   - quantity: int
   - price: Decimal
   - amount: Decimal

2. **Options** (pydantic model)
   - chain_id: str (e.g., "RC-001")
   - ticker: str
   - status: Literal["OPEN", "CLOSED"]
   - transactions: List[Transaction]
   - start_date: datetime
   - end_date: datetime
   - roll_count: int
   - total_credits: Decimal
   - total_debits: Decimal
   - net_pnl: Decimal

3. **PositionSpec** (pydantic model)
   - ticker: str
   - strike: Decimal
   - option_type: Literal["CALL", "PUT"]
   - expiration: datetime

4. (Optional) **OptionContract**
   - ticker: str
   - expiration: datetime
   - option_type: Literal["CALL", "PUT"]
   - strike: Decimal

### Parsing & Money Policies
- Centralize parsing of broker amounts/prices (parentheses for negatives, commas, `$`)
- Use `Decimal` everywhere for prices, amounts, and P&L; avoid floats in core/services
- Normalize dates to `datetime.date` internally; accept `M/D/YYYY` input

### Fees Policy
- Robinhood CSV does not include regulatory options fees.
- Apply a constant per‑contract‑leg fee of `$0.04` for now.
- Fee calculation: `fees = 0.04 * quantity` for each option leg; chain fees are the sum across legs.
- Future (deferred): allow overriding the fee via CLI flag or environment variable if broker changes.

## 📋 Phase 4: Extract Services

### Services to Create
1. **CSVParser** (`core/parser.py`)
   - `parse_csv(file_path)` → List[Transaction]
   - `is_options_transaction(row)` → bool
   - Handles null values, duplicates
   - `parse_description(desc)` → OptionContract (ticker, expiration, type, strike)

2. **ChainBuilder** (`services/chain_builder.py`)
   - `detect_roll_chains(transactions)` → List[Options]
   - `build_chain(transactions)` → Options
   - Handles open/closed status
   - Roll primitive: same‑day close+open pairs
     - Short: `BTC → STO`; Long: `STC → BTO`
     - Same quantity, same type (Call/Put), different strike OR expiration
   - Link legs using structured contract fields, not raw `Description` strings
   - Support deduplication and chronological ordering

3. **PnLAnalyzer** (`services/analyzer.py`)
   - `calculate_credits(chain)` → Decimal
   - `calculate_debits(chain)` → Decimal
   - `calculate_fees(chain)` → Decimal
   - `calculate_breakeven(chain)` → Decimal
   - `calculate_realized_pnl(chain)` → List[LegPnL]

4. **PositionFormatter** (`formatters/output.py`)
   - `format_position_spec(description)` → str
   - `parse_lookup_input(lookup_str)` → PositionSpec
   - `display_chain(chain)` → None
   - `display_chain_summary(chain)` → None

5. **Lookup** (`services/lookup.py`)
   - `find_chain_by_position(file_path, PositionSpec)` → Optional[Options]

### Deduplication Policy
- Prefer broker‑supplied unique IDs when available
- Fallback composite key (until IDs exist):
  - `Activity Date`, `Instrument`, `Trans Code`, `Quantity`, `Price`, `Amount`, `Description`
  - Keep `Process Date`/`Settle Date` available for troubleshooting; not part of the default key

## 📋 Phase 5: Refactor CLI

### Commands to Implement
1. **`options ingest`** (primary)
   - Backward‑compatible alias: `injest` (deprecated; warn on use)
   - `--options` flag
   - `--ticker TICKER` filter
   - `--strategy {calls,puts}` filter
   - `--file FILE` input
   - `--json` output for automation (serialize chains/rolls)

2. **`options lookup`**
   - Position argument: "TICKER $STRIKE TYPE DATE"
   - `--file FILE` input

### Using Click
```python
@click.group()
def cli():
    """Options - Options roll chain analysis tool"""
    pass

@cli.command(name='ingest')
@click.option('--options', is_flag=True)
@click.option('--ticker')
@click.option('--strategy', type=click.Choice(['calls', 'puts']))
@click.option('--file', default='all_transactions.csv')
@click.option('--json', is_flag=True, help='Output JSON')
def ingest(options, ticker, strategy, file, json):
    """Analyze options transactions"""
    pass

# Backward‑compat alias (deprecated)
cli.add_command(ingest, name='injest')
```

### JSON Output Schema (initial)
- Serialization: decimals emitted as strings; dates as `M/D/YYYY`.
- `chains`: list of chain objects with:
  - `chain_id`, `ticker`, `status` (OPEN|CLOSED), `start_date`, `end_date`, `roll_count`,
    `total_credits`, `total_debits`, `net_pnl`
  - `transactions`: list of legs with `activity_date`, `trans_code`, `quantity`, `price`, `amount`, `description`
  - `fees`: total chain fees (string decimal)
  - (Optional) `realized_legs`: list of realized P&L per matched open/close

## 📋 Phase 6: Update Tests

### Test Updates
1. **Split tests by module**
   - Current: All 26 tests centralized in `test_rollchain.py` (legacy)
   - Split into:
     - `test_parser.py` - CSV parsing
     - `test_chain_builder.py` - Chain detection
     - `test_analyzer.py` - P&L calculations
     - `test_formatters.py` - Output formatting
     - `test_lookup.py` - Lookup functionality

2. **Add conftest.py with fixtures**
   - Sample transactions fixture
   - Closed chain fixture
   - Open chain fixture
   - Multi-roll chain fixture

3. **Keep integration tests**
   - End-to-end CLI tests
   - Real CSV file tests
   - JSON output validation

4. **Migrate to Decimal**
   - Replace float asserts with `Decimal` comparisons (use `quantize` for rounding)
   - Update test data to use `Decimal` for prices/amounts

## 📋 Phase 7: Prepare for Extensions

### Future Additions
1. **Web Interface**
   - FastAPI backend
   - React frontend
   - REST API for chain analysis

2. **MCP Tools**
   - Chain lookup tool
   - P&L analysis tool
   - Position tracking tool

3. **Database Storage**
   - SQLite for local storage
   - Track chains over time
   - Historical analysis

## 🎯 Next Steps

1. ✅ Complete test suite (26 tests passing in `test_rollchain.py` (legacy))
2. ⏭️ Create `pyproject.toml` and package structure
3. ⏭️ Implement models (Pydantic + Decimal)
4. ⏭️ Extract services (parser, chain_builder, analyzer, lookup)
5. ✅ Refactor CLI to Click and split commands into dedicated modules (analyze/ingest/lookup/trace)
6. ✅ Split and reorganize tests by module; migrate to Decimal

## 📝 Notes

- **Incremental approach**: Refactor one module at a time
- **Keep tests passing**: Run tests after each change
- **Version control**: Commit frequently with clear messages
- **Dependencies**: Add to `pyproject.toml` as we go
 - **Backwards compatibility**: Maintain current CLI (`roll.py injest ...`) via a shim until migration completes
 - **Fees**: Keep `$0.04/contract` as default; make configurable later
 - **Performance**: Stream CSV parsing; avoid unnecessary large materializations
 - **Scope (v1)**: Focus on single‑leg, same‑quantity rolls (BTC→STO or STC→BTO). Partial rolls and multi‑leg spreads are out of scope for now.

## 🔗 Resources

- [PEP 517](https://peps.python.org/pep-0517/) - Build system
- [PEP 518](https://peps.python.org/pep-0518/) - pyproject.toml
- [Click Documentation](https://click.palletsprojects.com/)
- [Pydantic Documentation](https://docs.pydantic.dev/)
- [Rich Documentation](https://rich.readthedocs.io/)
