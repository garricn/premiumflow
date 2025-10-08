# RollChain Project Refactoring Plan

## ✅ Phase 1: Test Suite (COMPLETED)

**Status**: All 26 tests passing

### What We Accomplished
- Created comprehensive test suite (`test_rollchain_comprehensive.py`)
- Set up virtual environment with pytest
- Created `requirements.txt` for dependency management
- All current functionality is now tested and verified

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
rollchain/
├── pyproject.toml          # Project config & dependencies
├── README.md
├── .gitignore
├── requirements.txt        # ✅ Already created
├── src/
│   └── rollchain/
│       ├── __init__.py
│       ├── __main__.py     # CLI entry point
│       ├── cli/
│       │   ├── __init__.py
│       │   └── commands.py # injest, lookup commands
│       ├── core/
│       │   ├── __init__.py
│       │   ├── models.py   # Transaction, RollChain models
│       │   └── parser.py   # CSV parsing
│       ├── services/
│       │   ├── __init__.py
│       │   ├── chain_builder.py  # Chain detection
│       │   └── analyzer.py       # P&L calculations
│       └── formatters/
│           ├── __init__.py
│           └── output.py   # Display logic
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

2. **RollChain** (pydantic model)
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

## 📋 Phase 4: Extract Services

### Services to Create
1. **CSVParser** (`core/parser.py`)
   - `parse_csv(file_path)` → List[Transaction]
   - `is_options_transaction(row)` → bool
   - Handles null values, duplicates

2. **ChainBuilder** (`services/chain_builder.py`)
   - `detect_roll_chains(transactions)` → List[RollChain]
   - `build_chain(transactions)` → RollChain
   - Handles open/closed status

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

## 📋 Phase 5: Refactor CLI

### Commands to Implement
1. **`rollchain injest`**
   - `--options` flag
   - `--ticker TICKER` filter
   - `--strategy {calls,puts}` filter
   - `--file FILE` input

2. **`rollchain lookup`**
   - Position argument: "TICKER $STRIKE TYPE DATE"
   - `--file FILE` input

### Using Click
```python
@click.group()
def cli():
    """RollChain - Options roll chain analysis tool"""
    pass

@cli.command()
@click.option('--options', is_flag=True)
@click.option('--ticker')
@click.option('--strategy', type=click.Choice(['calls', 'puts']))
@click.option('--file', default='all_transactions.csv')
def injest(options, ticker, strategy, file):
    """Analyze options transactions"""
    pass
```

## 📋 Phase 6: Update Tests

### Test Updates
1. **Split tests by module**
   - `test_parser.py` - CSV parsing
   - `test_chain_builder.py` - Chain detection
   - `test_analyzer.py` - P&L calculations
   - `test_formatters.py` - Output formatting

2. **Add conftest.py with fixtures**
   - Sample transactions fixture
   - Closed chain fixture
   - Open chain fixture
   - Multi-roll chain fixture

3. **Keep integration tests**
   - End-to-end CLI tests
   - Real CSV file tests

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

1. ✅ Create comprehensive test suite
2. ⏭️ Create `pyproject.toml` and package structure
3. ⏭️ Extract models using Pydantic
4. ⏭️ Refactor one service at a time (keeping tests green)
5. ⏭️ Update CLI to use Click
6. ⏭️ Split and reorganize tests

## 📝 Notes

- **Incremental approach**: Refactor one module at a time
- **Keep tests passing**: Run tests after each change
- **Version control**: Commit frequently with clear messages
- **Dependencies**: Add to `pyproject.toml` as we go

## 🔗 Resources

- [PEP 517](https://peps.python.org/pep-0517/) - Build system
- [PEP 518](https://peps.python.org/pep-0518/) - pyproject.toml
- [Click Documentation](https://click.palletsprojects.com/)
- [Pydantic Documentation](https://docs.pydantic.dev/)
- [Rich Documentation](https://rich.readthedocs.io/)

