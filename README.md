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
- Run `uv run black src tests`, `uv run ruff check src tests --fix`, and the Markdown formatter before sending a PR; CI enforces all three checks.
- Other file types have their own formatter issues tracked separately.

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

To add or upgrade dependencies, edit `pyproject.toml` and regenerate the lockfile:

```bash
uv lock --upgrade-package <package-name>     # upgrade specific packages
uv lock                                      # refresh everything after edits
```

## Usage

```bash
uv run premiumflow analyze transactions.csv
uv run premiumflow import --json-output
uv run premiumflow lookup "TSLA 500C 2025-02-21"
uv run premiumflow trace "TSLA $550 Call" all_transactions.csv
```
