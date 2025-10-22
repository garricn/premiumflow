# Rollchain

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
gh api repos/garricn/rollchain/pulls/{PR}/reviews

# Get review comments for each review
gh api repos/garricn/rollchain/pulls/{PR}/reviews/{REVIEW_ID}/comments
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

### Financial Calculations
- Use `Decimal` for all financial calculations
- Maintain backward compatibility when refactoring
- Test coverage is critical for financial calculations
- Always preserve fallback logic when refactoring

## Installation

```bash
pip install -e .
```

## Usage

```bash
python -m rollchain analyze transactions.csv
python -m rollchain ingest --json-output
python -m rollchain lookup "TSLA 500C 2025-02-21"
```
