# Rollchain Project Agent Guidelines

## Code Review Process
When asked to "address code review comment" or similar:

1. **Always check for review comments first** using:
   ```bash
   gh api repos/garricn/rollchain/pulls/{PR_NUMBER}/reviews
   ```

2. **For each review, check comments**:
   ```bash
   gh api repos/garricn/rollchain/pulls/{PR_NUMBER}/reviews/{REVIEW_ID}/comments
   ```

3. **Look for P1/P2/P3 priority badges** in comment bodies

4. **Parse the actual feedback** from the comment body, not just the review summary

## GitHub Commands
- Use `gh pr view {PR} --json reviews,comments` for comprehensive data
- Always check both general comments AND review comments on specific lines
- Look for Codex, human reviewers, and automated feedback
- **Repository**: `garricn/rollchain` (not `garric/rollchain`)
- **Owner**: `garricn` (correct GitHub username)

## GitHub Projects Management
- **Adding issues to projects**: Issues don't automatically appear in projects just because they have labels
- **Required steps**:
  1. Create appropriate labels: `gh label create "label-name" --description "Description" --color "color"`
  2. Add labels to issues: `gh issue edit {ISSUE_NUMBER} --add-label "label-name"`
  3. **CRITICAL**: Explicitly add issues to project: `gh project item-add {PROJECT_NUMBER} --owner garricn --url https://github.com/garricn/rollchain/issues/{ISSUE_NUMBER}`
  4. Add PRs to project: `gh project item-add {PROJECT_NUMBER} --owner garricn --url https://github.com/garricn/rollchain/pull/{PR_NUMBER}`
- **List project items**: `gh project item-list {PROJECT_NUMBER} --owner garricn`
- **List projects**: `gh project list`
- **Project ID for RollChain Refactoring**: 2 (use `gh project list` to verify)

## Project Context
- This is a Python financial options trading analysis tool
- Focus on precision with Decimal arithmetic
- Maintain backward compatibility when refactoring
- Test coverage is critical for financial calculations

## Service Extraction Guidelines
- Each service should be self-contained when possible
- Branch from `main` for each new service
- Include comprehensive unit tests
- Export functions in `services/__init__.py`
- Address code review comments before merging

## Common Issues to Watch For
- P1: Critical functionality changes that could break existing behavior
- P2: Important improvements or optimizations
- P3: Minor suggestions or style improvements
- Always preserve fallback logic when refactoring
- Test edge cases thoroughly for financial calculations
