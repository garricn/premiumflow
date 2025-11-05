# Code Review Guidelines

## Code Review Process

When asked to "address code review comment" or similar:

1. **Always check for review comments first** using:

   ```bash
   gh api repos/garricn/premiumflow/pulls/{PR_NUMBER}/reviews
   ```

1. **For each review, check comments**:

   ```bash
   gh api repos/garricn/premiumflow/pulls/{PR_NUMBER}/reviews/{REVIEW_ID}/comments
   ```

1. **Look for P1/P2/P3 priority badges** in comment bodies

1. **Parse the actual feedback** from the comment body, not just the review summary

## GitHub Commands

- Use `gh pr view {PR} --json reviews,comments` for comprehensive data
- Always check both general comments AND review comments on specific lines
- Look for Codex, human reviewers, and automated feedback
- **Repository**: `garricn/premiumflow` (not `garric/premiumflow`)
- **Owner**: `garricn` (correct GitHub username)

### Quick Commands Cheat Sheet

```bash
# Check specific PR
gh pr view {PR} --json reviews,comments

# Get all reviews
gh api repos/garricn/premiumflow/pulls/{PR}/reviews

# Get review comments
gh api repos/garricn/premiumflow/pulls/{PR}/reviews/{REVIEW_ID}/comments

# Scan for priority markers
gh api repos/garricn/premiumflow/pulls/{PR}/reviews --jq '.[].id' | while read review_id; do
  gh api repos/garricn/premiumflow/pulls/{PR}/reviews/$review_id/comments --jq '.[] | select(.body | contains("P1") or contains("P2") or contains("P3"))'
done
```

## Priority Levels

- **P1**: Critical functionality changes that could break existing behavior
- **P2**: Important improvements or optimizations
- **P3**: Minor suggestions or style improvements

## Review Best Practices

- Address all **P1 issues** before merging
- Use `gh pr view {PR} --json reviews,comments` to see all feedback
- Check both general comments AND review comments on specific lines
