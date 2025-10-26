# Quick Reference for Code Review Comments

## When User Says "Address Code Review Comment"

1. **Run the feedback checker:**
   ```bash
   ./scripts/check-pr-feedback.sh {PR_NUMBER}
   ```

2. **Or manually check:**
   ```bash
   gh api repos/garricn/options/pulls/{PR}/reviews
   gh api repos/garricn/options/pulls/{PR}/reviews/{REVIEW_ID}/comments
   ```

3. **Look for priority badges:**
   - P1 = Critical (must fix)
   - P2 = Important (should fix)
   - P3 = Minor (nice to fix)

4. **Parse the actual feedback** from comment bodies, not just review summaries

## Common Commands
```bash
# Check specific PR
gh pr view {PR} --json reviews,comments

# Get all reviews
gh api repos/garricn/options/pulls/{PR}/reviews

# Get review comments
gh api repos/garricn/options/pulls/{PR}/reviews/{REVIEW_ID}/comments

# Check for priority issues
gh api repos/garricn/options/pulls/{PR}/reviews --jq '.[].id' | while read review_id; do
  gh api repos/garricn/options/pulls/{PR}/reviews/$review_id/comments --jq '.[] | select(.body | contains("P1") or contains("P2") or contains("P3"))'
done
```

## Project-Specific Notes
- This is a financial options trading tool
- Use Decimal for all financial calculations
- Preserve fallback logic when refactoring
- Test edge cases thoroughly
