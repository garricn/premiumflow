#!/bin/bash
# Script to comprehensively check PR feedback and review comments

PR_NUMBER=$1

if [ -z "$PR_NUMBER" ]; then
    echo "Usage: $0 <PR_NUMBER>"
    echo "Example: $0 38"
    exit 1
fi

echo "🔍 Checking PR #$PR_NUMBER for all feedback..."
echo

# 1. Get basic PR info
echo "📋 PR Information:"
gh pr view $PR_NUMBER --json title,state,author,url
echo

# 2. Get all reviews
echo "👀 Reviews:"
gh api repos/garricn/options/pulls/$PR_NUMBER/reviews --jq '.[] | {id: .id, author: .user.login, state: .state, body: .body[0:100]}'
echo

# 3. Get general comments
echo "💬 General Comments:"
gh pr view $PR_NUMBER --json comments --jq '.comments[] | {author: .author.login, body: .body[0:100]}'
echo

# 4. Get detailed review comments
echo "🔍 Detailed Review Comments:"
REVIEWS=$(gh api repos/garricn/options/pulls/$PR_NUMBER/reviews --jq '.[].id')

for review_id in $REVIEWS; do
    echo "--- Review ID: $review_id ---"
    gh api repos/garricn/options/pulls/$PR_NUMBER/reviews/$review_id/comments --jq '.[] | {path: .path, position: .position, body: .body[0:200]}'
    echo
done

# 5. Look for priority badges
echo "🚨 Priority Issues (P1/P2/P3):"
gh api repos/garricn/options/pulls/$PR_NUMBER/reviews --jq '.[].id' | while read review_id; do
    gh api repos/garricn/options/pulls/$PR_NUMBER/reviews/$review_id/comments --jq '.[] | select(.body | contains("P1") or contains("P2") or contains("P3")) | {priority: (.body | match("P[123]") | .string), body: .body[0:300]}'
done
