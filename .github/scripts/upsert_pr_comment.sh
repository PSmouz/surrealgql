#!/usr/bin/env bash
# Create or update a single "sticky" PR comment identified by a hidden marker.
# Usage: upsert_pr_comment.sh <pr-number> <marker> <body-file>
#
# Talks to the GitHub REST API via curl (not the `gh` CLI) so it works on
# self-hosted runners that don't ship `gh`. Requires curl + jq, and GH_TOKEN +
# GITHUB_REPOSITORY in the environment.
set -euo pipefail

PR="$1"
MARKER="$2"
BODY_FILE="$3"
REPO="${GITHUB_REPOSITORY:?GITHUB_REPOSITORY not set}"
TOKEN="${GH_TOKEN:?GH_TOKEN not set}"
API="https://api.github.com"

BODY="$(printf '%s\n%s' "$MARKER" "$(cat "$BODY_FILE")")"
PAYLOAD="$(jq -nc --arg b "$BODY" '{body: $b}')"

AUTH=(
  -H "Authorization: Bearer $TOKEN"
  -H "Accept: application/vnd.github+json"
  -H "X-GitHub-Api-Version: 2022-11-28"
  -H "Content-Type: application/json"
)

# Find an existing sticky comment by its marker, paging through all comments.
ID=""
page=1
while :; do
  resp="$(curl -fsSL "${AUTH[@]}" "$API/repos/$REPO/issues/$PR/comments?per_page=100&page=$page")"
  [ "$(printf '%s' "$resp" | jq 'length')" -eq 0 ] && break
  ID="$(printf '%s' "$resp" | jq -r --arg m "$MARKER" 'map(select(.body | startswith($m))) | (.[0].id // empty)')"
  [ -n "$ID" ] && break
  page=$((page + 1))
done

if [ -n "$ID" ]; then
  printf '%s' "$PAYLOAD" | curl -fsSL -X PATCH "${AUTH[@]}" \
    "$API/repos/$REPO/issues/comments/$ID" --data-binary @- >/dev/null
  echo "Updated comment $ID"
else
  printf '%s' "$PAYLOAD" | curl -fsSL -X POST "${AUTH[@]}" \
    "$API/repos/$REPO/issues/$PR/comments" --data-binary @- >/dev/null
  echo "Created comment"
fi
