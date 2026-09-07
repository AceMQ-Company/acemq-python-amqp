#!/usr/bin/env bash
# Scan a working tree and/or a commit range for AI-assistant attribution.
# Exit 1 if anything is found. Used locally, by the git hooks, and by CI.
#
#   ./attribution-guard.sh                    # tree + commits vs origin/main
#   ./attribution-guard.sh --path ../some-repo
#   ./attribution-guard.sh --range origin/main..HEAD
#   ./attribution-guard.sh --msg-file .git/COMMIT_EDITMSG
#
# Policy: docs/07-repositories-and-governance.md (ADR-010).
set -euo pipefail

PATTERN='claude|anthropic|co-authored-by[[:space:]]*:|generated with|assisted by (an )?ai|ai-generated|🤖'
TARGET="."; RANGE=""; MSG_FILE=""; found=0

while [ $# -gt 0 ]; do
  case "$1" in
    --path)  TARGET="$2"; shift 2 ;;
    --range) RANGE="$2";  shift 2 ;;
    --msg-file) MSG_FILE="$2"; shift 2 ;;
    -h|--help) sed -n '2,10p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

report() { echo "ATTRIBUTION VIOLATION — $1" >&2; found=1; }

# 1. a single commit message (commit-msg hook)
if [ -n "$MSG_FILE" ]; then
  if grep -qiE "$PATTERN" "$MSG_FILE"; then
    report "commit message contains forbidden attribution"
    grep -inE "$PATTERN" "$MSG_FILE" >&2 || true
  fi
  exit "$found"
fi

cd "$TARGET"

# 2. commit messages, author and committer identities in the range
if git rev-parse --git-dir >/dev/null 2>&1; then
  if [ -z "$RANGE" ]; then
    if git rev-parse --verify -q origin/main >/dev/null; then RANGE="origin/main..HEAD"; else RANGE="HEAD"; fi
  fi
  if hits="$(git log --format='%H%n%B%n%an <%ae>%n%cn <%ce>' "$RANGE" 2>/dev/null | grep -inE "$PATTERN" || true)"; then
    if [ -n "$hits" ]; then
      report "commit history in range '$RANGE'"
      echo "$hits" >&2
    fi
  fi
fi

# 3. the working tree
#    The enforcement machinery necessarily contains the forbidden words, and a
#    repository may allow-list additional paths in .attribution-guard-ignore
#    (one grep -E pattern per line, # for comments).
SELF_EXCLUDES=(
  --exclude=attribution-guard.sh --exclude=attribution-guard.yml
  --exclude=install-git-hooks.sh --exclude=commit-msg --exclude=pre-push
  --exclude=CONTRIBUTING.md --exclude=.attribution-guard-ignore
)
# -i, like the commit scans above. Without it this scan was case-sensitive while
# the message scans were not, so "Generated with ..." in a file passed while the
# identical text in a commit message was caught. The words being forbidden are
# ordinary English that tools capitalise however they like.
tree_hits="$(grep -rIilE "$PATTERN" . \
  --exclude-dir=.git --exclude-dir=node_modules --exclude-dir=target \
  --exclude-dir=build --exclude-dir=vendor --exclude-dir=site \
  --exclude-dir=.githooks \
  "${SELF_EXCLUDES[@]}" 2>/dev/null || true)"

if [ -n "$tree_hits" ] && [ -f .attribution-guard-ignore ]; then
  ignore_re="$(grep -vE '^\s*(#|$)' .attribution-guard-ignore | paste -sd'|' -)"
  [ -n "$ignore_re" ] && tree_hits="$(printf '%s\n' "$tree_hits" | grep -vE "$ignore_re" || true)"
fi
if [ -n "$tree_hits" ]; then
  report "files in the working tree"
  echo "$tree_hits" >&2
fi

if [ "$found" -eq 0 ]; then
  echo "attribution-guard: clean"
else
  cat >&2 <<'EOF'

These repositories must contain no AI-assistant attribution in commits,
documentation, code comments, issues, or release notes. Remove the offending
text (and amend or rebase the commits) before pushing.
EOF
fi
exit "$found"
