#!/usr/bin/env bash
# Everything that must be true before this repository's tag is pushed.
#
#   ./etc/release-preflight.sh 0.7.3
#
# Read-only. Run it as often as you like; it changes nothing.
#
# This is the copy that lives inside each repository, so the pre-push hook can
# run it without the surrounding workspace. The workspace keeps a larger one
# (scripts/release-preflight.sh) that also asks GitHub about secrets and CI and
# can sweep every repository at once. This one deliberately checks only what is
# knowable from a clone, because the hook has to work on a machine that has
# neither the workspace nor a GitHub token.
#
# Why a hook at all. A tag is the one irreversible act here: Go's module proxy
# caches a version for ever and the package feeds are public the moment the
# release job pushes. Every check below existed somewhere already — in a release
# workflow, where it runs *after* the tag exists and is therefore too late to
# prevent anything. The point of this file is to move them before the push.
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"

VERSION="${1:-}"
[ -n "$VERSION" ] || { echo "usage: $(basename "$0") <version>   e.g. 0.7.3" >&2; exit 2; }
VERSION="${VERSION#v}"

FAIL=0
if [ -t 1 ]; then RED=$'\033[31m'; GREEN=$'\033[32m'; YELLOW=$'\033[33m'; OFF=$'\033[0m'
else RED=''; GREEN=''; YELLOW=''; OFF=''; fi
ok()   { printf '%s  ok%s %s\n' "$GREEN" "$OFF" "$*"; }
bad()  { FAIL=1; printf '%sfail%s %s\n' "$RED" "$OFF" "$*" >&2; }
note() { printf '%swarn%s %s\n' "$YELLOW" "$OFF" "$*" >&2; }

case "$VERSION" in
  [0-9]*.[0-9]*.[0-9]*) ;;
  *) bad "'$VERSION' is not a semantic version"; exit 1 ;;
esac

# ---- the tag must not already exist ----
# Asked of the remote as well as the clone: a tag that exists only on GitHub
# still makes the release a no-op, and a stale clone cannot see it.
if git rev-parse -q --verify "refs/tags/v$VERSION" >/dev/null 2>&1; then
  note "v$VERSION already exists locally (expected if you are pushing the tag you just made)"
fi
if git ls-remote --exit-code --tags origin "refs/tags/v$VERSION" >/dev/null 2>&1; then
  bad "v$VERSION already exists on the remote; a version is never reissued"
else
  ok "v$VERSION is not on the remote yet"
fi

# ---- the working tree is what will be tagged ----
if [ -n "$(git status --porcelain)" ]; then
  bad "uncommitted changes; they are not in the tag"
else
  ok "working tree is clean"
fi

BRANCH="$(git rev-parse --abbrev-ref HEAD)"
git fetch -q origin "$BRANCH" 2>/dev/null || true
if git rev-parse -q --verify "origin/$BRANCH" >/dev/null 2>&1; then
  ahead=$(git rev-list --count "origin/$BRANCH..HEAD")
  behind=$(git rev-list --count "HEAD..origin/$BRANCH")
  if [ "$ahead" != 0 ] || [ "$behind" != 0 ]; then
    # Tagging while behind gives a tag whose commit is not on the branch. The
    # release still builds, because it builds from the tag, but the released
    # commit is not in the branch history. This has happened more than once.
    bad "$ahead ahead / $behind behind origin/$BRANCH; pull before tagging"
  else
    ok "in sync with origin/$BRANCH"
  fi
fi

# ---- the release-line guard ----
# Several release workflows carry `case "$VERSION" in 0.7.*)`, which has to be
# lifted by hand at every minor. Forgetting costs a tag: the job fails, and the
# tag it failed on is already public.
WORKFLOW=.github/workflows/release.yml
if [ -f "$WORKFLOW" ]; then
  guard=$(grep -oE '^[[:space:]]+[0-9]+\.[0-9]+\.\*\)' "$WORKFLOW" | tr -d ' )' | head -1 || true)
  if [ -n "$guard" ]; then
    # shellcheck disable=SC2254
    case "$VERSION" in
      $guard) ok "the release line admits $VERSION (guard is $guard)" ;;
      *) bad "release.yml releases $guard only; lift the guard before tagging $VERSION" ;;
    esac
  fi
else
  # Not fatal. Two repositories publish by hand and have no release workflow,
  # which is worth knowing at exactly this moment but is not this script's to
  # refuse.
  note "no .github/workflows/release.yml; nothing here publishes this tag automatically"
fi

# ---- every in-tree version source agrees ----
check_version_in() {  # check_version_in <path> <extended regex>
  [ -f "$1" ] || return 0
  local declared
  declared=$(grep -oE "$2" "$1" 2>/dev/null | head -1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+[^"<]*' | head -1 || true)
  [ -n "$declared" ] || return 0
  if [ "$declared" = "$VERSION" ]; then ok "$1 says $VERSION"
  else bad "$1 says $declared, not $VERSION"; fi
}
check_version_in Directory.Build.props '<Version>([0-9][^<]*)</Version>'
check_version_in pyproject.toml '^version = "([^"]+)"'
for f in src/*/__init__.py; do check_version_in "$f" '^__version__ = "([^"]+)"'; done
for f in lib/acemq/*/version.rb; do check_version_in "$f" 'VERSION = "([^"]+)"'; done
check_version_in amqp/interceptors.go 'Version = "([^"]+)"'

# ---- the changelog ----
if [ -f CHANGELOG.md ]; then
  if ! grep -qE "^## \[$VERSION\]" CHANGELOG.md; then
    bad "CHANGELOG.md has no [$VERSION] section"
  else
    dated=$(grep -E "^## \[$VERSION\]" CHANGELOG.md | grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}' | head -1 || true)
    if [ -z "$dated" ]; then bad "the [$VERSION] heading carries no date"
    else ok "CHANGELOG [$VERSION] dated $dated"; fi
  fi

  # The previous release needs a heading of its own. Checking only the version
  # being cut misses how this actually goes wrong: two repositories were tagged
  # v0.1.0 and never rolled their changelog, so each carried a single
  # [Unreleased] heading with the whole release under it and the published
  # artifact documented itself as containing nothing released. Nothing noticed
  # for seventeen days, because every check anyone ran was about the next one.
  previous=$(git tag --sort=-v:refname | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' | grep -vx "v$VERSION" | head -1 || true)
  if [ -n "$previous" ]; then
    if grep -qE "^## \[${previous#v}\]" CHANGELOG.md; then
      ok "the previous release $previous has its own heading"
    else
      bad "$previous is released but CHANGELOG.md has no [${previous#v}] section; roll it before adding another"
    fi
  fi
fi

# ---- the authorship policy ----
for guard_script in etc/attribution-guard.sh scripts/attribution-guard.sh; do
  if [ -x "$guard_script" ]; then
    if "$guard_script" --path . >/dev/null 2>&1; then ok "attribution guard clean"
    else bad "the attribution guard flags something in the tree"; fi
    break
  fi
done

# ---- Go: the nested modules ----
# A nested module is published by its own path-prefixed tag, which the root tag
# does not carry, and its go.mod must require a parent version that will exist.
# 0.6.0 shipped with every one of them requiring 0.5.0; it compiled, so nothing
# warned, and Go tags cannot be withdrawn.
if [ -f go.mod ]; then
  wrong=(); missing=()
  while IFS= read -r module; do
    [ -n "$module" ] || continue
    required=$(grep -oE 'github.com/AceMQ-Company/[a-z-]+ v[0-9][^ ]*' "$module/go.mod" 2>/dev/null | awk '{print $2}' | head -1 || true)
    [ "$required" = "v$VERSION" ] || wrong+=("$module wants ${required:-nothing}")
    git ls-remote --exit-code --tags origin "refs/tags/$module/v$VERSION" >/dev/null 2>&1 || missing+=("$module")
  done <<< "$(find . -mindepth 2 -name go.mod -not -path './.git/*' | sed -e 's|^\./||' -e 's|/go.mod$||' | sort)"

  if [ ${#wrong[@]} -gt 0 ]; then
    bad "nested modules require the wrong parent: ${wrong[*]}"
  else
    ok "every nested module requires the parent at v$VERSION"
  fi
  if [ ${#missing[@]} -gt 0 ]; then
    note "tag these from the root tag's commit after tagging: $(printf '%s/v'"$VERSION"' ' "${missing[@]}")"
  fi
fi

echo
if [ "$FAIL" -ne 0 ]; then
  printf '%sNot ready to tag.%s A tag cannot be withdrawn: fix the above first.\n' "$RED" "$OFF" >&2
  exit 1
fi
printf '%sClear to tag.%s\n' "$GREEN" "$OFF"
