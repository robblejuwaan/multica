#!/usr/bin/env bash
# Print a proven `codex-p1-ack` marker for Robb to paste into a PR comment.
#
# WHY THIS EXISTS (SAN-338). Every Santos agent authenticates to GitHub as the
# same account Robb does, so GitHub cannot tell a Robb acknowledgement from an
# agent one. SAN-321 shipped a manual branch that took `actor=Robb;
# source=manual` on trust, and an agent used it to publish "codex-p1-ack
# recorded by Robb" on martin#46 with no involvement from Robb. The gate now
# requires an HMAC over the PR, keyed by CODEX_ACK_MANUAL_SECRET, before it will
# accept a manual acknowledgement at all. This script produces that marker.
#
# WHAT IT BUYS, STATED HONESTLY. GitHub hands repository secrets to workflow
# runs on ANY branch, and the agents' token can push a branch, so this secret is
# reachable by anything with write access here. A passing manual acknowledgement
# therefore shows the secret was held, not that Robb was present, and the status
# says so — `with the repo ack secret (not proof of Robb)`. What it removes is
# the cheap path: an agent following its instructions has no proof in hand, and
# taking one leaves a branch and a run behind. Per-agent GitHub identities are
# the real fix and are still deferred.
#
# USAGE
#   scripts/codex-ack-marker.sh <pr-number> [owner/repo]
#
# It prompts for the secret without echoing it and prints the marker line.
# Paste that line as a PR comment, then apply the `codex-p1-ack` label within
# ten minutes, then post any comment to re-run the gate.
#
# THE SECRET lives in this repo's Actions secrets as CODEX_ACK_MANUAL_SECRET and
# in Robb's password store. It must NOT be written to a file, an env file, or a
# shell history on the box the agents run on — that is the first place they would
# look. This script reads it from a prompt rather than an argument or an env var,
# and hands it to python over a pipe rather than on a command line, so it stays
# out of shell history, out of /proc/*/cmdline and out of /proc/*/environ. Those
# are all readable by any process running as the same user, which on that box
# means every agent.
#
# A nonce is single-use per PR: acknowledging the same PR a second time needs a
# fresh marker, which is what running this again gives you.

set -euo pipefail

pr="${1:-}"
repo="${2:-}"

if [ -z "$pr" ]; then
  echo "usage: $0 <pr-number> [owner/repo]" >&2
  exit 64
fi
case "$pr" in
  ''|*[!0-9]*)
    echo "error: PR number must be digits, got '$pr'" >&2
    exit 64
    ;;
esac

if [ -z "$repo" ]; then
  # Derive owner/repo from the checkout so the signed message matches what the
  # workflow builds from GITHUB_REPOSITORY. Handles both remote spellings:
  # https://github.com/OWNER/REPO(.git) and git@github.com:OWNER/REPO(.git).
  origin=$(git config --get remote.origin.url 2>/dev/null || true)
  if [ -z "$origin" ]; then
    echo "error: no git remote 'origin'; pass owner/repo as the second argument" >&2
    exit 64
  fi
  repo=${origin%.git}
  repo=${repo#*github.com}
  repo=${repo#:}
  repo=${repo#/}
fi
case "$repo" in
  */*/*|*/)
    echo "error: could not read owner/repo (got '$repo'); pass it as the second argument" >&2
    exit 64
    ;;
  */*) ;;
  *)
    echo "error: could not read owner/repo (got '$repo'); pass it as the second argument" >&2
    exit 64
    ;;
esac
# CASE-NORMALISED, because the workflow signs a name from a different source.
# It uses GITHUB_REPOSITORY, which is GitHub's canonical casing; this script
# uses `remote.origin.url` (or argv), which is whatever was typed. On a repo
# with a capital in its name, a differently-cased local remote mints a proof
# that will not verify. It fails safe, but it reads as "the manual path is
# broken" to whoever hits it. Owner and repo names are case-insensitively
# unique on GitHub, so lowercasing both sides is a total normalisation with no
# network call. `A-Z` rather than `[:upper:]`: repo names are ASCII, and the
# class form is locale-dependent. Codex P2, SAN-350.
repo=$(printf '%s' "$repo" | tr 'A-Z' 'a-z')

printf 'CODEX_ACK_MANUAL_SECRET (not echoed): ' >&2
read -rs secret
printf '\n' >&2
if [ -z "$secret" ]; then
  # An empty key still produces a stable HMAC that anyone could recompute, so
  # refuse rather than emit a marker that looks proven and is not.
  echo "error: empty secret; refusing to produce a marker" >&2
  exit 65
fi

nonce=$(openssl rand -hex 8)
proof=$(
  printf '%s' "$secret" | python3 -c '
import hashlib, hmac, sys
key = sys.stdin.buffer.read()
print(hmac.new(key, sys.argv[1].encode(), hashlib.sha256).hexdigest())
' "codex-p1-ack:v1:${repo}#${pr}:${nonce}"
)

printf '<!-- codex-p1-ack: actor=Robb; source=manual; nonce=%s; proof=%s -->\n' "$nonce" "$proof"
