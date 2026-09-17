#!/usr/bin/env bash
#
# Apply a repository's local git configuration: commit identity, SSH
# signing, and workflow defaults (pull.rebase, fsmonitor, rerere, etc.).
#
# DCO sign-offs are not configured here: git reads no setting that enables
# --signoff by default (gitfaq(7)), so a config line would advertise
# sign-offs that never happen. The dco-signoff commit-msg hook applies
# them; install the pre-commit hooks instead.
#
# No contributor-specific values are hard-coded. The script auto-discovers
# the git identity and SSH signing key, optionally confirms them
# interactively, and writes them to the local repository config. It allows
# contributor settings to be supplied explicitly so CI and agents can run it
# without a terminal.
#
# Usage:
#   scripts/setup-git-config.sh [options]
#
# Options:
#   --name <name>          commit identity (default: discovered)
#   --email <email>        commit email (default: discovered)
#   --signing-key <path>   SSH public signing key (default: discovered)
#   --yes                  do not prompt; accept discovered values unchanged
#   -h, --help             show this help and exit
#
# Without --yes and without explicit identity options, the script prompts to
# confirm the discovered values and allows each to be edited before it is
# applied.
#
# Idempotent: each key is read before it is written, so re-running the script
# is a no-op when every value is already correct. Operates only on the local
# repository config (--local), so a fresh clone takes the project settings
# without touching the contributor's global git config.
#
# Discovery order for the signing key: ~/.ssh/github_ed25519.pub, then
# ~/.ssh/id_ed25519.pub, then ~/.ssh/id_rsa.pub. An existing key is verified
# so the next signed commit does not fail with a missing-key error.

set -euo pipefail

name=""
email=""
signing_key=""
assume_yes=0

usage() {
    sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//'
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --name)
            name="$2"; shift 2 ;;
        --email)
            email="$2"; shift 2 ;;
        --signing-key)
            signing_key="$2"; shift 2 ;;
        --yes)
            assume_yes=1; shift ;;
        -h|--help)
            usage; exit 0 ;;
        *)
            echo "error: unknown option: $1" >&2
            usage >&2
            exit 2 ;;
    esac
done

if ! git rev-parse --git-dir >/dev/null 2>&1; then
    echo "error: not inside a git repository" >&2
    exit 1
fi

# ---- Discover defaults (only used when not supplied explicitly) -----------

discover_name() {
    local existing
    existing="$(git config --global user.name 2>/dev/null || true)"
    if [ -n "$existing" ]; then
        printf '%s\n' "$existing"
        return
    fi
    if command -v getent >/dev/null 2>&1; then
        getent passwd "${USER:-$(id -un)}" | cut -d: -f5 | sed 's/,.*//'
    else
        printf '%s\n' "${USER:-$(id -un)}"
    fi
}

discover_email() {
    local existing
    existing="$(git config --global user.email 2>/dev/null || true)"
    if [ -n "$existing" ]; then
        printf '%s\n' "$existing"
        return
    fi
    printf '%s@%s\n' "${USER:-$(id -un)}" "${HOSTNAME:-localhost}"
}

discover_key() {
    local candidate
    for candidate in \
        "$HOME/.ssh/github_ed25519.pub" \
        "$HOME/.ssh/id_ed25519.pub" \
        "$HOME/.ssh/id_rsa.pub"; do
        if [ -f "$candidate" ]; then
            printf '%s\n' "$candidate"
            return
        fi
    done
}

[ -z "$name" ] && name="$(discover_name)"
[ -z "$email" ] && email="$(discover_email)"
[ -z "$signing_key" ] && signing_key="$(discover_key)"

# ---- Interactive confirmation (skipped with --yes) -------------------------

if [ "$assume_yes" -eq 0 ] && [ -t 0 ]; then
    printf 'Detected git configuration:\n'
    printf '  name        : %s\n' "$name"
    printf '  email       : %s\n' "$email"
    printf '  signing key : %s\n' "${signing_key:-<none found>}"
    printf 'Use these values? [Y/n] '
    read -r reply
    case "${reply:-Y}" in
        [Yy]*) ;;
        *)
            printf 'Enter git name [%s]: ' "$name"; read -r -e in; [ -n "$in" ] && name="$in"
            printf 'Enter git email [%s]: ' "$email"; read -r -e in; [ -n "$in" ] && email="$in"
            printf 'Enter SSH signing key path [%s]: ' "$signing_key"; read -r -e in; [ -n "$in" ] && signing_key="$in"
            ;;
    esac
fi

set_config() {
    local key="$1"
    local value="$2"
    local current
    current="$(git config --local --get "$key" 2>/dev/null || true)"
    if [ "$current" = "$value" ]; then
        printf '  ok   %s\n' "$key"
    else
        git config --local "$key" "$value"
        if [ -z "$current" ]; then
            printf '  set  %s = %s\n' "$key" "$value"
        else
            printf '  set  %s = %s (was %s)\n' "$key" "$value" "$current"
        fi
    fi
}

printf 'Applying local git config:\n'
set_config user.name               "$name"
set_config user.email              "$email"
set_config user.signingkey         "$signing_key"
set_config commit.gpgsign          true
set_config commit.verbose          true
set_config gpg.format              ssh
set_config pull.rebase             true
set_config fetch.writeCommitGraph  true
set_config core.fsmonitor          true
set_config core.untrackedCache     true
set_config rebase.autoSquash       true
set_config rerere.enabled          true
set_config diff.algorithm          histogram

if [ -n "$signing_key" ] && [ ! -f "$signing_key" ]; then
    printf 'warning: signing key not found at %s; signed commits will fail until it exists\n' \
        "$signing_key" >&2
fi

printf 'Local git config applied.\n'
