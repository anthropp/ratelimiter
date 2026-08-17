#!/usr/bin/env bash
# Evaluator CLI for the distributed rate limiter demo.
#
#   ./run.sh --help        list scenarios
#   ./run.sh <scenario>    run one scenario; exits 0 on PASS, 1 on FAIL
#
# Needs only curl. RATELIM_ADDR overrides the deployed endpoint.
set -euo pipefail

ADDR="${RATELIM_ADDR:-http://136.66.105.38}"
# Demo-grade shared token (see DESIGN.md D7); intentionally checked in.
TOKEN="rldemo-c7f31a92d4e85b06"

if [[ "${1:---help}" == "--help" || "${1:-}" == "-h" ]]; then
  echo "usage: $0 <scenario>"
  echo
  echo "Available scenarios:"
  # Render "name: description" from the JSON without requiring jq.
  curl -fsS "$ADDR/scenarios" | python3 -c '
import json, sys, textwrap
for s in json.load(sys.stdin):
    print()
    print("  " + s["Name"])
    print(textwrap.fill(s["Description"], width=76, initial_indent="      ", subsequent_indent="      "))
'
  exit 0
fi

out="$(mktemp)"
trap 'rm -f "$out" "$out.err"' EXIT
# -N disables buffering so progress streams live. One scenario runs at a
# time; HTTP 409 means another run (or its post-scenario cleanup, up to
# ~2 minutes after a kill scenario) still holds the slot — wait and retry.
for attempt in $(seq 1 10); do
  rc=0
  curl -fsSN -H "Authorization: Bearer $TOKEN" "$ADDR/run?scenario=$1" 2>"$out.err" | tee "$out" || rc=$?
  if [[ $rc -eq 22 ]] && grep -q ": 409" "$out.err"; then
    echo "another scenario is still running (HTTP 409); retrying in 30s (attempt $attempt/10)..." >&2
    sleep 30
    continue
  fi
  cat "$out.err" >&2  # surface any real curl error; empty on success
  break
done
[[ "$(tail -n 1 "$out")" == PASS* ]]
