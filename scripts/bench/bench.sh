#!/usr/bin/env bash
#
# bench.sh — unified entry point for benching a single language-test bench.
#
#   bench.sh <filter> [--save] [--backend mem] [--dataset NAME]
#       Measure: time the statement(s), compare against the saved baseline, and
#       (with --save) persist this run as the new baseline.  -> measure.sh
#
#   bench.sh <filter> --profile [--backend mem] [--dataset NAME]
#       Profile: record a samply flamegraph of the measured statements only
#       (import + warmup excluded).  -> profile.sh
#
# Routing is on the presence of `--profile`; all other flags pass straight
# through to the underlying script. With no filter, prints usage.
set -euo pipefail

# `cargo make bench -- <args>` forwards the `--` separator through as a literal
# first argument; drop it so the filter parses cleanly.
if [[ "${1:-}" == "--" ]]; then shift; fi

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

usage() {
	cat <<'EOF'
Measure or profile a single language-test bench.

Usage:
  cargo make bench -- <filter> [options]

  <filter>           path substring selecting ONE bench (e.g. scans/where_integer_in_many_full)

Options:
  --profile          record a samply flamegraph instead of measuring timing
  --save             persist this run as the new baseline (measure mode only)
  --quick            fast, coarse pass: shrinks every timing knob ~10x
  --dataset <name>   for a matrix scan, restrict to one variant (e.g. indexed, unindexed)
  --backend <name>   storage backend (default: mem)

Examples:
  cargo make bench -- scans/count
  cargo make bench -- scans/count --save
  cargo make bench -- scans/count --quick
  cargo make bench -- scans/where_integer_in_many_full --profile --dataset indexed
EOF
}

# Split out the `--profile` switch; forward everything else verbatim.
mode="measure"
args=()
for a in "$@"; do
	if [[ "$a" == "--profile" ]]; then
		mode="profile"
	else
		args+=("$a")
	fi
done

# No filter given (no args, or only flags) → show usage and stop cleanly.
if [[ ${#args[@]} -eq 0 || "${args[0]:-}" == -* ]]; then
	usage
	exit 0
fi

if [[ "$mode" == "profile" ]]; then
	exec "$DIR/profile.sh" ${args[@]+"${args[@]}"}
else
	exec "$DIR/measure.sh" ${args[@]+"${args[@]}"}
fi
