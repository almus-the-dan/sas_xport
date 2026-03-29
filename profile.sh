#!/usr/bin/env bash
#
# Profile the sas_xport reader and writer with samply, then report the
# top functions from this crate by inclusive/self-time percentage.
#
# Usage:
#   ./profile.sh [--records N] [--top N]
#   ./profile.sh                        # 1M records, top 10 functions
#   ./profile.sh --records 500000       # 500K records, top 10 functions
#   ./profile.sh --top 20              # 1M records, top 20 functions
#
# Requirements:
#   cargo install --locked samply

set -euo pipefail

RECORDS=1000000
TOP_N=10

while [[ $# -gt 0 ]]; do
    case "$1" in
        --records) RECORDS="$2"; shift 2 ;;
        --top)     TOP_N="$2";   shift 2 ;;
        *)         echo "Unknown option: $1"; exit 1 ;;
    esac
done

PROFILE_BIN="target/profiling/examples/profile"
REPORT_BIN="target/profiling/examples/profile_report"

# ---------------------------------------------------------------------------
# Preflight
# ---------------------------------------------------------------------------

if ! command -v samply &>/dev/null; then
    echo "Error: 'samply' not found."
    echo "Install with: cargo install --locked samply"
    exit 1
fi

echo "Building profiling binaries..."
cargo build --example profile --example profile_report --profile profiling -p sas_xport --all-features --quiet

# ---------------------------------------------------------------------------
# Profile each phase
# ---------------------------------------------------------------------------

run_phase() {
    local phase="$1"
    local json_file="target/profile_${phase}.json.gz"

    echo ""
    echo "Recording ${phase} phase ($RECORDS records)..."
    samply record --save-only --unstable-presymbolicate \
        -o "$json_file" -- "./$PROFILE_BIN" --phase "$phase" --records "$RECORDS" 2>/dev/null

    echo ""
    echo "=== ${phase^^} ==="
    "./$REPORT_BIN" --input "$json_file" --top "$TOP_N"

    rm -f "$json_file" "${json_file%.gz}.syms.json"
}

run_phase sync-write
run_phase sync-read
run_phase async-write
run_phase async-read

echo ""
echo "Done."