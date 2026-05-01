#!/usr/bin/env bash
# Run every hermitage script at every isolation level and print a
# Kleppmann-style summary table. Rows are tests; columns are isolation
# levels.
#
# Usage: run-hermitage.sh [driver] [dsn]
#   defaults: postgres / host=localhost port=4000 sslmode=disable dbname=postgres
#
# Per-run logs (jsonl + stderr) land in .hermitage-logs/.

set -u
trap 'printf "\n" >&2; exit 130' INT

DRIVER="${1:-postgres}"
DSN="${2:-host=localhost port=4000 sslmode=disable dbname=postgres}"

LEVELS=(read-uncommitted read-committed repeatable-read serializable)
LEVEL_LABELS=("Read Uncommitted" "Read Committed" "Repeatable Read" "Serializable")

ROOT="$(cd "$(dirname "$0")" && pwd)"
MONASTERY="${ROOT}/monastery"
LOG_DIR="${ROOT}/.hermitage-logs"
mkdir -p "$LOG_DIR"

if [[ ! -x "$MONASTERY" ]]; then
    (cd "$ROOT" && go build) || exit 1
fi
if [[ ! -f "${ROOT}/${DRIVER}.so" ]]; then
    (cd "$ROOT" && go build -buildmode=plugin -o "${DRIVER}.so" "./plugins/${DRIVER}/") || exit 1
fi

NAME_W=4
for f in "$ROOT"/hermitage/*.sql; do
    name="$(basename "$f" .sql)"
    w=${#name}
    if [ "$w" -gt "$NAME_W" ]; then NAME_W=$w; fi
done
LVL_W=18

dashes() {
    local n=$1 out=""
    while [ "$n" -gt 0 ]; do out+="-"; n=$((n - 1)); done
    printf '%s' "$out"
}

printf "| %-${NAME_W}s |" "Test"
for L in "${LEVEL_LABELS[@]}"; do
    printf " %-${LVL_W}s |" "$L"
done
printf '\n'

printf "|%s|" "$(dashes $((NAME_W + 2)))"
for _ in "${LEVELS[@]}"; do
    printf "%s|" "$(dashes $((LVL_W + 2)))"
done
printf '\n'

for f in "$ROOT"/hermitage/*.sql; do
    name="$(basename "$f" .sql)"
    printf "| %-${NAME_W}s |" "$name"
    for L in "${LEVELS[@]}"; do
        out="${LOG_DIR}/${name}-${L}.out"
        jsonl="${LOG_DIR}/${name}-${L}.jsonl"
        : >"$jsonl"
        if "$MONASTERY" -events-only -interval 50ms \
                -log "$jsonl" \
                "$DRIVER" "$DSN" "$L" "$f" >"$out" 2>&1; then
            status="OK"
        else
            status="FAIL"
        fi
        printf " %-${LVL_W}s |" "$status"
    done
    printf '\n'
done
