#!/usr/bin/env bash
#
#  This file is part of paikka.
#
#  Paikka is free software: you can redistribute it and/or
#  modify it under the terms of the GNU Affero General Public License
#  as published by the Free Software Foundation, either version 3 of
#  the License, or (at your option) any later version.
#
#  Paikka is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU Affero General Public License for more details.
#
#  You should have received a copy of the GNU Affero General Public License
#  along with Paikka. If not, see <https://www.gnu.org/licenses/>.
#

# PAIKKA reverse geocoding benchmark
#
# Measures single-request latency and concurrent throughput against a running
# PAIKKA server. It only issues GET requests to /api/v1/reverse.

set -euo pipefail
export LC_ALL=C

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

BASE_URL="http://localhost:8080"
POIS_FILE="$SCRIPT_DIR/benchmark-pois.csv"
LIMIT=10
LANG_CODE="en"
WARMUP=10
RUNS=50
MODE="both"
CONCURRENCY="1,4,8,16"
REQUESTS=5000
TIMEOUT=10
CSV_FILE=""

usage() {
    cat <<EOF
Usage: $0 [OPTIONS]

Benchmarks PAIKKA reverse geocoding requests.

Options:
  --url URL              PAIKKA base URL (default: http://localhost:8080)
  --pois FILE            CSV file with name,lat,lon (default: scripts/benchmark-pois.csv)
  --limit N              Result limit per request (default: 10)
  --lang LANG            Language parameter (default: en)
  --warmup N             Warmup requests per POI, excluded from results (default: 10)
  --runs N               Measured sequential requests per POI (default: 50)
  --mode MODE            latency, throughput, or both (default: both)
  --concurrency LIST     Comma-separated throughput concurrency levels (default: 1,4,8,16)
  --requests N           Requests per throughput concurrency level (default: 5000)
  --timeout SECONDS      Per-request timeout (default: 10)
  --csv FILE             Write aggregated results as CSV
  -h, --help             Show this help message

Examples:
  $0
  $0 --mode latency --runs 100
  $0 --mode throughput --concurrency 1,8,32 --requests 20000
  $0 --url https://geo.dedicatedcode.com --limit 5
EOF
}

die() {
    echo "Error: $*" >&2
    exit 1
}

require_value() {
    local option="$1"
    local value="${2:-}"
    [ -n "$value" ] || die "Missing value for $option"
}

trim() {
    local s="$1"
    s="${s#"${s%%[![:space:]]*}"}"
    s="${s%"${s##*[![:space:]]}"}"
    printf '%s' "$s"
}

now_us() {
    if [ -n "${EPOCHREALTIME:-}" ]; then
        printf '%s' "${EPOCHREALTIME/./}"
    else
        date +%s%6N
    fi
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --url) require_value "$1" "${2:-}"; BASE_URL="$2"; shift 2 ;;
        --pois) require_value "$1" "${2:-}"; POIS_FILE="$2"; shift 2 ;;
        --limit) require_value "$1" "${2:-}"; LIMIT="$2"; shift 2 ;;
        --lang) require_value "$1" "${2:-}"; LANG_CODE="$2"; shift 2 ;;
        --warmup) require_value "$1" "${2:-}"; WARMUP="$2"; shift 2 ;;
        --runs) require_value "$1" "${2:-}"; RUNS="$2"; shift 2 ;;
        --mode) require_value "$1" "${2:-}"; MODE="$2"; shift 2 ;;
        --concurrency) require_value "$1" "${2:-}"; CONCURRENCY="$2"; shift 2 ;;
        --requests) require_value "$1" "${2:-}"; REQUESTS="$2"; shift 2 ;;
        --timeout) require_value "$1" "${2:-}"; TIMEOUT="$2"; shift 2 ;;
        --csv) require_value "$1" "${2:-}"; CSV_FILE="$2"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) die "Unknown option: $1" ;;
    esac
done

BASE_URL="${BASE_URL%/}"

case "$MODE" in
    latency|throughput|both) ;;
    *) die "Invalid --mode '$MODE' (expected latency, throughput, or both)" ;;
esac

[[ "$LIMIT" =~ ^[0-9]+$ ]] && [ "$LIMIT" -gt 0 ] || die "--limit must be a positive integer"
[[ "$WARMUP" =~ ^[0-9]+$ ]] || die "--warmup must be a non-negative integer"
[[ "$RUNS" =~ ^[0-9]+$ ]] && [ "$RUNS" -gt 0 ] || die "--runs must be a positive integer"
[[ "$REQUESTS" =~ ^[0-9]+$ ]] && [ "$REQUESTS" -gt 0 ] || die "--requests must be a positive integer"
[[ "$TIMEOUT" =~ ^[0-9]+$ ]] && [ "$TIMEOUT" -gt 0 ] || die "--timeout must be a positive integer"

command -v curl >/dev/null 2>&1 || die "curl is required"
command -v awk >/dev/null 2>&1 || die "awk is required"
command -v sort >/dev/null 2>&1 || die "sort is required"
[ -f "$POIS_FILE" ] || die "POI file not found: $POIS_FILE"

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/paikka-benchmark.XXXXXX")"
cleanup() {
    rm -rf "$TMP_DIR"
}
trap cleanup EXIT

POI_NAMES=()
POI_LATS=()
POI_LONS=()
POI_INDEX=0

while IFS= read -r line || [ -n "$line" ]; do
    line="${line%$'\r'}"
    trimmed="$(trim "$line")"
    [ -z "$trimmed" ] && continue
    case "$trimmed" in
        \#*) continue ;;
    esac

    name=""
    lat=""
    lon=""
    extra=""
    IFS=',' read -r name lat lon extra <<< "$line"
    name="$(trim "$name")"
    lat="$(trim "$lat")"
    lon="$(trim "$lon")"
    extra="$(trim "$extra")"

    [ -z "$extra" ] || die "Invalid POI line (too many fields): $line"
    [ -n "$name" ] || name="poi-$POI_INDEX"
    [[ "$lat" =~ ^-?[0-9]+([.][0-9]+)?$ ]] || die "Invalid latitude in POI line: $line"
    [[ "$lon" =~ ^-?[0-9]+([.][0-9]+)?$ ]] || die "Invalid longitude in POI line: $line"

    POI_NAMES+=("$name")
    POI_LATS+=("$lat")
    POI_LONS+=("$lon")
    POI_INDEX=$((POI_INDEX + 1))
done < "$POIS_FILE"

[ "${#POI_NAMES[@]}" -gt 0 ] || die "No POIs found in $POIS_FILE"

reverse_url() {
    local lat="$1"
    local lon="$2"
    printf '%s/api/v1/reverse?lat=%s&lon=%s&lang=%s&limit=%s' \
        "$BASE_URL" "$lat" "$lon" "$LANG_CODE" "$LIMIT"
}

collect_stats() {
    local file="$1"
    local label="$2"
    local fails="$3"

    sort -n "$file" | awk -v label="$label" -v fails="$fails" '
        { values[NR] = $1; sum += $1 }
        END {
            n = NR
            if (n == 0) {
                printf "%s\t0\t-\t-\t-\t-\t-\t-\t-\t%d\n", label, fails
                exit
            }
            mean = sum / n
            p50 = values[int((n - 1) * 0.50) + 1]
            p90 = values[int((n - 1) * 0.90) + 1]
            p95 = values[int((n - 1) * 0.95) + 1]
            p99 = values[int((n - 1) * 0.99) + 1]
            printf "%s\t%d\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%d\n",
                label, n, values[1], mean, p50, p90, p95, p99, values[n], fails
        }
    '
}

print_latency_table() {
    local stats_file="$1"

    printf "%-28s %5s %7s %7s %7s %7s %7s %7s %7s %5s\n" \
        "POI" "n" "min" "mean" "p50" "p90" "p95" "p99" "max" "fails"

    while IFS=$'\t' read -r label n min mean p50 p90 p95 p99 max fails; do
        if [ "$n" = "0" ]; then
            printf "%-28s %5s %7s %7s %7s %7s %7s %7s %7s %5s\n" \
                "$label" "$n" "-" "-" "-" "-" "-" "-" "-" "$fails"
        else
            printf "%-28s %5s %7s %7s %7s %7s %7s %7s %7s %5s\n" \
                "$label" "$n" "$min" "$mean" "$p50" "$p90" "$p95" "$p99" "$max" "$fails"
        fi
    done < "$stats_file"
}

write_latency_csv() {
    local stats_file="$1"

    while IFS=$'\t' read -r label n min mean p50 p90 p95 p99 max fails; do
        printf 'latency,%s,,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
            "$label" "$n" "$min" "$mean" "$p50" "$p90" "$p95" "$p99" "$max" "$fails" "" \
            >> "$CSV_FILE"
    done < "$stats_file"
}

run_latency() {
    echo
    echo "=== Single-request latency ==="
    echo

    local -a latency_files=()
    local -a error_files=()
    local all_latency="$TMP_DIR/latency_all.ms"
    local stats_file="$TMP_DIR/latency_stats.tsv"

    : > "$all_latency"
    for i in "${!POI_NAMES[@]}"; do
        latency_files[$i]="$TMP_DIR/latency_$i.ms"
        error_files[$i]="$TMP_DIR/latency_errors_$i.log"
        : > "${latency_files[$i]}"
        : > "${error_files[$i]}"
    done

    if [ "$WARMUP" -gt 0 ]; then
        echo "Warming up (${WARMUP} requests per POI)..."
        for ((warmup_index = 0; warmup_index < WARMUP; warmup_index++)); do
            for i in "${!POI_NAMES[@]}"; do
                curl -sS --max-time "$TIMEOUT" -o /dev/null \
                    "$(reverse_url "${POI_LATS[$i]}" "${POI_LONS[$i]}")" \
                    >/dev/null 2>&1 || true
            done
        done
    fi

    echo "Measuring ${RUNS} requests per POI..."
    for ((run_index = 0; run_index < RUNS; run_index++)); do
        for i in "${!POI_NAMES[@]}"; do
            output="$(curl -sS --max-time "$TIMEOUT" -o /dev/null \
                -w '%{http_code} %{time_total}' \
                "$(reverse_url "${POI_LATS[$i]}" "${POI_LONS[$i]}")" \
                2>/dev/null || echo "000 0")"
            status="${output%% *}"
            seconds="${output#* }"
            if [ "$status" = "200" ]; then
                ms="$(awk -v s="$seconds" 'BEGIN { printf "%.3f", s * 1000 }')"
                printf '%s\n' "$ms" >> "${latency_files[$i]}"
                printf '%s\n' "$ms" >> "$all_latency"
            else
                printf '%s\n' "$status" >> "${error_files[$i]}"
            fi
        done
    done

    : > "$stats_file"
    total_errors=0
    for i in "${!POI_NAMES[@]}"; do
        errors="$(wc -l < "${error_files[$i]}" | tr -d ' ')"
        total_errors=$((total_errors + errors))
        collect_stats "${latency_files[$i]}" "${POI_NAMES[$i]}" "$errors" >> "$stats_file"
    done
    collect_stats "$all_latency" "ALL" "$total_errors" >> "$stats_file"

    echo
    print_latency_table "$stats_file"
    echo "Times are in milliseconds."
    echo
    echo "Note: latency numbers are warm-JIT/warm-cache steady-state results."

    if [ -n "$CSV_FILE" ]; then
        write_latency_csv "$stats_file"
    fi
}

supports_parallel_curl() {
    curl --help all 2>/dev/null | grep -q -- '--parallel'
}

make_url_files() {
    local count="$1"
    local config_file="$2"
    local plain_file="$3"

    : > "$config_file"
    : > "$plain_file"

    local poi_count="${#POI_NAMES[@]}"
    local i=0
    local k=0
    local url
    for ((k = 0; k < count; k++)); do
        i=$((k % poi_count))
        url="$(reverse_url "${POI_LATS[$i]}" "${POI_LONS[$i]}")"
        printf 'url = "%s"\n' "$url" >> "$config_file"
        printf 'output = "/dev/null"\n' >> "$config_file"
        printf '%s\n' "$url" >> "$plain_file"
    done
}

run_load() {
    local parallel="$1"
    local config_file="$2"
    local plain_file="$3"
    local raw_file="$4"

    if [ "$parallel" = "yes" ]; then
        curl -sS --parallel --parallel-max "$CURRENT_CONCURRENCY" \
            --max-time "$TIMEOUT" \
            -w '%{http_code} %{time_total}\n' \
            --config "$config_file" > "$raw_file" 2> "${raw_file}.err" || true
    else
        xargs -P "$CURRENT_CONCURRENCY" -n1 -I{} \
            curl -sS --max-time "$TIMEOUT" -o /dev/null \
            -w '%{http_code} %{time_total}\n' "{}" \
            < "$plain_file" > "$raw_file" 2> "${raw_file}.err" || true
    fi
}

run_throughput() {
    echo
    echo "=== Concurrent throughput ==="
    echo

    local parallel="yes"
    if ! supports_parallel_curl; then
        parallel="no"
        command -v xargs >/dev/null 2>&1 || die "curl lacks --parallel and xargs is not available"
        echo "Warning: curl does not support --parallel; falling back to xargs -P." >&2
    fi

    IFS=',' read -r -a levels <<< "$CONCURRENCY"
    [ "${#levels[@]}" -gt 0 ] || die "No concurrency levels configured"

    printf "%-11s %8s %10s %10s %8s %8s %8s %8s %8s %7s\n" \
        "CONCURRENCY" "REQUESTS" "DURATION_S" "REQ/SEC" "mean" "p50" "p95" "p99" "max" "errors"

    for level in "${levels[@]}"; do
        level="$(trim "$level")"
        [[ "$level" =~ ^[0-9]+$ ]] && [ "$level" -gt 0 ] || die "Invalid concurrency level: $level"
        CURRENT_CONCURRENCY="$level"

        if [ "$WARMUP" -gt 0 ]; then
            make_url_files "$WARMUP" "$TMP_DIR/warmup_urls.conf" "$TMP_DIR/warmup_urls.txt"
            run_load "$parallel" "$TMP_DIR/warmup_urls.conf" "$TMP_DIR/warmup_urls.txt" "$TMP_DIR/warmup_raw.txt"
        fi

        make_url_files "$REQUESTS" "$TMP_DIR/throughput_urls.conf" "$TMP_DIR/throughput_urls.txt"

        start_us="$(now_us)"
        run_load "$parallel" "$TMP_DIR/throughput_urls.conf" "$TMP_DIR/throughput_urls.txt" "$TMP_DIR/throughput_raw.txt"
        end_us="$(now_us)"

        elapsed="$(awk -v a="$start_us" -v b="$end_us" 'BEGIN { printf "%.6f", (b - a) / 1000000 }')"
        total="$(wc -l < "$TMP_DIR/throughput_raw.txt" | tr -d ' ')"
        ok="$(awk '$1 == 200 { count++ } END { print count + 0 }' "$TMP_DIR/throughput_raw.txt")"
        errors=$((total - ok))
        req_per_sec="$(awk -v ok="$ok" -v sec="$elapsed" 'BEGIN { if (sec > 0) printf "%.1f", ok / sec; else printf "0.0" }')"

        awk '$1 == 200 { printf "%.3f\n", $2 * 1000 }' "$TMP_DIR/throughput_raw.txt" \
            > "$TMP_DIR/throughput_times.ms"

        stats_line="$(collect_stats "$TMP_DIR/throughput_times.ms" "c=$level" "$errors")"
        IFS=$'\t' read -r label n min mean p50 p90 p95 p99 max stats_errors <<< "$stats_line"

        if [ "$n" = "0" ]; then
            printf "%-11s %8s %10s %10s %8s %8s %8s %8s %8s %7s\n" \
                "$level" "$total" "$elapsed" "$req_per_sec" "-" "-" "-" "-" "-" "$stats_errors"
        else
            printf "%-11s %8s %10s %10s %8s %8s %8s %8s %8s %7s\n" \
                "$level" "$total" "$elapsed" "$req_per_sec" "$mean" "$p50" "$p95" "$p99" "$max" "$stats_errors"
        fi

        if [ -n "$CSV_FILE" ]; then
            printf 'throughput,ALL,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
                "$level" "$n" "$min" "$mean" "$p50" "$p90" "$p95" "$p99" "$max" "$stats_errors" "$req_per_sec" \
                >> "$CSV_FILE"
        fi
    done

    echo
    echo "Throughput is successful requests per wall-clock second."
    echo "Latency columns are in milliseconds and include concurrent contention."
}

echo "PAIKKA reverse geocoding benchmark"
echo "URL:          $BASE_URL"
echo "POIs:         $POIS_FILE"
echo "POI count:    ${#POI_NAMES[@]}"
echo "Limit:        $LIMIT"
echo "Language:     $LANG_CODE"
echo "Warmup:       $WARMUP"
echo "Mode:         $MODE"

health_status="$(curl -sS --max-time "$TIMEOUT" -o "$TMP_DIR/health.json" \
    -w '%{http_code}' "$BASE_URL/api/v1/health" 2>/dev/null || echo "000")"
[ "$health_status" = "200" ] || die "Health check failed for $BASE_URL/api/v1/health (HTTP $health_status)"

data_version="$(sed -n 's/.*"dataVersion"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' \
    "$TMP_DIR/health.json" | head -1 || true)"
[ -n "$data_version" ] || data_version="unknown"
echo "Data version: $data_version"

if [ -n "$CSV_FILE" ]; then
    printf 'mode,poi,concurrency,n,min_ms,mean_ms,p50_ms,p90_ms,p95_ms,p99_ms,max_ms,errors,req_per_sec\n' \
        > "$CSV_FILE"
fi

echo
echo "=== Preflight ==="
printf "%-28s %6s %8s  %s\n" "POI" "status" "results" "first result"

for i in "${!POI_NAMES[@]}"; do
    headers="$TMP_DIR/headers_$i.txt"
    body="$TMP_DIR/body_$i.json"
    status="$(curl -sS --max-time "$TIMEOUT" -D "$headers" -o "$body" \
        -w '%{http_code}' "$(reverse_url "${POI_LATS[$i]}" "${POI_LONS[$i]}")" \
        2>/dev/null || echo "000")"
    [ "$status" = "200" ] || die "Preflight failed for ${POI_NAMES[$i]}: HTTP $status"

    count="$(grep -i '^X-Result-Count:' "$headers" 2>/dev/null \
        | tail -1 | sed 's/.*: *//' | tr -d '\015' || true)"
    [ -n "$count" ] || count="0"

    first="$(grep -o '"display_name"[[:space:]]*:[[:space:]]*"[^"]*"' "$body" 2>/dev/null \
        | head -1 | sed 's/.*:[[:space:]]*"//; s/"$//' || true)"
    [ -n "$first" ] || first="-"

    printf "%-28s %6s %8s  %s\n" "${POI_NAMES[$i]}" "$status" "$count" "$first"

    if [ "$count" = "0" ]; then
        echo "Warning: ${POI_NAMES[$i]} returned zero results" >&2
    fi
done

if [ "$MODE" = "latency" ] || [ "$MODE" = "both" ]; then
    run_latency
fi

if [ "$MODE" = "throughput" ] || [ "$MODE" = "both" ]; then
    run_throughput
fi

if [ -n "$CSV_FILE" ]; then
    echo
    echo "CSV written to: $CSV_FILE"
fi
