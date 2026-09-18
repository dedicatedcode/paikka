#!/bin/bash

# ==============================================================================
# SCRIPT CONFIGURATION AND GLOBAL DEFAULTS
# ==============================================================================
# --- General Settings ---
set -e # Exit immediately if a command exits with a non-zero status.
set -o pipefail # The return value of a pipeline is the status of the last command to exit with a non-zero status.

# --- Local Machine Settings ---
PLANET_URL="https://planet.osm.org/pbf/planet-latest.osm.pbf"
LOCAL_WORK_DIR="$(pwd)" # Use the current directory as the working directory.

# --- Local configuration (optional .env file, e.g. GitHub credentials) ---
if [ -f "$LOCAL_WORK_DIR/.env" ]; then
    # shellcheck disable=SC1091
    source "$LOCAL_WORK_DIR/.env"
fi

PBF_INPUT_FILE="planet-latest.osm.pbf"
PBF_FILTERED_FILE="planet-filtered.pbf"
IMPORT_DIR="import"
DOCKER_IMAGE="dedicatedcode/paikka:latest"

DOWNLOAD_DIR="${DOWNLOAD_DIR:-$LOCAL_WORK_DIR}"
IMPORT_DATA_DIR="${IMPORT_DATA_DIR:-$LOCAL_WORK_DIR/$IMPORT_DIR}"
IMPORT_MEMORY="${IMPORT_MEMORY:-16G}"
IMPORT_THREADS="${IMPORT_THREADS:-10}"

# --- Remote Machine Settings ---
REMOTE_BASE_DIR="/opt/paikka/data"

# --- Cloudflare Cache Invalidation Settings ---
CLOUDFLARE_API_BASE_URL="https://api.cloudflare.com/client/v4"
PURGE_CLOUDFLARE_CACHE="${PURGE_CLOUDFLARE_CACHE:-false}"
CLOUDFLARE_API_TOKEN="${CLOUDFLARE_API_TOKEN:-}"
CLOUDFLARE_ZONE_ID="${CLOUDFLARE_ZONE_ID:-}"
CLOUDFLARE_PURGE_PATTERN="${CLOUDFLARE_PURGE_PATTERN:-}"
PURGE_URLS=() # Populated from CLOUDFLARE_PURGE_PATTERN by validate_purge_pattern

# --- GitHub Import Statistics Settings (see issue #63) ---
GH_STATS_REPO="${GH_STATS_REPO:-dedicatedcode/paikka}"
GITHUB_TOKEN="${GITHUB_TOKEN:-}"
GH_DISCUSSION_NUMBER="${GH_DISCUSSION_NUMBER:-}"
POST_IMPORT_STATS="${POST_IMPORT_STATS:-false}"

# --- Import statistics runtime state ---
STATS_FILE_NAME="paikka-import-stats.ndjson"
IMPORT_LOG_FILE_NAME="paikka-import-run.log"
STATS_IMPORT_SUCCESS=false
STATS_OUTCOME_LINE=""
STATS_TOTAL_TIME=""
STATS_THROUGHPUT=""
STATS_DATASET_SIZE=""
STATS_FINAL_BLOCK=""
STATS_FILTER_INPUT_NAME=""
STATS_FILTER_INPUT_SIZE=0
STATS_FILTER_OUTPUT_NAME=""
STATS_FILTER_OUTPUT_SIZE=0
STATS_FILTER_DURATION=0

# Global variables that will be set by parse_args_and_configure or environment
REMOTE_USER="${REMOTE_USER:-}"
REMOTE_HOST="${REMOTE_HOST:-}"
GEOCODER_API_TOKEN="${GEOCODER_API_TOKEN:-}"
PBF_INPUT_PATH="${PBF_INPUT_PATH:-}"

# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

log() {
  echo -e "\n[$(date +'%Y-%m-%d %H:%M:%S')] --- $1 ---"
}

###
# Prints usage information for this script.
###
print_usage() {
    cat <<EOF
Usage: $0 [OPTIONS]

Downloads (or uses a provided) OSM planet file, filters and imports it into a
geocoder bundle, syncs it to the remote host, deploys it atomically, and
verifies the result. Optionally purges the Cloudflare cache of the public zone
afterwards.

Options:
  --remote-user USER          SSH user of the remote host (env: REMOTE_USER)
  --remote-host HOST          SSH host to deploy to. Should bypass Cloudflare,
                              e.g. geo-direkt.dedicatedcode.com (env: REMOTE_HOST)
  --api-token TOKEN           Admin API token of the geocoder (env: GEOCODER_API_TOKEN)
  --download-dir DIR          Where to download PBF files (env: DOWNLOAD_DIR, default: current directory)
  --import-data-dir DIR       Where to store import data (env: IMPORT_DATA_DIR, default: ./import)
  --memory SIZE               Memory for import (env: IMPORT_MEMORY, default: 16G)
  --threads NUM               Threads for import (env: IMPORT_THREADS, default: 10)
  --pbf-file PATH             Use a local PBF file instead of downloading the latest planet file

Cloudflare cache invalidation:
  --purge-cache               Purge the Cloudflare cache after a successful, verified update
                              (env: PURGE_CLOUDFLARE_CACHE)
  --cloudflare-token TOKEN    Cloudflare API token with "Zone.Cache Purge" permission
                              (env: CLOUDFLARE_API_TOKEN)
  --cloudflare-zone-id ID     Cloudflare zone ID of the public zone to purge, e.g. the zone
                              of geo.dedicatedcode.com (env: CLOUDFLARE_ZONE_ID)
  --purge-pattern URLS        Comma-separated list of exact https:// URLs to purge instead of
                              the entire zone cache, at most 30 URLs per request
                              (env: CLOUDFLARE_PURGE_PATTERN)

GitHub import statistics (posts one comment per successful import to a GitHub
discussion so import performance can be monitored over time):
  --post-stats                Upload import statistics after a successful, verified
                              update (env: POST_IMPORT_STATS)
  --github-token TOKEN        GitHub token with "Discussions: Read & write"
                              permission for the target repository
                              (env: GITHUB_TOKEN, usually set in .env)
  --discussion-number NUM     Number of the discussion that receives one comment per
                              successful import, taken from the discussion URL
                              (env: GH_DISCUSSION_NUMBER, usually set in .env)
  --stats-repo OWNER/REPO     Repository to post to (env: GH_STATS_REPO,
                              default: dedicatedcode/paikka)

  -h, --help                  Show this help message

Examples:
  $0 --remote-user deploy --remote-host geo-direkt.dedicatedcode.com --api-token SECRET
  $0 --remote-user deploy --remote-host geo-direkt.dedicatedcode.com --api-token SECRET \\
     --purge-cache --cloudflare-token CF_TOKEN --cloudflare-zone-id CF_ZONE_ID
  $0 --remote-user deploy --remote-host geo-direkt.dedicatedcode.com --api-token SECRET \\
     --pbf-file ./new-zealand.osm.pbf --memory 8G --threads 4
  $0 --remote-user deploy --remote-host geo-direkt.dedicatedcode.com --api-token SECRET \\
     --post-stats --github-token GH_TOKEN --discussion-number 7
EOF
}

###
# Trims leading and trailing whitespace from the given string.
###
trim() {
    local s="$1"
    s="${s#"${s%%[![:space:]]*}"}"
    s="${s%"${s##*[![:space:]]}"}"
    printf '%s' "$s"
}

###
# Ensures that the given flag was followed by a non-flag value.
# $1: flag name, $2: candidate value
###
require_flag_value() {
    if [ -z "$2" ] || [[ "$2" == -* ]]; then
        echo "Error: Missing value for $1"
        print_usage
        exit 1
    fi
}

# ==============================================================================
# CORE LOGIC FUNCTIONS
# ==============================================================================

###
# Validates the configured purge pattern and fills the PURGE_URLS array with
# the trimmed https:// URLs.
###
validate_purge_pattern() {
    local raw_url trimmed_url
    local validated_urls=()
    local count=0

    IFS=',' read -r -a PURGE_URLS <<< "$CLOUDFLARE_PURGE_PATTERN"

    for raw_url in "${PURGE_URLS[@]}"; do
        trimmed_url="$(trim "$raw_url")"
        [ -z "$trimmed_url" ] && continue
        if [[ "$trimmed_url" != https://* ]]; then
            echo "Error: Invalid purge pattern entry (must start with https://): $trimmed_url"
            exit 1
        fi
        validated_urls+=("$trimmed_url")
        count=$((count + 1))
    done

    if [ "$count" -eq 0 ]; then
        echo "Error: --purge-pattern was given but contains no URLs."
        print_usage
        exit 1
    fi
    if [ "$count" -gt 30 ]; then
        echo "Warning: $count URLs given, but Cloudflare allows at most 30 URLs per purge request." >&2
    fi

    PURGE_URLS=("${validated_urls[@]}")
}

###
# Parses command-line arguments and applies configuration.
# Precedence: command-line arguments > environment variables > defaults.
###
parse_args_and_configure() {
    local arg
    for arg in "$@"; do
        if [ "$arg" = "-h" ] || [ "$arg" = "--help" ]; then
            print_usage
            exit 0
        fi
    done

    log "Step 0: Parsing arguments and setting configuration"

    while [ $# -gt 0 ]; do
        case "$1" in
            --remote-user)          require_flag_value "$1" "${2:-}"; REMOTE_USER="$2"; shift 2 ;;
            --remote-user=*)        REMOTE_USER="${1#*=}"; shift ;;
            --remote-host)          require_flag_value "$1" "${2:-}"; REMOTE_HOST="$2"; shift 2 ;;
            --remote-host=*)        REMOTE_HOST="${1#*=}"; shift ;;
            --api-token)            require_flag_value "$1" "${2:-}"; GEOCODER_API_TOKEN="$2"; shift 2 ;;
            --api-token=*)          GEOCODER_API_TOKEN="${1#*=}"; shift ;;
            --download-dir)         require_flag_value "$1" "${2:-}"; DOWNLOAD_DIR="$2"; shift 2 ;;
            --download-dir=*)       DOWNLOAD_DIR="${1#*=}"; shift ;;
            --import-data-dir)      require_flag_value "$1" "${2:-}"; IMPORT_DATA_DIR="$2"; shift 2 ;;
            --import-data-dir=*)    IMPORT_DATA_DIR="${1#*=}"; shift ;;
            --memory)               require_flag_value "$1" "${2:-}"; IMPORT_MEMORY="$2"; shift 2 ;;
            --memory=*)             IMPORT_MEMORY="${1#*=}"; shift ;;
            --threads)              require_flag_value "$1" "${2:-}"; IMPORT_THREADS="$2"; shift 2 ;;
            --threads=*)            IMPORT_THREADS="${1#*=}"; shift ;;
            --pbf-file)             require_flag_value "$1" "${2:-}"; PBF_INPUT_PATH="$2"; shift 2 ;;
            --pbf-file=*)           PBF_INPUT_PATH="${1#*=}"; shift ;;
            --purge-cache)          PURGE_CLOUDFLARE_CACHE=true; shift ;;
            --purge-cache=*)
                case "${1#*=}" in
                    true)  PURGE_CLOUDFLARE_CACHE=true ;;
                    false) PURGE_CLOUDFLARE_CACHE=false ;;
                    *) echo "Error: Invalid value for --purge-cache (expected true or false)"; print_usage; exit 1 ;;
                esac
                shift ;;
            --cloudflare-token)     require_flag_value "$1" "${2:-}"; CLOUDFLARE_API_TOKEN="$2"; shift 2 ;;
            --cloudflare-token=*)   CLOUDFLARE_API_TOKEN="${1#*=}"; shift ;;
            --cloudflare-zone-id)   require_flag_value "$1" "${2:-}"; CLOUDFLARE_ZONE_ID="$2"; shift 2 ;;
            --cloudflare-zone-id=*) CLOUDFLARE_ZONE_ID="${1#*=}"; shift ;;
            --purge-pattern)        require_flag_value "$1" "${2:-}"; CLOUDFLARE_PURGE_PATTERN="$2"; shift 2 ;;
            --purge-pattern=*)      CLOUDFLARE_PURGE_PATTERN="${1#*=}"; shift ;;
            --post-stats)           POST_IMPORT_STATS=true; shift ;;
            --post-stats=*)
                case "${1#*=}" in
                    true)  POST_IMPORT_STATS=true ;;
                    false) POST_IMPORT_STATS=false ;;
                    *) echo "Error: Invalid value for --post-stats (expected true or false)"; print_usage; exit 1 ;;
                esac
                shift ;;
            --github-token)         require_flag_value "$1" "${2:-}"; GITHUB_TOKEN="$2"; shift 2 ;;
            --github-token=*)       GITHUB_TOKEN="${1#*=}"; shift ;;
            --discussion-number)    require_flag_value "$1" "${2:-}"; GH_DISCUSSION_NUMBER="$2"; shift 2 ;;
            --discussion-number=*)  GH_DISCUSSION_NUMBER="${1#*=}"; shift ;;
            --stats-repo)           require_flag_value "$1" "${2:-}"; GH_STATS_REPO="$2"; shift 2 ;;
            --stats-repo=*)         GH_STATS_REPO="${1#*=}"; shift ;;
            *)                      echo "Error: Unknown option: $1"; print_usage; exit 1 ;;
        esac
    done

    if [ -z "$REMOTE_USER" ] || [ -z "$REMOTE_HOST" ] || [ -z "$GEOCODER_API_TOKEN" ]; then
        echo "Error: Missing required configuration (--remote-user, --remote-host, --api-token)."
        print_usage
        exit 1
    fi
    if [ -n "$PBF_INPUT_PATH" ] && [ ! -f "$PBF_INPUT_PATH" ]; then
        echo "Error: PBF file not found: $PBF_INPUT_PATH"
        exit 1
    fi
    if [ "$PURGE_CLOUDFLARE_CACHE" = "true" ]; then
        if [ -z "$CLOUDFLARE_API_TOKEN" ]; then
            echo "Error: Cache purge is enabled but no Cloudflare API token was provided (--cloudflare-token)."
            print_usage
            exit 1
        fi
        if [ -z "$CLOUDFLARE_ZONE_ID" ]; then
            echo "Error: Cache purge is enabled but no Cloudflare zone ID was provided (--cloudflare-zone-id)."
            print_usage
            exit 1
        fi
        if [ -n "$CLOUDFLARE_PURGE_PATTERN" ]; then
            validate_purge_pattern
        fi
    elif [ -n "$CLOUDFLARE_PURGE_PATTERN" ]; then
        echo "Warning: --purge-pattern given but cache purge is not enabled (--purge-cache). The pattern will be ignored."
    fi

    if [ "$POST_IMPORT_STATS" = "true" ]; then
        if [ -z "$GITHUB_TOKEN" ]; then
            echo "Error: Statistics upload is enabled but no GitHub token was provided (--github-token or GITHUB_TOKEN in .env)."
            print_usage
            exit 1
        fi
        if ! [[ "$GH_DISCUSSION_NUMBER" =~ ^[0-9]+$ ]]; then
            echo "Error: Statistics upload is enabled but no valid discussion number was provided (--discussion-number or GH_DISCUSSION_NUMBER in .env)."
            print_usage
            exit 1
        fi
        if ! [[ "$GH_STATS_REPO" =~ ^[^/[:space:]]+/[^/[:space:]]+$ ]]; then
            echo "Error: --stats-repo must be in OWNER/REPO format (got: $GH_STATS_REPO)."
            print_usage
            exit 1
        fi
        for stats_dep in jq curl; do
            if ! command -v "$stats_dep" >/dev/null 2>&1; then
                echo "Error: Statistics upload is enabled but '$stats_dep' is not installed on this machine."
                print_usage
                exit 1
            fi
        done
    elif [ -n "$GITHUB_TOKEN" ] || [ -n "$GH_DISCUSSION_NUMBER" ]; then
        echo "Warning: GitHub statistics configuration found but statistics upload is not enabled (--post-stats). It will be ignored."
    fi

    echo "Configuration loaded for ${REMOTE_USER}@${REMOTE_HOST}"
    echo "  Download directory: $DOWNLOAD_DIR"
    echo "  Import data directory: $IMPORT_DATA_DIR"
    echo "  Import memory: $IMPORT_MEMORY"
    echo "  Import threads: $IMPORT_THREADS"
    if [ -n "$PBF_INPUT_PATH" ]; then
        echo "  PBF input file: $PBF_INPUT_PATH"
    fi
    if [ "$PURGE_CLOUDFLARE_CACHE" = "true" ]; then
        echo "  Cloudflare cache purge: enabled (zone ID: $CLOUDFLARE_ZONE_ID)"
        if [ "${#PURGE_URLS[@]}" -gt 0 ]; then
            echo "  Purge pattern: ${PURGE_URLS[*]}"
        else
            echo "  Purge pattern: entire zone cache"
        fi
    else
        echo "  Cloudflare cache purge: disabled"
    fi
    if [ "$POST_IMPORT_STATS" = "true" ]; then
        echo "  GitHub statistics: enabled (repo: $GH_STATS_REPO, discussion: #$GH_DISCUSSION_NUMBER)"
    else
        echo "  GitHub statistics: disabled"
    fi
}

###
# LOCAL: Creates the necessary working directories.
###
local_prepare_directories() {
    log "LOCAL: Ensuring import directory exists"
    mkdir -p "$DOWNLOAD_DIR"
    mkdir -p "$IMPORT_DATA_DIR"
    cd "$DOWNLOAD_DIR"
    # Start with a clean statistics log for this run
    : > "$DOWNLOAD_DIR/$STATS_FILE_NAME"
}

###
# LOCAL: Appends the filter step result to the import statistics log and
# remembers it for the GitHub statistics report.
# $1: input (source) PBF path, $2: filtered output PBF path, $3: duration in seconds
###
record_filter_stats() {
    local input_path="$1" output_path="$2" duration_seconds="$3"
    local input_size output_size
    input_size=$(stat -c%s "$input_path" 2>/dev/null || stat -f%z "$input_path" 2>/dev/null || echo 0)
    output_size=$(stat -c%s "$output_path" 2>/dev/null || stat -f%z "$output_path" 2>/dev/null || echo 0)

    STATS_FILTER_INPUT_NAME=$(basename "$input_path")
    STATS_FILTER_INPUT_SIZE="$input_size"
    STATS_FILTER_OUTPUT_NAME=$(basename "$output_path")
    STATS_FILTER_OUTPUT_SIZE="$output_size"
    STATS_FILTER_DURATION="$duration_seconds"

    if command -v jq >/dev/null 2>&1; then
        jq -cn \
            --arg step "filter" \
            --arg input_file "$STATS_FILTER_INPUT_NAME" \
            --argjson input_size "$STATS_FILTER_INPUT_SIZE" \
            --arg output_file "$STATS_FILTER_OUTPUT_NAME" \
            --argjson output_size "$STATS_FILTER_OUTPUT_SIZE" \
            --argjson duration_seconds "$STATS_FILTER_DURATION" \
            '{step: $step, input_file: $input_file, input_size: $input_size, output_file: $output_file, output_size: $output_size, duration_seconds: $duration_seconds}' \
            >> "$DOWNLOAD_DIR/$STATS_FILE_NAME" \
            || echo "WARNING: Could not append filter statistics to $DOWNLOAD_DIR/$STATS_FILE_NAME" >&2
    fi
}

###
# LOCAL: Downloads the latest OSM planet file.
###
local_download_planet_file() {
    log "LOCAL: Downloading latest OSM planet file to $DOWNLOAD_DIR"
    cd "$DOWNLOAD_DIR"
    wget -N "$PLANET_URL"
}

###
# LOCAL: Pulls the latest version of the Paikka Docker image.
###
local_pull_docker_image() {
    log "LOCAL: Pulling latest Docker image: $DOCKER_IMAGE"
    sudo docker pull "$DOCKER_IMAGE"
}

###
# LOCAL: Filters the full planet PBF file using the Paikka container.
# Measures the filter duration per file and records it for the statistics report.
###
local_filter_pbf() {
    log "LOCAL: Filtering PBF file"
    local filter_start_ts filter_end_ts filter_input_path
    if [ -n "$PBF_INPUT_PATH" ]; then
        INPUT_DIR="$(dirname "$PBF_INPUT_PATH")"
        INPUT_FILE="$(basename "$PBF_INPUT_PATH")"
        filter_input_path="$PBF_INPUT_PATH"
    else
        filter_input_path="$DOWNLOAD_DIR/$PBF_INPUT_FILE"
    fi

    filter_start_ts=$(date +%s)
    if [ -n "$PBF_INPUT_PATH" ]; then
        sudo docker run --rm -v "$INPUT_DIR":/input -v "$DOWNLOAD_DIR":/data "$DOCKER_IMAGE" prepare "/input/$INPUT_FILE" "/data/$PBF_FILTERED_FILE"
    else
        sudo docker run --rm -v "$DOWNLOAD_DIR":/data "$DOCKER_IMAGE" prepare "/data/$PBF_INPUT_FILE" "/data/$PBF_FILTERED_FILE"
    fi
    filter_end_ts=$(date +%s)

    record_filter_stats "$filter_input_path" "$DOWNLOAD_DIR/$PBF_FILTERED_FILE" "$((filter_end_ts - filter_start_ts))"
}

###
# LOCAL: Creates the geocoder import bundle from the filtered PBF.
# Captures the paikka import output in a log file which is then parsed for
# the statistics report. A run only counts as successful when the import
# process exited with code 0 AND reported "IMPORT OUTCOME: OK".
###
local_create_import_bundle() {
    log "LOCAL: Creating import bundle with $IMPORT_MEMORY memory and $IMPORT_THREADS threads"
        sudo docker run --rm -v "$DOWNLOAD_DIR":/download -v "$IMPORT_DATA_DIR":/import "$DOCKER_IMAGE" import \
          --memory "$IMPORT_MEMORY" \
          --threads "$IMPORT_THREADS" \
          --data-dir "/import/" \
          "/download/$PBF_FILTERED_FILE" 2>&1 | tee "$DOWNLOAD_DIR/$IMPORT_LOG_FILE_NAME"

    analyze_import_log
}

###
# LOCAL: Removes the large, intermediate PBF files.
###
local_cleanup_pbf() {
    log "LOCAL: Cleaning up intermediate PBF files from $DOWNLOAD_DIR"
    cd "$DOWNLOAD_DIR"
    rm -f "$PBF_FILTERED_FILE" "$PBF_INPUT_FILE"
    echo "Deleted '$DOWNLOAD_DIR/$PBF_FILTERED_FILE' and '$DOWNLOAD_DIR/$PBF_INPUT_FILE'"
}
###
# REMOTE: Syncs the import directory to the remote server using rsync.
# Uses --link-dest to minimize bandwidth and remote disk usage.
###
remote_sync_bundle() {
      log "REMOTE: Syncing bundle via rsync (Delta transfer)"

      CURRENT_LIVE=$(ssh "${REMOTE_USER}@${REMOTE_HOST}" "readlink -f ${REMOTE_BASE_DIR}/live_data || true")
      NEW_RELEASE_TIMESTAMP=$(date +%Y%m%d%H%M%S)
      NEW_RELEASE_DIR="${REMOTE_BASE_DIR}/releases/${NEW_RELEASE_TIMESTAMP}"

      ssh "${REMOTE_USER}@${REMOTE_HOST}" "mkdir -p ${REMOTE_BASE_DIR}/releases"

      rsync -avz --progress \
          ${CURRENT_LIVE:+--link-dest="$CURRENT_LIVE"} \
          "$IMPORT_DATA_DIR/" \
          "${REMOTE_USER}@${REMOTE_HOST}:$NEW_RELEASE_DIR/"

      export LATEST_RELEASE_DIR_NAME="$NEW_RELEASE_TIMESTAMP"
}

###
# REMOTE: Deploys, verifies, and handles rollback/cleanup on the remote host.
###
remote_deploy_and_verify() {
    log "REMOTE: Executing remote deployment (Atomic Swap)"

    # shellcheck disable=SC2087
    ssh "${REMOTE_USER}@${REMOTE_HOST}" /bin/bash << EOF
  set -e
  BASE_DIR="/opt/paikka/data"
  API_TOKEN="${GEOCODER_API_TOKEN}"
  ADMIN_URL="http://localhost:8080/admin/refresh-db"
  TEST_URL_BASE="http://localhost:8080/api/v1/reverse"
  NEW_RELEASE_DIR="releases/${LATEST_RELEASE_DIR_NAME}"
  LIVE_DATA_SYMLINK="live_data"

  echo_remote() {
    echo "[REMOTE] \$1"
  }

  cd "\$BASE_DIR"

  OLD_RELEASE_DIR=""
  [ -L "\$LIVE_DATA_SYMLINK" ] && OLD_RELEASE_DIR=\$(readlink "\$LIVE_DATA_SYMLINK")

  echo_remote "Switching symlink: \$LIVE_DATA_SYMLINK -> \$NEW_RELEASE_DIR"
  ln -sfn "\$NEW_RELEASE_DIR" "\$LIVE_DATA_SYMLINK"

  echo_remote "Refreshing Geocoder DB..."
  HTTP_STATUS=\$(curl -s -o /dev/null -w "%{http_code}" --max-time 300 -X POST -H "X-Admin-Token: \$API_TOKEN" "\$ADMIN_URL")

  if [ "\$HTTP_STATUS" -ne 200 ]; then
      echo_remote "ERROR: Refresh failed (\$HTTP_STATUS). Rolling back."
      [ -n "\$OLD_RELEASE_DIR" ] && ln -sfn "\$OLD_RELEASE_DIR" "\$LIVE_DATA_SYMLINK"
      exit 1
  fi

  echo_remote "Refresh completed successfully"

  # --- 2. Verify ---
  echo_remote "Verifying new data..."
  VERIFICATION_FAILED=0

  QUERIES[0]="lat=52.516280&lon=13.377635"
  QUERIES[1]="lat=48.85826&lon=2.2945008"
  QUERIES[2]="lat=40.68924&lon=-74.044502"

  EXPECTED_IDS[0]="518071791"
  EXPECTED_IDS[1]="5013364"
  EXPECTED_IDS[2]="32965412"

  # Get the number of elements
  NUM_TESTS=3

  for ((i=0; i<NUM_TESTS; i++)); do
    query="\${QUERIES[\$i]}"
    expected_id="\${EXPECTED_IDS[\$i]}"
    echo_remote "Testing URL: \$TEST_URL_BASE?\$query"
    ACTUAL_ID=\$(curl -s --max-time 30 "\$TEST_URL_BASE?\$query" | jq -r '.results[0].id // "not_found"')
    echo_remote "Got ID: \$ACTUAL_ID, Expected: \$expected_id"
    if [ "\$ACTUAL_ID" != "\$expected_id" ]; then
      echo_remote "  --> FAILED: For \$query, expected '\$expected_id', got '\$ACTUAL_ID'"
      VERIFICATION_FAILED=1
    else
      echo_remote "  --> SUCCESS: Verified query for \$query"
    fi
  done
  # --- 3. Finalize or Rollback ---
  if [ \$VERIFICATION_FAILED -eq 1 ]; then
    echo_remote "VERIFICATION FAILED. Rolling back and re-refreshing."
    if [ -n "\$OLD_RELEASE_DIR" ] && [ -d "\$OLD_RELEASE_DIR" ]; then
      ln -sfn "\$OLD_RELEASE_DIR" "\$LIVE_DATA_SYMLINK"
      curl -s -o /dev/null --max-time 300 -X POST -H "X-Admin-Token: \$API_TOKEN" "\$ADMIN_URL"
      echo_remote "Rollback to \$OLD_RELEASE_DIR complete. Faulty data in \$NEW_RELEASE_DIR is kept for inspection."
      exit 1
    else
      echo_remote "ERROR: Verification failed, but no previous version to roll back to!"
      exit 1
    fi
  else
    echo_remote "VERIFICATION SUCCEEDED. Cleaning up old release and archive."
    [ -n "\$OLD_RELEASE_DIR" ] && [ -d "\$OLD_RELEASE_DIR" ] && rm -rf "\$OLD_RELEASE_DIR"
    echo_remote "Deployment successful."
  fi
EOF
}

remote_cleanup_old_releases() {
    log "REMOTE: Cleaning up old releases (keeping last 3 successful ones)"

    ssh "${REMOTE_USER}@${REMOTE_HOST}" /bin/bash << EOF
  set -e
  BASE_DIR="${REMOTE_BASE_DIR}"

  echo_remote() {
    echo "[REMOTE CLEANUP] \$1"
  }

  cd "\$BASE_DIR"

  # Keep last 3 successful releases (excluding current live)
  echo_remote "Finding old releases to clean up..."
  CURRENT_LIVE=\$(readlink -f live_data 2>/dev/null || echo "")

  # List all releases, sort by timestamp, exclude current live
  RELEASES=\$(find releases -maxdepth 1 -type d -name "[0-9]*" | sort -r)

  KEEP_COUNT=3
  COUNT=0
  for release in \$RELEASES; do
    if [ "\$release" = "\$CURRENT_LIVE" ] || [ "\$release" = "\$(basename "\$CURRENT_LIVE")" ]; then
      echo_remote "Skipping current live release: \$release"
      continue
    fi

    COUNT=\$((COUNT + 1))
    if [ \$COUNT -gt \$KEEP_COUNT ]; then
      echo_remote "Removing old release: \$release"
      rm -rf "\$release"
    else
      echo_remote "Keeping release: \$release"
    fi
  done

  # Also clean up any empty directories
  find releases -type d -empty -delete 2>/dev/null || true

  echo_remote "Cleanup complete"
EOF
}

###
# LOCAL: Purges the Cloudflare cache for the configured zone after a successful
# update. Purges the entire zone when no pattern is configured, otherwise only
# the configured URLs. Fails soft: warns and continues if the purge fails,
# because the data deployment itself has already completed at this point.
###
purge_cloudflare_cache() {
    if [ "$PURGE_CLOUDFLARE_CACHE" != "true" ]; then
        return 0
    fi

    log "CLOUDFLARE: Purging cache for zone $CLOUDFLARE_ZONE_ID"

    local purge_body
    if [ "${#PURGE_URLS[@]}" -gt 0 ]; then
        log "CLOUDFLARE: Purging ${#PURGE_URLS[@]} URL(s) from the configured pattern"
        local urls_json=""
        local url
        for url in "${PURGE_URLS[@]}"; do
            urls_json+="\"${url//\"/\\\"}\","
        done
        purge_body="{\"files\":[${urls_json%,}]}"
    else
        log "CLOUDFLARE: Purging the entire zone cache"
        purge_body='{"purge_everything":true}'
    fi

    local response_file
    response_file="$(mktemp)"
    local http_status
    http_status="$(curl -s -o "$response_file" -w "%{http_code}" \
        --max-time 60 \
        -X POST \
        "${CLOUDFLARE_API_BASE_URL}/zones/${CLOUDFLARE_ZONE_ID}/purge_cache" \
        -H "Authorization: Bearer ${CLOUDFLARE_API_TOKEN}" \
        -H "Content-Type: application/json" \
        --data "$purge_body")" || http_status="curl_failed"

    if [ "$http_status" = "200" ] && grep -Eq '"success":[[:space:]]*true' "$response_file"; then
        log "CLOUDFLARE: Cache purge successful"
    else
        echo "WARNING: Cloudflare cache purge failed (HTTP status: $http_status). Continuing anyway." >&2
        echo "WARNING: Cloudflare response: $(cat "$response_file")" >&2
    fi
    rm -f "$response_file"
}
###
# Parses the captured paikka import log (which contains ANSI color codes) and
# extracts the values needed for the statistics report. Strips all ANSI escape
# sequences first so the report is clean markdown.
###
analyze_import_log() {
    local raw_log="$DOWNLOAD_DIR/$IMPORT_LOG_FILE_NAME"
    local clean_log="$DOWNLOAD_DIR/$IMPORT_LOG_FILE_NAME.clean"

    # Strip ANSI color codes and carriage returns emitted by the paikka import
    sed -e $'s/\033\\[[0-9;]*[mK]//g' "$raw_log" | tr -d '\r' > "$clean_log"

    STATS_OUTCOME_LINE=$(grep '^IMPORT OUTCOME: ' "$clean_log" | tail -n 1 || true)
    STATS_TOTAL_TIME=$(sed -n 's/.*Total Import Time: *//p' "$clean_log" | tail -n 1)
    STATS_THROUGHPUT=$(sed -n 's/.*Overall Throughput: *//p' "$clean_log" | tail -n 1)
    STATS_DATASET_SIZE=$(sed -n 's/.*Dataset Size: *//p' "$clean_log" | tail -n 1)
    STATS_FINAL_BLOCK=$(awk '/FINAL IMPORT STATISTICS/{found=1} found{print} found && /^IMPORT OUTCOME: /{exit}' "$clean_log")

    if grep -q '^IMPORT OUTCOME: OK ' "$clean_log"; then
        STATS_IMPORT_SUCCESS=true
    else
        STATS_IMPORT_SUCCESS=false
        echo "WARNING: Import did not report 'IMPORT OUTCOME: OK'. Statistics will not be uploaded." >&2
    fi

    rm -f "$clean_log"
}

###
# Formats a duration given in seconds as H:MM:SS.
###
format_duration() {
    local total_seconds="$1"
    printf '%d:%02d:%02d' $((total_seconds / 3600)) $((total_seconds % 3600 / 60)) $((total_seconds % 60))
}

###
# Formats a size given in bytes as a human readable value.
###
format_bytes() {
    awk -v bytes="$1" 'BEGIN {
        split("B KB MB GB TB PB", units, " ")
        i = 1
        value = bytes + 0
        while (value >= 1024 && i < 6) { value /= 1024; i++ }
        printf "%.1f %s", value, units[i]
    }'
}

###
# Gathers system information of the machine that performed the import.
###
gather_system_info() {
    STATS_CPU=$(LC_ALL=C lscpu 2>/dev/null | sed -n 's/^[[:space:]]*Model name:[[:space:]]*//p' | head -n 1)
    if [ -z "$STATS_CPU" ]; then
        STATS_CPU=$(sed -n 's/^model name[[:space:]]*:[[:space:]]*//p' /proc/cpuinfo 2>/dev/null | head -n 1)
    fi
    [ -z "$STATS_CPU" ] && STATS_CPU="unknown"
    STATS_CPU_CORES=$(nproc 2>/dev/null || echo "unknown")
    STATS_RAM=$(awk '/^MemTotal/ {printf "%.1f GB", $2 / 1048576}' /proc/meminfo 2>/dev/null)
    [ -z "$STATS_RAM" ] && STATS_RAM="unknown"
    STATS_OS=$(sed -n 's/^PRETTY_NAME="\([^"]*\)".*/\1/p' /etc/os-release 2>/dev/null | head -n 1)
    [ -z "$STATS_OS" ] && STATS_OS="$(uname -s 2>/dev/null || echo unknown)"
    STATS_KERNEL=$(uname -r 2>/dev/null || echo "unknown")
}

###
# Builds the markdown report for one successful import and writes it to stdout.
###
build_stats_report() {
    local data_version="unknown" paikka_version="unknown" grid_level="unknown" result_line
    local metadata_file="$IMPORT_DATA_DIR/paikka_metadata.json"

    if [ -f "$metadata_file" ] && command -v jq >/dev/null 2>&1; then
        data_version=$(jq -r '.dataVersion // "unknown"' "$metadata_file" 2>/dev/null || echo "unknown")
        paikka_version=$(jq -r '.paikkaVersion // "unknown"' "$metadata_file" 2>/dev/null || echo "unknown")
        grid_level=$(jq -r '.gridLevel // "unknown"' "$metadata_file" 2>/dev/null || echo "unknown")
    fi

    result_line=$(sed 's/|/\\|/g' <<< "${STATS_OUTCOME_LINE#IMPORT OUTCOME: }")

    cat <<REPORT_HEADER
### Import statistics — \`${data_version}\` ($(date -u '+%Y-%m-%d %H:%M UTC'))

| Metric | Value |
| --- | --- |
| Result | \`${result_line}\` |
| Source PBF | \`${STATS_FILTER_INPUT_NAME}\` ($(format_bytes "$STATS_FILTER_INPUT_SIZE")) |
| Filtered PBF (imported) | \`${STATS_FILTER_OUTPUT_NAME}\` ($(format_bytes "$STATS_FILTER_OUTPUT_SIZE")) |
| Filter duration | $(format_duration "$STATS_FILTER_DURATION") |
| Import duration | ${STATS_TOTAL_TIME} |
| Throughput | ${STATS_THROUGHPUT} |
| Dataset size | ${STATS_DATASET_SIZE} |
| Heap (Xmx/Xms) | ${IMPORT_MEMORY} |
| Import threads | ${IMPORT_THREADS} |
| Paikka version | ${paikka_version} (image \`${DOCKER_IMAGE}\`) |
| Grid level | ${grid_level} |
| CPU | ${STATS_CPU} · ${STATS_CPU_CORES} cores |
| RAM | ${STATS_RAM} |
| OS | ${STATS_OS} · kernel ${STATS_KERNEL} |
REPORT_HEADER

    cat <<'REPORT_BLOCK_OPEN'

<details><summary>Full paikka import statistics</summary>

```
REPORT_BLOCK_OPEN

    printf '%s\n' "$STATS_FINAL_BLOCK"

    cat <<'REPORT_BLOCK_CLOSE'
```

</details>
REPORT_BLOCK_CLOSE
}

###
# LOCAL: Removes the intermediate statistics artifacts after a successful run.
# On a degraded import the import log is kept for inspection.
###
local_cleanup_stats_files() {
    if [ "$STATS_IMPORT_SUCCESS" != "true" ]; then
        echo "Keeping '$DOWNLOAD_DIR/$IMPORT_LOG_FILE_NAME' for inspection (import was not fully successful)."
        return 0
    fi
    rm -f "$DOWNLOAD_DIR/$STATS_FILE_NAME" "$DOWNLOAD_DIR/$IMPORT_LOG_FILE_NAME"
}

###
# Posts the collected import statistics as a comment to the configured GitHub
# discussion. Uses the GraphQL API because the REST API does not support
# creating discussion comments. Only runs that were deployed and verified
# successfully are posted. Fails soft: warns and continues if the upload
# fails, because the data deployment itself has already completed at this point.
###
post_import_stats_to_github() {
    if [ "$POST_IMPORT_STATS" != "true" ]; then
        return 0
    fi

    if [ "$STATS_IMPORT_SUCCESS" != "true" ]; then
        echo "WARNING: Import was not fully successful (exit code 0 and 'IMPORT OUTCOME: OK' required). Skipping statistics upload." >&2
        return 0
    fi

    log "GITHUB: Posting import statistics to discussion #$GH_DISCUSSION_NUMBER in $GH_STATS_REPO"

    gather_system_info

    local body_file payload_file response_file resolve_file graphql_status comment_url discussion_id
    body_file="$(mktemp)"
    payload_file="$(mktemp)"
    response_file="$(mktemp)"
    resolve_file="$(mktemp)"

    build_stats_report > "$body_file"

    # Resolve the discussion node id for the configured discussion number
    jq -n \
        --arg owner "${GH_STATS_REPO%%/*}" \
        --arg name "${GH_STATS_REPO##*/}" \
        --argjson number "$GH_DISCUSSION_NUMBER" \
        '{query: "query($owner:String!,$name:String!,$number:Int!){ repository(owner:$owner,name:$name){ discussion(number:$number){ id } } }", variables: {owner: $owner, name: $name, number: $number}}' \
        > "$payload_file"

    graphql_status="$(curl -s -o "$resolve_file" -w "%{http_code}" \
        --max-time 60 \
        -X POST \
        "https://api.github.com/graphql" \
        -H "Authorization: Bearer ${GITHUB_TOKEN}" \
        -H "Content-Type: application/json" \
        --data @"$payload_file")" || graphql_status="curl_failed"

    discussion_id=$(jq -r '.data.repository.discussion.id // empty' "$resolve_file" 2>/dev/null)
    if [ -z "$discussion_id" ]; then
        echo "WARNING: Could not resolve discussion #$GH_DISCUSSION_NUMBER in $GH_STATS_REPO (HTTP status: $graphql_status). Continuing anyway." >&2
        echo "WARNING: GitHub response: $(cat "$resolve_file")" >&2
        rm -f "$body_file" "$payload_file" "$response_file" "$resolve_file"
        return 0
    fi

    jq -n \
        --arg discussion_id "$discussion_id" \
        --rawfile body "$body_file" \
        '{query: "mutation($id:ID!,$body:String!){ addDiscussionComment(input:{discussionId:$id, body:$body}){ comment{ url } } }", variables: {id: $discussion_id, body: $body}}' \
        > "$payload_file"

    graphql_status="$(curl -s -o "$response_file" -w "%{http_code}" \
        --max-time 60 \
        -X POST \
        "https://api.github.com/graphql" \
        -H "Authorization: Bearer ${GITHUB_TOKEN}" \
        -H "Content-Type: application/json" \
        --data @"$payload_file")" || graphql_status="curl_failed"

    comment_url=$(jq -r '.data.addDiscussionComment.comment.url // empty' "$response_file" 2>/dev/null)
    if [ -n "$comment_url" ]; then
        log "GITHUB: Import statistics posted successfully: $comment_url"
    else
        echo "WARNING: Posting import statistics failed (HTTP status: $graphql_status). Continuing anyway." >&2
        echo "WARNING: GitHub response: $(cat "$response_file")" >&2
    fi
    rm -f "$body_file" "$payload_file" "$response_file" "$resolve_file"
}
# ==============================================================================
# MAIN ORCHESTRATION FUNCTION
# ==============================================================================

main() {
    # Run all steps in sequence
    parse_args_and_configure "$@"
    local_prepare_directories
    if [ -z "$PBF_INPUT_PATH" ]; then
      local_download_planet_file
    else
      log "LOCAL: Using provided PBF file – skipping download"
    fi
    local_pull_docker_image
    local_filter_pbf
    local_create_import_bundle
    local_cleanup_pbf
    remote_sync_bundle
    remote_deploy_and_verify
    post_import_stats_to_github
    local_cleanup_stats_files
    purge_cloudflare_cache
    remote_cleanup_old_releases

    log "Update process finished."
}

# ==============================================================================
# SCRIPT ENTRYPOINT
# ==============================================================================
# This construct allows the script to be both executed directly to run the `main`
# function, and sourced (`source ./script.sh`) to load the functions for
# individual testing without executing anything.
###
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi
