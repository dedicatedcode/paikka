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
PBF_INPUT_FILE="planet-latest.osm.pbf"
PBF_FILTERED_FILE="planet-filtered.pbf"
IMPORT_DIR="import"
DOCKER_IMAGE="dedicatedcode/paikka:develop"

DOWNLOAD_DIR="${DOWNLOAD_DIR:-$LOCAL_WORK_DIR}"
IMPORT_DATA_DIR="${IMPORT_DATA_DIR:-$LOCAL_WORK_DIR/$IMPORT_DIR}"
IMPORT_MEMORY="${IMPORT_MEMORY:-16G}"
IMPORT_THREADS="${IMPORT_THREADS:-10}"

# --- Remote Machine Settings ---
REMOTE_BASE_DIR="/opt/paikka/data"

# --- Geocoder API Settings ---
GEOCODER_ADMIN_URL="http://localhost:8080/admin/refresh-db"
GEOCODER_TEST_URL_BASE="http://localhost:8080/v1/reverse"

# --- Verification Test Cases ---
declare -A TEST_CASES=(
  ["lat=52.516280&lon=13.377635"]="518071791" # Brandenburger Tor
  ["lat=48.85826&lon=2.2945008"]="5013364"    # Eiffel Tower
  ["lat=40.68924&lon=-74.044502"]="32965412"  # Statue of Liberty
)

# Global variables that will be set by parse_args_and_configure or environment
REMOTE_USER=""
REMOTE_HOST=""
GEOCODER_API_TOKEN=""
ZIP_FILENAME=""

# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

log() {
  echo -e "\n[$(date +'%Y-%m-%d %H:%M:%S')] --- $1 ---"
}

# ==============================================================================
# CORE LOGIC FUNCTIONS
# ==============================================================================

###
# Parses command-line arguments or uses environment variables for configuration.
###
parse_args_and_configure() {
    log "Step 0: Parsing arguments and setting configuration"

    # Precedence: Command-line arguments > Environment variables
    REMOTE_USER="${1:-$REMOTE_USER}"
    REMOTE_HOST="${2:-$REMOTE_HOST}"
    GEOCODER_API_TOKEN="${3:-$GEOCODER_API_TOKEN}"

    DOWNLOAD_DIR="${4:-$DOWNLOAD_DIR}"
    IMPORT_DATA_DIR="${5:-$IMPORT_DATA_DIR}"
    IMPORT_MEMORY="${6:-$IMPORT_MEMORY}"
    IMPORT_THREADS="${7:-$IMPORT_THREADS}"

    if [ -z "$REMOTE_USER" ] || [ -z "$REMOTE_HOST" ] || [ -z "$GEOCODER_API_TOKEN" ]; then
        echo "Usage: $0 <REMOTE_USER> <REMOTE_HOST> <API_TOKEN> [DOWNLOAD_DIR] [IMPORT_DATA_DIR] [MEMORY] [THREADS]"
        echo "  DOWNLOAD_DIR: Where to download PBF files (default: current directory)"
        echo "  IMPORT_DATA_DIR: Where to store import data (default: ./import)"
        echo "  MEMORY: Memory for import (default: 16G)"
        echo "  THREADS: Threads for import (default: 10)"
        echo "Error: Missing required configuration."
        exit 1
    fi
    echo "Configuration loaded for ${REMOTE_USER}@${REMOTE_HOST}"
    echo "  Download directory: $DOWNLOAD_DIR"
    echo "  Import data directory: $IMPORT_DATA_DIR"
    echo "  Import memory: $IMPORT_MEMORY"
    echo "  Import threads: $IMPORT_THREADS"}

###
# LOCAL: Creates the necessary working directories.
###
local_prepare_directories() {
    log "LOCAL: Ensuring import directory exists"
    mkdir -p "$DOWNLOAD_DIR"
    mkdir -p "$IMPORT_DATA_DIR"
    cd "$DOWNLOAD_DIR"
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
###
local_filter_pbf() {
    log "LOCAL: Filtering PBF file (approx. 50 mins)"
    cd "$DOWNLOAD_DIR"
    sudo docker run --rm -ti -v "$DOWNLOAD_DIR":/data "$DOCKER_IMAGE" prepare "$PBF_INPUT_FILE" "$PBF_FILTERED_FILE"
}

###
# LOCAL: Creates the geocoder import bundle from the filtered PBF.
###
local_create_import_bundle() {
    log "LOCAL: Creating import bundle (approx. 15 hours) with $IMPORT_MEMORY memory and $IMPORT_THREADS threads"
        sudo docker run --rm -ti -v "$DOWNLOAD_DIR":/download -v "$IMPORT_DATA_DIR":/import "$DOCKER_IMAGE" import \
          --memory "$IMPORT_MEMORY" \
          --threads "$IMPORT_THREADS" \
          --data-dir "/import/" \
          "/download/$PBF_FILTERED_FILE"
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

    # Convert TEST_CASES to a format that can be passed to remote shell
    local test_cases_str=""
    for key in "${!TEST_CASES}"; do
        test_cases_str+="[\"$key\"]=\"${TEST_CASES[$key]}\" "
    done

    ssh "${REMOTE_USER}@${REMOTE_HOST}" /bin/bash << EOF
  set -e
  BASE_DIR="${REMOTE_BASE_DIR}"
  API_TOKEN="${GEOCODER_API_TOKEN}"
  ADMIN_URL="${GEOCODER_ADMIN_URL}"
  TEST_URL_BASE="${GEOCODER_TEST_URL_BASE}"
  NEW_RELEASE_DIR="releases/${LATEST_RELEASE_DIR_NAME}"
  LIVE_DATA_SYMLINK="live_data"

  # Define TESTS array on remote side
  declare -A TESTS=($test_cases_str)

  echo_remote() {
    echo "[REMOTE] \$1"
  }

  cd "\$BASE_DIR"

  OLD_RELEASE_DIR=""
  [ -L "\$LIVE_DATA_SYMLINK" ] && OLD_RELEASE_DIR=\$(readlink \$LIVE_DATA_SYMLINK)

  echo_remote "Switching symlink: \$LIVE_DATA_SYMLINK -> \$NEW_RELEASE_DIR"
  ln -sfn "\$NEW_RELEASE_DIR" "\$LIVE_DATA_SYMLINK"

  echo_remote "Refreshing Geocoder DB..."
  HTTP_STATUS=\$(curl -s -o /dev/null -w "%{http_code}" -X POST -H "X-Admin-Token: \$API_TOKEN" "\$ADMIN_URL")

  if [ "\$HTTP_STATUS" -ne 200 ]; then
      echo_remote "ERROR: Refresh failed (\$HTTP_STATUS). Rolling back."
      [ -n "\$OLD_RELEASE_DIR" ] && ln -sfn "\$OLD_RELEASE_DIR" "\$LIVE_DATA_SYMLINK"
      exit 1
  fi

  # --- 2. Verify ---
  echo_remote "Verifying new data..."
  VERIFICATION_FAILED=0
  for query in "\${!TESTS}"; do
    ACTUAL_ID=\$(curl -s "\$TEST_URL_BASE?\$query" | jq -r '.[0].id // "not_found"')
    if [ "\$ACTUAL_ID" != "\${TESTS[\$query]}" ]; then
      echo_remote "  --> FAILED: For \$query, expected '\${TESTS[\$query]}', got '\$ACTUAL_ID'"
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
      curl -s -o /dev/null -X POST -H "X-Admin-Token: \$API_TOKEN" "\$ADMIN_URL"
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
# ==============================================================================
# MAIN ORCHESTRATION FUNCTION
# ==============================================================================

main() {
    # Run all steps in sequence
    parse_args_and_configure "$@"
    local_prepare_directories
    local_download_planet_file
    local_pull_docker_image
    local_filter_pbf
    local_create_import_bundle
    local_cleanup_pbf
    remote_sync_bundle
    remote_deploy_and_verify
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