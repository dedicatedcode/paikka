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

# --- Remote Machine Settings ---
REMOTE_BASE_DIR="/opt/paikka/data"

# --- Geocoder API Settings ---
GEOCODER_ADMIN_URL="http://localhost:8080/admin/refresh-db"
GEOCODER_TEST_URL_BASE="http://localhost:8080/v1/reverse"

# --- Verification Test Cases ---
declare -A TEST_CASES=(
  ["lat=52.516280&lon=13.377635"]="518071791"     # Brandenburger Tor
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

    if [ -z "$REMOTE_USER" ] || [ -z "$REMOTE_HOST" ] || [ -z "$GEOCODER_API_TOKEN" ]; then
        echo "Usage: $0 <REMOTE_USER> <REMOTE_HOST> <API_TOKEN>"
        echo "Error: Missing required configuration."
        echo "Provide arguments or set them as environment variables (REMOTE_USER, REMOTE_HOST, GEOCODER_API_TOKEN)."
        exit 1
    fi
    # Set a dynamic variable that depends on other config being set
    ZIP_FILENAME="paikka_import_$(date +%Y%m%d_%H%M%S).zip"
    echo "Configuration loaded for ${REMOTE_USER}@${REMOTE_HOST}"
}

###
# LOCAL: Creates the necessary working directories.
###
local_prepare_directories() {
    log "LOCAL: Ensuring import directory exists"
    mkdir -p "$LOCAL_WORK_DIR/$IMPORT_DIR"
    cd "$LOCAL_WORK_DIR"
}

###
# LOCAL: Downloads the latest OSM planet file.
###
local_download_planet_file() {
    log "LOCAL: Downloading latest OSM planet file (if newer)"
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
    sudo docker run --rm -v "$(pwd)":/data "$DOCKER_IMAGE" prepare "$PBF_INPUT_FILE" "$PBF_FILTERED_FILE"
}

###
# LOCAL: Creates the geocoder import bundle from the filtered PBF.
###
local_create_import_bundle() {
    log "LOCAL: Creating import bundle (approx. 15 hours)"
    sudo docker run --rm -ti -v "$(pwd)":/data "$DOCKER_IMAGE" import \
      --memory 16G \
      --threads 10 \
      --data-dir "/data/$IMPORT_DIR/" \
      "$PBF_FILTERED_FILE"
}

###
# LOCAL: Removes the large, intermediate PBF files.
###
local_cleanup_pbf() {
    log "LOCAL: Cleaning up intermediate PBF files"
    rm -f "$PBF_FILTERED_FILE" "$PBF_INPUT_FILE"
    echo "Deleted '$PBF_FILTERED_FILE' and '$PBF_INPUT_FILE'"
}
###
# REMOTE: Syncs the import directory to the remote server using rsync.
# Uses --link-dest to minimize bandwidth and remote disk usage.
###
remote_sync_bundle() {
    log "REMOTE: Syncing bundle via rsync (Delta transfer)"

    # We need to know the current live directory to use it as a link-dest
    CURRENT_LIVE=$(ssh "${REMOTE_USER}@${REMOTE_HOST}" "readlink -f ${REMOTE_BASE_DIR}/live_data || true")
    NEW_RELEASE_TIMESTAMP=$(date +%Y%m%d%H%M%S)
    NEW_RELEASE_DIR="${REMOTE_BASE_DIR}/releases/${NEW_RELEASE_TIMESTAMP}"

    # Ensure the remote releases directory exists
    ssh "${REMOTE_USER}@${REMOTE_HOST}" "mkdir -p ${REMOTE_BASE_DIR}/releases"

    # --link-dest makes rsync hard-link unchanged files from the current release
    # into the new release directory, saving bandwidth and space.
    rsync -avz --progress \
        ${CURRENT_LIVE:+--link-dest="$CURRENT_LIVE"} \
        "$LOCAL_WORK_DIR/$IMPORT_DIR/" \
        "${REMOTE_USER}@${REMOTE_HOST}:$NEW_RELEASE_DIR/"

    # Export this for the next step
    export LATEST_RELEASE_DIR_NAME="$NEW_RELEASE_TIMESTAMP"
}

###
# REMOTE: Deploys, verifies, and handles rollback/cleanup on the remote host.
###
remote_deploy_and_verify() {
    log "REMOTE: Executing remote deployment (Atomic Swap)"

    ssh "${REMOTE_USER}@${REMOTE_HOST}" /bin/bash << EOF
  set -e
  BASE_DIR="${REMOTE_BASE_DIR}"
  API_TOKEN="${GEOCODER_API_TOKEN}"
  ADMIN_URL="${GEOCODER_ADMIN_URL}"
  TEST_URL_BASE="${GEOCODER_TEST_URL_BASE}"
  NEW_RELEASE_DIR="releases/${LATEST_RELEASE_DIR_NAME}"
  LIVE_DATA_SYMLINK="live_data"

  cd "\$BASE_DIR"

  OLD_RELEASE_DIR=""
  [ -L "\$LIVE_DATA_SYMLINK" ] && OLD_RELEASE_DIR=\$(readlink \$LIVE_DATA_SYMLINK)

  echo "Switching symlink: \$LIVE_DATA_SYMLINK -> \$NEW_RELEASE_DIR"
  ln -sfn "\$NEW_RELEASE_DIR" "\$LIVE_DATA_SYMLINK"

  echo "Refreshing Geocoder DB..."
  HTTP_STATUS=\$(curl -s -o /dev/null -w "%{http_code}" -X POST -H "X-Admin-Token: \$API_TOKEN" "\$ADMIN_URL")

  if [ "\$HTTP_STATUS" -ne 200 ]; then
      echo "ERROR: Refresh failed (\$HTTP_STATUS). Rolling back."
      [ -n "\$OLD_RELEASE_DIR" ] && ln -sfn "\$OLD_RELEASE_DIR" "\$LIVE_DATA_SYMLINK"
      exit 1
  fi

  # ... [Verification logic remains the same] ...

  if [ \$VERIFICATION_FAILED -eq 0 ]; then
    echo "Success. Cleaning up old release..."
    [ -n "\$OLD_RELEASE_DIR" ] && [ "\$OLD_RELEASE_DIR" != "\$NEW_RELEASE_DIR" ] && rm -rf "\$OLD_RELEASE_DIR"
  fi
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