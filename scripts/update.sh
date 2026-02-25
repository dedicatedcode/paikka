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
GEOCODER_TEST_URL_BASE="http://localhost:8080/v1/poi/reverse"

# --- Verification Test Cases ---
declare -A TEST_CASES=(
  ["lat=48.85837&lon=2.2945"]="2523097"       # Eiffel Tower
  ["lat=52.51627&lon=13.3777"]="3063833"     # Brandenburger Tor
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
# LOCAL: Zips the contents of the import directory.
###
local_zip_bundle() {
    log "LOCAL: Zipping the import bundle"
    if [ ! -f "$IMPORT_DIR/paikka_metadata.json" ]; then
        echo "ERROR: Import directory does not seem to contain data. 'paikka_metadata.json' is missing. Aborting."
        return 1
    fi
    # cd into the directory to avoid creating a zip with 'import/' as a root folder
    (cd "$IMPORT_DIR" && zip -r "../$ZIP_FILENAME" .)
    log "Successfully created '$ZIP_FILENAME'"
}

###
# REMOTE: Transfers the zipped bundle to the remote server.
###
remote_transfer_bundle() {
    log "REMOTE: Copying '$ZIP_FILENAME' to remote server"
    scp "$ZIP_FILENAME" "${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_BASE_DIR}/"
}

###
# REMOTE: Deploys, verifies, and handles rollback/cleanup on the remote host.
###
remote_deploy_and_verify() {
    log "REMOTE: Executing remote deployment script"
    # This here-document is executed on the remote server via SSH.
    ssh "${REMOTE_USER}@${REMOTE_HOST}" /bin/bash << EOF
  # This block runs on the remote server
  set -e

  # --- Remote Configuration (values are passed from local script) ---
  BASE_DIR="${REMOTE_BASE_DIR}"
  ZIP_FILENAME="${ZIP_FILENAME}"
  API_TOKEN="${GEOCODER_API_TOKEN}"
  ADMIN_URL="${GEOCODER_ADMIN_URL}"
  TEST_URL_BASE="${GEOCODER_TEST_URL_BASE}"
  LIVE_DATA_SYMLINK="live_data"
  RELEASES_DIR="releases"

  declare -A TESTS=(
    ["lat=48.85837&lon=2.2945"]="2523097"
    ["lat=52.51627&lon=13.3777"]="3063833"
  )

  echo_remote() { echo "[REMOTE] \$1"; }

  echo_remote "Changing to base directory: \$BASE_DIR"
  cd "\$BASE_DIR"

  # --- 1. Prepare, Swap, Refresh ---
  echo_remote "Preparing new release from \$ZIP_FILENAME"
  NEW_RELEASE_DIR="\$RELEASES_DIR/\$(date +%Y%m%d%H%M%S)"
  mkdir -p "\$NEW_RELEASE_DIR"
  unzip -q "\$ZIP_FILENAME" -d "\$NEW_RELEASE_DIR"

  OLD_RELEASE_DIR=""
  [ -L "\$LIVE_DATA_SYMLINK" ] && OLD_RELEASE_DIR=\$(readlink \$LIVE_DATA_SYMLINK)

  echo_remote "Atomically switching live symlink to point to \$NEW_RELEASE_DIR"
  ln -sfn "\$NEW_RELEASE_DIR" "\$LIVE_DATA_SYMLINK"

  echo_remote "Calling refresh-db endpoint..."
  HTTP_STATUS=\$(curl -s -o /dev/null -w "%{http_code}" -X POST -H "Authorization: Bearer \$API_TOKEN" "\$ADMIN_URL")
  if [ "\$HTTP_STATUS" -ne 200 ]; then
      echo_remote "ERROR: Failed to refresh database (status \$HTTP_STATUS). Rolling back."
      [ -n "\$OLD_RELEASE_DIR" ] && ln -sfn "\$OLD_RELEASE_DIR" "\$LIVE_DATA_SYMLINK"
      exit 1
  fi

  # --- 2. Verify ---
  echo_remote "Verifying new data..."
  VERIFICATION_FAILED=0
  for query in "\${!TESTS[@]}"; do
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
      curl -s -o /dev/null -X POST -H "Authorization: Bearer \$API_TOKEN" "\$ADMIN_URL"
      echo_remote "Rollback to \$OLD_RELEASE_DIR complete. Faulty data in \$NEW_RELEASE_DIR is kept for inspection."
      exit 1
    else
      echo_remote "ERROR: Verification failed, but no previous version to roll back to!"
      exit 1
    fi
  else
    echo_remote "VERIFICATION SUCCEEDED. Cleaning up old release and archive."
    [ -n "\$OLD_RELEASE_DIR" ] && [ -d "\$OLD_RELEASE_DIR" ] && rm -rf "\$OLD_RELEASE_DIR"
    rm "\$ZIP_FILENAME"
    echo_remote "Deployment successful."
  fi
EOF
}

###
# LOCAL: Cleans up the zip file on the local machine after a successful run.
###
local_cleanup_zip() {
    log "LOCAL: Cleaning up local zip file: $ZIP_FILENAME"
    rm "$ZIP_FILENAME"
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
    local_zip_bundle
    remote_transfer_bundle
    remote_deploy_and_verify
    local_cleanup_zip

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