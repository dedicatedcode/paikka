#!/bin/bash

#
#  This file is part of paikka.
#
#  Paikka is free software: you can redistribute it and/or
#  modify it under the terms of the GNU Affero General Public License
#  as published by the Free Software Foundation, either version 3 or
#  any later version.
#
#  Paikka is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied
#  warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#  See the GNU Affero General Public License for more details.
# You should have received a copy of the GNU Affero General Public License
# along with Paikka. If not, see <https://www.gnu.org/licenses/>.
#

# ==============================================================================
# PAIKKA H3 Bundle Pipeline
# ==============================================================================
# Single command to download, filter, import, zip, and upload H3 bundles.
#
# Usage:
#   ./scripts/update-h3.sh [OPTIONS]
#
# Options:
#   --env-file PATH       Path to .env file (default: ./.env)
#   --data-dir PATH       Directory for import data (default: ./)
#   --tmp-dir PATH        Scratch dir for RocksDB temp data, e.g. an NVMe mount
#                         (default: <data-dir>/tmp)
#   --memory SIZE         JVM heap size (default: 16g)
#   --threads NUM         Import threads (default: 10)
#   --pbf-file PATH       Use local PBF file instead of downloading
#   --version STR         Bundle version (default: YYYY-MM-DD-v1)
#   --output-dir PATH     Bundle output directory (default: ./dist)
#   --docker-image IMAGE  Docker image for filtering/importing (default: dedicatedcode/paikka:latest)
#   --no-pull             Do not pull the Docker image before use (for local test builds)
#   --no-upload           Skip R2 upload (local bundle only)
#   --clean               Remove the downloaded and filtered PBF files after a successful
#                         run (they are kept by default so filter/import can be re-run)
#   -h, --help            Show this help message
# ==============================================================================

set -e
set -o pipefail

# ==============================================================================
# SCRIPT CONFIGURATION AND GLOBAL DEFAULTS
# ==============================================================================

# --- General Settings ---
PLANET_URL="https://planet.osm.org/pbf/planet-latest.osm.pbf"
LOCAL_WORK_DIR="$(pwd)"
PBF_INPUT_FILE="planet-latest.osm.pbf"
PBF_FILTERED_FILE="planet-boundaries-filtered.pbf"
DOCKER_IMAGE_DEFAULT="dedicatedcode/paikka:latest"
DOCKER_IMAGE="${DOCKER_IMAGE:-$DOCKER_IMAGE_DEFAULT}"
PULL_DOCKER_IMAGE="${PULL_DOCKER_IMAGE:-true}"

# --- Local Paths ---
DOWNLOAD_DIR="${DOWNLOAD_DIR:-$LOCAL_WORK_DIR}"
DATA_DIR="${DATA_DIR:-$LOCAL_WORK_DIR}"
TMP_DIR="${TMP_DIR:-}"

# --- Import Settings ---
IMPORT_MEMORY="${IMPORT_MEMORY:-16g}"
IMPORT_INITIAL_MEMORY="${IMPORT_INITIAL_MEMORY:-2g}"
IMPORT_THREADS="${IMPORT_THREADS:-10}"

# --- Bundle Settings ---
VERSION="${VERSION:-$(date +%Y-%m-%d)-v1}"
DOWNLOAD_BASE_URL="${DOWNLOAD_BASE_URL:-https://h3-osm.dedicatedcode.com}"
BUNDLE_OUTPUT_DIR="${BUNDLE_OUTPUT_DIR:-$LOCAL_WORK_DIR/dist}"

# --- R2 Upload Settings (from .env) ---
R2_ACCOUNT_ID="${R2_ACCOUNT_ID:-}"
R2_ACCESS_KEY_ID="${R2_ACCESS_KEY_ID:-}"
R2_SECRET_ACCESS_KEY="${R2_SECRET_ACCESS_KEY:-}"
R2_BUCKET="${R2_BUCKET:-}"
R2_PATH="${R2_PATH:-}"

# --- Script Flags ---
PBF_INPUT_PATH=""
NO_UPLOAD=false
CLEAN_PBF="${CLEAN_PBF:-false}"

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
# Parses command-line arguments and loads environment configuration.
###
parse_args_and_configure() {
    log "Step 0: Parsing arguments and setting configuration"

    # Load .env file if it exists (environment variables can override these)
    ENV_FILE="$LOCAL_WORK_DIR/.env"
    LEGACY_ENV_FILE="$LOCAL_WORK_DIR/scripts/.env"
    if [ ! -f "$ENV_FILE" ] && [ -f "$LEGACY_ENV_FILE" ]; then
        echo "Warning: $LEGACY_ENV_FILE is deprecated; move it to $ENV_FILE" >&2
        ENV_FILE="$LEGACY_ENV_FILE"
    fi
    if [ -f "$ENV_FILE" ]; then
        echo "Loading configuration from $ENV_FILE"
        set -a
        source "$ENV_FILE"
        set +a
    fi

    # Parse command-line arguments (highest precedence)
    while [[ $# -gt 0 ]]; do
        case $1 in
            --env-file)
                ENV_FILE="$2"
                if [ -f "$ENV_FILE" ]; then
                    echo "Loading configuration from $ENV_FILE"
                    set -a
                    source "$ENV_FILE"
                    set +a
                else
                    echo "Error: .env file not found: $ENV_FILE"
                    exit 1
                fi
                shift 2
                ;;
            --data-dir)
                DATA_DIR="$2"
                shift 2
                ;;
            --tmp-dir)
                TMP_DIR="$2"
                shift 2
                ;;
            --memory)
                IMPORT_MEMORY="$2"
                shift 2
                ;;
            --threads)
                IMPORT_THREADS="$2"
                shift 2
                ;;
            --pbf-file)
                PBF_INPUT_PATH="$2"
                shift 2
                ;;
            --version)
                VERSION="$2"
                shift 2
                ;;
            --output-dir)
                BUNDLE_OUTPUT_DIR="$2"
                shift 2
                ;;
            --docker-image)
                DOCKER_IMAGE="$2"
                shift 2
                ;;
            --no-pull)
                PULL_DOCKER_IMAGE=false
                shift
                ;;
            --no-upload)
                NO_UPLOAD=true
                shift
                ;;
            --clean)
                CLEAN_PBF=true
                shift
                ;;
            -h|--help)
                echo "Usage: $0 [OPTIONS]"
                echo ""
                echo "Options:"
                echo "  --env-file PATH       Path to .env file (default: ./.env)"
                echo "  --data-dir PATH       Directory for import data (default: ./)"
                echo "  --tmp-dir PATH        Scratch dir for RocksDB temp data, e.g. an NVMe mount"
                echo "                        (default: <data-dir>/tmp)"
                echo "  --memory SIZE         JVM heap size (default: 16g)"
                echo "  --threads NUM         Import threads (default: 10)"
                echo "  --pbf-file PATH       Use local PBF file instead of downloading"
                echo "  --version STR         Bundle version (default: YYYY-MM-DD-v1)"
                echo "  --output-dir PATH     Bundle output directory (default: ./dist)"
                echo "  --docker-image IMAGE  Docker image for filtering/importing (default: dedicatedcode/paikka:latest)"
                echo "  --no-pull             Do not pull the Docker image before use (for local test builds)"
                echo "  --no-upload           Skip R2 upload (local bundle only)"
                echo "  --clean               Remove the downloaded and filtered PBF files after a"
                echo "                        successful run (kept by default so the import can be re-run)"
                echo "  -h, --help            Show this help message"
                exit 1
                ;;
            *)
                echo "Error: Unknown option: $1"
                echo ""
                echo "Usage: $0 [OPTIONS]"
                exit 1
                ;;
        esac
    done

    # Re-apply environment variable defaults (env vars take precedence over .env file)
    DOWNLOAD_DIR="${DOWNLOAD_DIR:-$LOCAL_WORK_DIR}"
    DATA_DIR="${DATA_DIR:-$LOCAL_WORK_DIR}"
    IMPORT_MEMORY="${IMPORT_MEMORY:-16g}"
    IMPORT_INITIAL_MEMORY="${IMPORT_INITIAL_MEMORY:-2g}"
    IMPORT_THREADS="${IMPORT_THREADS:-10}"
    VERSION="${VERSION:-$(date +%Y-%m-%d)-v1}"
    BUNDLE_OUTPUT_DIR="${BUNDLE_OUTPUT_DIR:-$LOCAL_WORK_DIR/dist}"
    DOCKER_IMAGE="${DOCKER_IMAGE:-$DOCKER_IMAGE_DEFAULT}"
    PULL_DOCKER_IMAGE="${PULL_DOCKER_IMAGE:-true}"
    TMP_DIR="${TMP_DIR:-}"

    # Validate PBF input if provided
    if [ -n "$PBF_INPUT_PATH" ] && [ ! -f "$PBF_INPUT_PATH" ]; then
        echo "Error: PBF file not found: $PBF_INPUT_PATH"
        exit 1
    fi

    # Display configuration
    echo "=========================================="
    echo "H3 Bundle Pipeline Configuration"
    echo "=========================================="
    echo "  Data directory:    $DATA_DIR"
    echo "  Scratch directory: ${TMP_DIR:-$DATA_DIR/tmp}"
    echo "  Import max memory: $IMPORT_MEMORY"
    echo "  Import init memory: $IMPORT_INITIAL_MEMORY"
    echo "  Import threads:    $IMPORT_THREADS"
    echo "  Bundle version:    $VERSION"
    echo "  Bundle output:     $BUNDLE_OUTPUT_DIR"
    echo "  Docker image:      $DOCKER_IMAGE (pull: $PULL_DOCKER_IMAGE)"
    echo "  Skip upload:       $NO_UPLOAD"
    echo "  Clean PBF files:   $CLEAN_PBF"
    if [ -n "$PBF_INPUT_PATH" ]; then
        echo "  PBF input:         $PBF_INPUT_PATH"
    else
        echo "  PBF input:         Download from $PLANET_URL"
    fi
    echo "=========================================="

    # Validate R2 upload settings (only if upload is enabled)
    if [ "$NO_UPLOAD" = false ]; then
        local missing_vars=()
        [ -z "$R2_ACCOUNT_ID" ] && missing_vars+=("R2_ACCOUNT_ID")
        [ -z "$R2_ACCESS_KEY_ID" ] && missing_vars+=("R2_ACCESS_KEY_ID")
        [ -z "$R2_SECRET_ACCESS_KEY" ] && missing_vars+=("R2_SECRET_ACCESS_KEY")
        [ -z "$R2_BUCKET" ] && missing_vars+=("R2_BUCKET")

        if [ ${#missing_vars} -gt 0 ]; then
            echo ""
            echo "Error: Missing required R2 configuration:"
            for var in ${missing_vars}; do
                echo "  - $var"
            done
            echo ""
            echo "Provide via --env-file, .env file, or environment variables."
            exit 1
        fi
    fi
}

###
# LOCAL: Creates the necessary working directories.
###
local_prepare_directories() {
    log "Step 1: Preparing directories"
    mkdir -p "$DOWNLOAD_DIR"
    mkdir -p "$DATA_DIR"
    if [ -n "$TMP_DIR" ]; then
        mkdir -p "$TMP_DIR"
    fi
    mkdir -p "$BUNDLE_OUTPUT_DIR"
}

###
# LOCAL: Downloads the latest OSM planet file.
###
local_download_planet_file() {
    if [ -n "$PBF_INPUT_PATH" ]; then
        log "Step 2a: Using provided PBF file – skipping download"
        return 0
    fi

    log "Step 2a: Downloading latest OSM planet file"
    cd "$DOWNLOAD_DIR"
    wget -N "$PLANET_URL"
}

###
# LOCAL: Pulls the latest version of the Paikka Docker image.
###
local_pull_docker_image() {
    if [ "$PULL_DOCKER_IMAGE" != "true" ]; then
        log "Step 2b: Skipping Docker image pull (--no-pull)"
        return 0
    fi

    log "Step 2b: Pulling Docker image: $DOCKER_IMAGE"
    if ! sudo docker pull "$DOCKER_IMAGE"; then
        if [ "$DOCKER_IMAGE" != "$DOCKER_IMAGE_DEFAULT" ]; then
            echo "WARNING: Could not pull '$DOCKER_IMAGE'; using the local image instead." >&2
        else
            echo "Error: Could not pull '$DOCKER_IMAGE'." >&2
            return 1
        fi
    fi
}

###
# LOCAL: Filters the PBF file using the Paikka container.
###
local_filter_pbf() {
    if [ -n "$PBF_INPUT_PATH" ]; then
        log "Step 3: Skipping filter – using provided PBF directly"
        return 0
    fi

    log "Step 3: Filtering PBF file"

    sudo docker run --rm \
        -v "$DOWNLOAD_DIR":/data \
        "$DOCKER_IMAGE" prepare-boundaries "/data/$PBF_INPUT_FILE" "/data/$PBF_FILTERED_FILE"
}
###
# LOCAL: Runs the Java H3 import.
###
local_import_h3() {
    log "Step 4: Running H3 import"

    local scratch_args=()
    local scratch_mount=()
    if [ -n "$TMP_DIR" ]; then
        mkdir -p "$TMP_DIR"
        scratch_args=(--tmp-dir /scratch)
        scratch_mount=(-v "$TMP_DIR":/scratch)
    fi

    if [ -n "$PBF_INPUT_PATH" ]; then
        INPUT_DIR="$(dirname "$PBF_INPUT_PATH")"
        INPUT_FILE="$(basename "$PBF_INPUT_PATH")"
        sudo docker run --rm \
            -v "$INPUT_DIR":/input \
            -v "$DATA_DIR":/data \
            "${scratch_mount[@]+"${scratch_mount[@]}"}" \
            "$DOCKER_IMAGE" \
            import-boundaries \
            --data-dir /data \
            "${scratch_args[@]+"${scratch_args[@]}"}" \
            --memory "$IMPORT_MEMORY" \
            --initial-memory "$IMPORT_INITIAL_MEMORY" \
            --threads "$IMPORT_THREADS" \
            "/input/$INPUT_FILE"
    else
        sudo docker run --rm \
            -v "$DOWNLOAD_DIR":/input \
            -v "$DATA_DIR":/data \
            "${scratch_mount[@]+"${scratch_mount[@]}"}" \
            "$DOCKER_IMAGE" \
            import-boundaries \
            --data-dir /data \
            "${scratch_args[@]+"${scratch_args[@]}"}" \
            --memory "$IMPORT_MEMORY" \
            --initial-memory "$IMPORT_INITIAL_MEMORY" \
            --threads "$IMPORT_THREADS" \
            "/input/$PBF_FILTERED_FILE"
    fi
}
###
# LOCAL: Removes intermediate PBF files when --clean is given.
###
local_cleanup_pbf() {
    if [ "$CLEAN_PBF" != "true" ]; then
        log "Step 5: Keeping intermediate PBF files (use --clean to remove them)"
        return 0
    fi

    log "Step 5: Cleaning up intermediate PBF files"

    if [ -n "$PBF_INPUT_PATH" ]; then
        echo "No intermediate files to clean up (user-provided PBF)"
        return 0
    fi

    cd "$DOWNLOAD_DIR"
    rm -f "$PBF_FILTERED_FILE"
    rm -f "$PBF_INPUT_FILE"
    echo "Cleaned up downloaded PBF files"
}
###
# LOCAL: Creates the H3 RocksDB bundle ZIP and manifest.
###
local_create_bundle() {
    log "Step 6: Creating H3 bundle"

    ./scripts/create-h3-bundle.sh \
        --db-dir "$DATA_DIR" \
        --version "$VERSION" \
        --url "$DOWNLOAD_BASE_URL" \
        --output-dir "$BUNDLE_OUTPUT_DIR"
}

###
# LOCAL: Uploads the bundle to Cloudflare R2.
###
local_upload_bundle() {
    if [ "$NO_UPLOAD" = true ]; then
        log "Step 7: Skipping R2 upload (--no-upload)"
        return 0
    fi

    log "Step 7: Uploading bundle to R2"

    local upload_args=(
        --dist-dir "$BUNDLE_OUTPUT_DIR"
    )
    if [ -n "$ENV_FILE" ] && [ -f "$ENV_FILE" ]; then
        upload_args+=(--env-file "$ENV_FILE")
    fi

    ./scripts/upload-h3-bundle.sh "${upload_args}"
}
# ==============================================================================
# MAIN ORCHESTRATION FUNCTION
# ==============================================================================

main() {
    parse_args_and_configure "$@"
    local_prepare_directories
    local_download_planet_file
    local_pull_docker_image
    local_filter_pbf
    local_import_h3
    local_cleanup_pbf
    local_create_bundle
    local_upload_bundle

    log "H3 bundle pipeline completed successfully"
    echo "=========================================="
    echo "  Bundle: $BUNDLE_OUTPUT_DIR/h3-rocksdb-${VERSION}.zip"
    echo "  Manifest: $BUNDLE_OUTPUT_DIR/manifest.json"
    echo "=========================================="
}

# ==============================================================================
# SCRIPT ENTRYPOINT
# ==============================================================================

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi