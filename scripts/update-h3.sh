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
#   ./scripts/build-h3-bundle.sh [OPTIONS]
#
# Options:
#   --env-file PATH       Path to .env file (default: ./scripts/.env)
#   --data-dir PATH       Directory for import data (default: ./data)
#   --jar-file PATH       Path to PAIKKA jar (auto-detected if not provided)
#   --memory SIZE         JVM heap size (default: 16g)
#   --threads NUM         Import threads (default: 10)
#   --pbf-file PATH       Use local PBF file instead of downloading
#   --version STR         Bundle version (default: YYYY-MM-DD-v1)
#   --output-dir PATH     Bundle output directory (default: ./dist)
#   --no-upload           Skip R2 upload (local bundle only)
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
PBF_FILTERED_FILE="planet-filtered.pbf"
DOCKER_IMAGE="dedicatedcode/paikka:latest"

# --- Local Paths ---
DOWNLOAD_DIR="${DOWNLOAD_DIR:-$LOCAL_WORK_DIR}"
DATA_DIR="${DATA_DIR:-$LOCAL_WORK_DIR}"

# --- Import Settings ---
IMPORT_MEMORY="${IMPORT_MEMORY:-16g}"
IMPORT_THREADS="${IMPORT_THREADS:-10}"
JAR_FILE="${JAR_FILE:-}"

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
    ENV_FILE="./scripts/.env"
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
            --jar-file)
                JAR_FILE="$2"
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
            --no-upload)
                NO_UPLOAD=true
                shift
                ;;
            -h|--help)
                echo "Usage: $0 [OPTIONS]"
                echo ""
                echo "Options:"
                echo "  --env-file PATH       Path to .env file (default: ./scripts/.env)"
                echo "  --data-dir PATH       Directory for import data (default: ./data)"
                echo "  --jar-file PATH       Path to PAIKKA jar (auto-detected if not provided)"
                echo "  --memory SIZE         JVM heap size (default: 16g)"
                echo "  --threads NUM         Import threads (default: 10)"
                echo "  --pbf-file PATH       Use local PBF file instead of downloading"
                echo "  --version STR         Bundle version (default: YYYY-MM-DD-v1)"
                echo "  --output-dir PATH     Bundle output directory (default: ./dist)"
                echo "  --no-upload           Skip R2 upload (local bundle only)"
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
    IMPORT_THREADS="${IMPORT_THREADS:-10}"
    VERSION="${VERSION:-$(date +%Y-%m-%d)-v1}"
    BUNDLE_OUTPUT_DIR="${BUNDLE_OUTPUT_DIR:-$LOCAL_WORK_DIR/dist}"

    # Validate PBF input if provided
    if [ -n "$PBF_INPUT_PATH" ] && [ ! -f "$PBF_INPUT_PATH" ]; then
        echo "Error: PBF file not found: $PBF_INPUT_PATH"
        exit 1
    fi

    # Auto-detect JAR file if not provided
    if [ -z "$JAR_FILE" ]; then
        JAR_FILE=$(find target -name "paikka-*.jar" -not -name "*-sources.jar" 2>/dev/null | head -1)
    fi

    # Validate JAR file
    if [ -n "$JAR_FILE" ] && [ ! -f "$JAR_FILE" ]; then
        echo "Error: JAR file not found: $JAR_FILE"
        exit 1
    fi

    # Display configuration
    echo "=========================================="
    echo "H3 Bundle Pipeline Configuration"
    echo "=========================================="
    echo "  Data directory:    $DATA_DIR"
    echo "  Import memory:     $IMPORT_MEMORY"
    echo "  Import threads:    $IMPORT_THREADS"
    echo "  JAR file:          ${JAR_FILE:-auto-detect}"
    echo "  Bundle version:    $VERSION"
    echo "  Bundle output:     $BUNDLE_OUTPUT_DIR"
    echo "  Skip upload:       $NO_UPLOAD"
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
    log "Step 2b: Pulling latest Docker image: $DOCKER_IMAGE"
    sudo docker pull "$DOCKER_IMAGE"
}

###
# LOCAL: Filters the PBF file using the Paikka container.
###
local_filter_pbf() {
    log "Step 3: Filtering PBF file"

    if [ -n "$PBF_INPUT_PATH" ]; then
        INPUT_DIR="$(dirname "$PBF_INPUT_PATH")"
        INPUT_FILE="$(basename "$PBF_INPUT_PATH")"
        sudo docker run --rm \
            -v "$INPUT_DIR":/input \
            -v "$DOWNLOAD_DIR":/data \
            "$DOCKER_IMAGE" prepare-boundaries "/input/$INPUT_FILE" "/data/$PBF_FILTERED_FILE"
    else
        sudo docker run --rm \
            -v "$DOWNLOAD_DIR":/data \
            "$DOCKER_IMAGE" prepare-boundaries "/data/$PBF_INPUT_FILE" "/data/$PBF_FILTERED_FILE"
    fi
}

###
# LOCAL: Runs the Java H3 import.
###
local_import_h3() {
    log "Step 4: Running H3 import"

    local PBF_TO_IMPORT="$DOWNLOAD_DIR/$PBF_FILTERED_FILE"

    cd "$LOCAL_WORK_DIR"
    ./scripts/import-boundaries.sh \
        --jar-file "$JAR_FILE" \
        --data-dir "$DATA_DIR" \
        --memory "$IMPORT_MEMORY" \
        --threads "$IMPORT_THREADS" \
        "$PBF_TO_IMPORT"
}

###
# LOCAL: Removes intermediate PBF files.
###
local_cleanup_pbf() {
    log "Step 5: Cleaning up intermediate PBF files"
    cd "$DOWNLOAD_DIR"
    rm -f "$PBF_FILTERED_FILE"
    if [ -z "$PBF_INPUT_PATH" ]; then
        rm -f "$PBF_INPUT_FILE"
    fi
    echo "Cleaned up filtered PBF file"
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

    ./scripts/upload-h3-bundle.sh \
        --dist-dir "$BUNDLE_OUTPUT_DIR"
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