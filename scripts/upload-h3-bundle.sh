#!/usr/bin/env bash

set -e

# --- Configuration & Defaults ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE=""  # No default - must be provided or use environment variables
DIST_DIR="$SCRIPT_DIR/dist"
KEEP_COUNT="${KEEP_COUNT:-2}"

# Show help/usage instructions
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -d, --dist-dir     Path to folder containing the ZIP file and manifest.json (Default: $DIST_DIR)"
    echo "  -e, --env-file     Path to .env file with R2 credentials (optional if set via environment)"
    echo "  -k, --keep         Number of newest ZIP versions to keep in the bucket (Default: $KEEP_COUNT)"
    echo "  -h, --help         Show this help message"
}

# Parse command line arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        -d|--dist-dir) DIST_DIR="$2"; shift ;;
        -e|--env-file) ENV_FILE="$2"; shift ;;
        -k|--keep) KEEP_COUNT="$2"; shift ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown parameter: $1"; usage; exit 1 ;;
    esac
    shift
done

if ! [[ "$KEEP_COUNT" =~ ^[0-9]+$ ]]; then
    echo "Error: --keep must be a non-negative integer (got: $KEEP_COUNT)"
    exit 1
fi

# Load credentials from .env file (if provided)
if [ -n "$ENV_FILE" ]; then
    if [ -f "$ENV_FILE" ]; then
        source "$ENV_FILE"
    else
        echo "Error: Configuration file .env was not found at: $ENV_FILE"
        exit 1
    fi
fi

# AWS CLI check
if ! command -v aws &> /dev/null; then
    echo "Error: The AWS CLI is not installed on this server."
    exit 1
fi

# Validate specified dist directory path and convert to absolute path
if [ ! -d "$DIST_DIR" ]; then
    echo "Error: The specified dist directory does not exist: $DIST_DIR"
    exit 1
fi
DIST_DIR_ABS=$(cd "$DIST_DIR" && pwd)

# Setup endpoint URL for Cloudflare R2
R2_ENDPOINT="https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com"

# Set environment variables for AWS CLI
export AWS_ACCESS_KEY_ID="$R2_ACCESS_KEY_ID"
export AWS_SECRET_ACCESS_KEY="$R2_SECRET_ACCESS_KEY"
export AWS_DEFAULT_REGION="auto"

# Resolve local files
MANIFEST_FILE="$DIST_DIR_ABS/manifest.json"

if [ ! -f "$MANIFEST_FILE" ]; then
    echo "Error: manifest.json was not found in folder $DIST_DIR_ABS."
    echo "Please ensure the bundle script was executed successfully there."
    exit 1
fi

# Extract download URL and filename from the local manifest.json
ZIP_URL=$(grep -o '"downloadUrl": *"[^"]*"' "$MANIFEST_FILE" | grep -o '"[^"]*"$' | tr -d '"')
ZIP_FILENAME=$(basename "$ZIP_URL")
ZIP_FILE="$DIST_DIR_ABS/$ZIP_FILENAME"

if [ ! -f "$ZIP_FILE" ]; then
    echo "Error: The bundle ZIP file was not found at: $ZIP_FILE"
    exit 1
fi

# Normalize remote paths
REMOTE_PREFIX=""
if [ -n "$R2_PATH" ]; then
    REMOTE_PREFIX="${R2_PATH%/}/"
fi

REMOTE_ZIP_KEY="${REMOTE_PREFIX}${ZIP_FILENAME}"
REMOTE_MANIFEST_KEY="${REMOTE_PREFIX}manifest.json"

echo "=========================================="
echo "Uploading H3 RocksDB Bundle to R2"
echo "Source Dir: $DIST_DIR_ABS"
echo "Bundle:     $ZIP_FILENAME"
echo "Bucket:     $R2_BUCKET"
echo "Prefix:     ${REMOTE_PREFIX:-}"
echo "=========================================="

# 1. Upload the heavy ZIP file first
echo "Uploading $ZIP_FILENAME..."
aws s3 cp "$ZIP_FILE" "s3://$R2_BUCKET/$REMOTE_ZIP_KEY" \
    --endpoint-url "$R2_ENDPOINT" \
    --cache-control "public, max-age=31536000, immutable"

if ! aws s3api head-object --endpoint-url "$R2_ENDPOINT" --bucket "$R2_BUCKET" --key "$REMOTE_ZIP_KEY" > /dev/null 2>&1; then
    echo "WARNING: $REMOTE_ZIP_KEY not found in bucket after upload." >&2
fi

# 2. Upload manifest last for atomic update execution
echo "Uploading manifest.json..."
aws s3 cp "$MANIFEST_FILE" "s3://$R2_BUCKET/$REMOTE_MANIFEST_KEY" \
    --endpoint-url "$R2_ENDPOINT" \
    --cache-control "no-cache, no-store, must-revalidate" \
    --content-type "application/json"

echo "Files successfully uploaded."
echo "------------------------------------------"
echo "Applying retention policy (keeping the $KEEP_COUNT newest ZIP files)..."

# 3. List all ZIPs in the bucket, newest first
ZIP_LISTING=$(aws s3api list-objects-v2 \
    --endpoint-url "$R2_ENDPOINT" \
    --bucket "$R2_BUCKET" \
    --prefix "$REMOTE_PREFIX" \
    --query "Contents[?ends_with(Key, '.zip')] | sort_by(@, &LastModified)[::-1].Key" \
    --output text)

# Normalize the listing into one key per line (text output may join keys with tabs)
ZIP_ARRAY=()
while IFS= read -r key; do
    [ -n "$key" ] && [ "$key" != "None" ] && ZIP_ARRAY+=("$key")
done < <(printf '%s\n' "$ZIP_LISTING" | tr '\t' '\n')

TOTAL_ZIPS=${#ZIP_ARRAY[@]}
echo "$TOTAL_ZIPS ZIP file(s) found in bucket."

if [ "$TOTAL_ZIPS" -le "$KEEP_COUNT" ]; then
    echo "Only $TOTAL_ZIPS version(s) found in bucket. No cleanup required."
else
    echo "Keeping:"
    for ((i=0; i<KEEP_COUNT; i++)); do
        echo "  - ${ZIP_ARRAY[$i]}"
    done

    DELETE_KEYS=()
    for ((i=KEEP_COUNT; i<TOTAL_ZIPS; i++)); do
        OLD_ZIP_KEY="${ZIP_ARRAY[$i]}"
        if [ -z "$OLD_ZIP_KEY" ]; then
            continue
        elif [ "$OLD_ZIP_KEY" = "$REMOTE_ZIP_KEY" ]; then
            echo "Keeping $OLD_ZIP_KEY (bundle uploaded by this run)"
        elif [[ "$OLD_ZIP_KEY" != *.zip ]]; then
            echo "WARNING: unexpected key '$OLD_ZIP_KEY' in bucket listing; aborting cleanup." >&2
            DELETE_KEYS=()
            break
        else
            DELETE_KEYS+=("$OLD_ZIP_KEY")
        fi
    done

    if [ ${#DELETE_KEYS[@]} -eq 0 ]; then
        echo "Nothing to delete."
    else
        echo "Retaining the $KEEP_COUNT newest version(s). Deleting ${#DELETE_KEYS[@]} older version(s)..."
        for OLD_ZIP_KEY in "${DELETE_KEYS[@]}"; do
            echo "Deleting old package: s3://$R2_BUCKET/$OLD_ZIP_KEY"
            aws s3 rm "s3://$R2_BUCKET/$OLD_ZIP_KEY" \
                --endpoint-url "$R2_ENDPOINT"
        done
    fi
fi

echo "=========================================="
echo "Deployment & Cleanup complete!"
echo "=========================================="