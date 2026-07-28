#!/usr/bin/env bash

set -e

# --- Configuration & Defaults ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"
DIST_DIR="$SCRIPT_DIR/dist"  # Default fallback if no directory parameter is provided

# Show help/usage instructions
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -d, --dist-dir     Path to folder containing the ZIP file and manifest.json (Default: $DIST_DIR)"
    echo "  -h, --help         Show this help message"
    exit 1
}

# Parse command line arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        -d|--dist-dir) DIST_DIR="$2"; shift ;;
        -h|--help) usage ;;
        *) echo "Unknown parameter: $1"; usage ;;
    esac
    shift
done

# Load credentials from .env file
if [ -f "$ENV_FILE" ]; then
    source "$ENV_FILE"
else
    echo "Error: Configuration file .env was not found at: $ENV_FILE"
    exit 1
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
echo "Prefix:     ${REMOTE_PREFIX:-[root]}"
echo "=========================================="

# 1. Upload the heavy ZIP file first
echo "Uploading $ZIP_FILENAME..."
aws s3 cp "$ZIP_FILE" "s3://$R2_BUCKET/$REMOTE_ZIP_KEY" \
    --endpoint-url "$R2_ENDPOINT" \
    --cache-control "public, max-age=31536000, immutable"

# 2. Upload manifest last for atomic update execution
echo "Uploading manifest.json..."
aws s3 cp "$MANIFEST_FILE" "s3://$R2_BUCKET/$REMOTE_MANIFEST_KEY" \
    --endpoint-url "$R2_ENDPOINT" \
    --cache-control "no-cache, no-store, must-revalidate" \
    --content-type "application/json"

echo "Files successfully uploaded."
echo "------------------------------------------"
echo "Applying retention policy (Keeping only the 2 newest ZIP files)..."

# 3. List all ZIPs in the bucket, sorted chronologically (oldest first)
ZIPS_IN_BUCKET=$(aws s3api list-objects-v2 \
    --endpoint-url "$R2_ENDPOINT" \
    --bucket "$R2_BUCKET" \
    --prefix "$REMOTE_PREFIX" \
    --query "Contents[?ends_with(Key, '.zip')] | sort_by(@, &LastModified)[].Key" \
    --output text)

# Convert output into a Bash array
read -r -a ZIP_ARRAY <<< "$ZIPS_IN_BUCKET"
TOTAL_ZIPS=${#ZIP_ARRAY[@]}

echo "$TOTAL_ZIPS ZIP file(s) found in bucket."

# If more than 2 versions are present, clean up the oldest
if [ "$TOTAL_ZIPS" -gt 2 ]; then
    DELETE_COUNT=$((TOTAL_ZIPS - 2))
    echo "Retaining the 2 newest versions. Deleting $DELETE_COUNT older version(s)..."

    for ((i=0; i<DELETE_COUNT; i++)); do
        OLD_ZIP_KEY="${ZIP_ARRAY[$i]}"
        echo "Deleting old package: s3://$R2_BUCKET/$OLD_ZIP_KEY"

        aws s3 rm "s3://$R2_BUCKET/$OLD_ZIP_KEY" \
            --endpoint-url "$R2_ENDPOINT"
    done
else
    echo "Only $TOTAL_ZIPS version(s) found in bucket. No cleanup required."
fi

echo "=========================================="
echo "Deployment & Cleanup complete!"
echo "=========================================="