#!/usr/bin/env bash

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

# Exit immediately if a command exits with a non-zero status
set -e

# Default values
VERSION=$(date +%Y-%m-%d)-v1
DOWNLOAD_BASE_URL="https://h3-osm.dedicatedcode.com"
OUTPUT_DIR="./dist"

# Function to display usage instructions
usage() {
    echo "Usage: $0 -d <rocksdb_parent_directory> [OPTIONS]"
    echo ""
    echo "Required:"
    echo "  -d, --db-dir       Path to the directory containing h3_to_osm, region_metadata, and region_geometry"
    echo ""
    echo "Options:"
    echo "  -v, --version      Version string for the bundle (default: $VERSION)"
    echo "  -u, --url          Base CDN URL where the zip will be hosted (default: $DOWNLOAD_BASE_URL)"
    echo "  -o, --output-dir   Where to write the ZIP and manifest.json (default: $OUTPUT_DIR)"
    echo "  -h, --help         Show this help message"
    exit 1
}

# Parse command line arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        -d|--db-dir) DB_DIR="$2"; shift ;;
        -v|--version) VERSION="$2"; shift ;;
        -u|--url) DOWNLOAD_BASE_URL="$2"; shift ;;
        -o|--output-dir) OUTPUT_DIR="$2"; shift ;;
        -h|--help) usage ;;
        *) echo "Unknown parameter passed: $1"; usage ;;
    esac
    shift
done

# Validate required argument
if [ -z "$DB_DIR" ]; then
    echo "Error: Database source directory (-d / --db-dir) is required."
    usage
fi

# Ensure absolute paths
DB_DIR_ABS=$(cd "$DB_DIR" && pwd)
mkdir -p "$OUTPUT_DIR"
OUTPUT_DIR_ABS=$(cd "$OUTPUT_DIR" && pwd)

# Check that the three essential database directories actually exist
REQUIRED_DIRS=("h3_to_osm" "region_metadata" "region_geometry")
for dir in "${REQUIRED_DIRS[@]}"; do
    if [ ! -d "$DB_DIR_ABS/$dir" ]; then
        echo "Error: Required directory '$dir' not found inside $DB_DIR_ABS"
        exit 1
    fi
done

echo "=========================================="
echo "Preparing H3 RocksDB Bundle"
echo "Version:    $VERSION"
echo "Source:     $DB_DIR_ABS"
echo "Output:     $OUTPUT_DIR_ABS"
echo "=========================================="

ZIP_FILENAME="h3-rocksdb-${VERSION}.zip"
ZIP_PATH="$OUTPUT_DIR_ABS/$ZIP_FILENAME"

# 1. Clean up any pre-existing zip at the target location to avoid mixing versions
rm -f "$ZIP_PATH"

echo "Creating ZIP archive..."
# We run zip inside the source directory so that the subfolders are at the ROOT of the zip.
# -r: recursive
# -q: quiet
# -x "**/LOCK": IMPORTANT. Excludes RocksDB native file system locks which prevent startup on target systems.
(
    cd "$DB_DIR_ABS"
    zip -r -q "$ZIP_PATH" h3_to_osm region_metadata region_geometry osm_names.tsv -x "**/LOCK"
)

echo "Calculating bundle metadata..."
# Check OS to use the correct parameters for 'stat' and 'sha256' tools (Mac vs Linux)
if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS
    SIZE_BYTES=$(stat -f%z "$ZIP_PATH")
    SHA256=$(shasum -a 256 "$ZIP_PATH" | awk '{print $1}')
else
    # Linux / WSL
    SIZE_BYTES=$(stat -c%s "$ZIP_PATH")
    SHA256=$(sha256sum "$ZIP_PATH" | awk '{print $1}')
fi

# 2. Build the exact Manifest structure the Spring Boot Lifecycle Manager expects
MANIFEST_PATH="$OUTPUT_DIR_ABS/manifest.json"
DOWNLOAD_URL="${DOWNLOAD_BASE_URL}/${ZIP_FILENAME}"

cat <<EOF > "$MANIFEST_PATH"
{
  "version": "${VERSION}",
  "downloadUrl": "${DOWNLOAD_URL}",
  "sha256": "${SHA256}",
  "sizeBytes": ${SIZE_BYTES}
}
EOF

echo "=========================================="
echo "Success! Package files generated:"
echo "Archive:  $ZIP_PATH ($(numfmt --to=iec --suffix=B $SIZE_BYTES) / $SIZE_BYTES bytes)"
echo "Checksum: $SHA256"
echo "Manifest: $MANIFEST_PATH"
echo "=========================================="