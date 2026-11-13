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

REQUIRED_FILES=("osm_names.tsv" "region_hierarchy.tsv")
for file in "${REQUIRED_FILES[@]}"; do
    if [ ! -f "$DB_DIR_ABS/$file" ]; then
        echo "Error: Required file '$file' not found inside $DB_DIR_ABS"
        exit 1
    fi
done

# ---------------------------------------------------------------------------
# Hierarchy sanity check
#
# Fail only on structural problems. Root counts are reported, not thresholded:
# regional extracts legitimately contain regions whose parent relation was
# clipped away, while a planet build should have complete country coverage.
# ---------------------------------------------------------------------------
HIERARCHY_FILE="$DB_DIR_ABS/region_hierarchy.tsv"
COUNTRIES_FILE="$DB_DIR_ABS/region_countries.tsv"

if [ "${SKIP_HIERARCHY_CHECK:-false}" = "true" ]; then
    echo "Skipping region hierarchy validation (SKIP_HIERARCHY_CHECK=true)"
else
echo "Validating region hierarchy..."
TOTAL_ROWS=$(awk -F'\t' 'END{print NR}' "$HIERARCHY_FILE")
ROOT_ROWS=$(awk -F'\t' '$2==""{c++} END{print c+0}' "$HIERARCHY_FILE")
HIGH_LEVEL_ROOTS=$(awk -F'\t' '$2=="" && $4>2{c++} END{print c+0}' "$HIERARCHY_FILE")
MISSING_PARENTS=$(awk -F'\t' 'NR==FNR{ids[$1]=1; next} $2!="" && !($2 in ids){c++} END{print c+0}' "$HIERARCHY_FILE" "$HIERARCHY_FILE")
DUPLICATE_IDS=$(awk -F'\t' '{c[$1]++} END{for (id in c) if(c[id]>1) d++; print d+0}' "$HIERARCHY_FILE")

echo "  rows:               $TOTAL_ROWS"
echo "  roots:              $ROOT_ROWS"
echo "  roots above level 2: $HIGH_LEVEL_ROOTS"
echo "  missing parents:    $MISSING_PARENTS"
echo "  duplicate IDs:      $DUPLICATE_IDS"

if [ "$MISSING_PARENTS" -gt 0 ]; then
    echo "Error: $MISSING_PARENTS regions reference a parent that is not in the hierarchy"
    exit 1
fi
if [ "$DUPLICATE_IDS" -gt 0 ]; then
    echo "Error: $DUPLICATE_IDS region IDs appear more than once in the hierarchy"
    exit 1
fi

# OSM relation IDs for countries that regressed before the sub-cell fallback
# existed. Only enforce this when the relation is actually present in the
# imported extract; a regional extract is not expected to contain all of them.
KNOWN_COUNTRIES=(
    "21335:Hungary"
    "214885:Croatia"
    "1155955:Liechtenstein"
    "54624:San Marino"
    "1124039:Monaco"
    "16239:Austria"
    "2202162:France"
    "365331:Italy"
    "51477:Germany"
    "148838:United States"
)

if [ ! -f "$COUNTRIES_FILE" ]; then
    echo "Error: $COUNTRIES_FILE not found; cannot validate country roots."
    echo "       Re-run the boundary import with the current importer."
    exit 1
fi

    IMPORTED_COUNTRIES=$(awk -F'\t' '$4=="imported"{c++} END{print c+0}' "$COUNTRIES_FILE")
    PLACEHOLDER_COUNTRIES=$(awk -F'\t' '$4=="placeholder"{c++} END{print c+0}' "$COUNTRIES_FILE")
    SKIPPED_COUNTRIES=$(awk -F'\t' '$4=="skipped"{c++} END{print c+0}' "$COUNTRIES_FILE")
    COUNTRY_ROOTS=$(awk -F'\t' '
        NR==FNR { if ($4=="imported" || $4=="placeholder") country[$1]=1; next }
        ($1 in country) && $2=="" { c++ }
        END { print c+0 }
    ' "$COUNTRIES_FILE" "$HIERARCHY_FILE")
    NON_COUNTRY_LEVEL2_ROOTS=$(awk -F'\t' '
        NR==FNR { if ($4=="imported" || $4=="placeholder") country[$1]=1; next }
        $2=="" && $4==2 && !($1 in country) { c++ }
        END { print c+0 }
    ' "$COUNTRIES_FILE" "$HIERARCHY_FILE")
    CONTINENT_COUNTS=$(awk -F'\t' '
        NR==FNR { if ($4=="imported" || $4=="placeholder") country[$1]=1; next }
        ($1 in country) && $2=="" { c[$9]++ }
        END { for (k in c) printf "%s=%d ", k, c[k] }
    ' "$COUNTRIES_FILE" "$HIERARCHY_FILE")

    echo "  country relations:  $IMPORTED_COUNTRIES imported, $PLACEHOLDER_COUNTRIES placeholder, $SKIPPED_COUNTRIES skipped"
    echo "  country roots:      $COUNTRY_ROOTS"
    [ -n "$CONTINENT_COUNTS" ] && echo "  by continent:       $CONTINENT_COUNTS"
    echo "  non-country level-2 roots: $NON_COUNTRY_LEVEL2_ROOTS"

    if [ "$ROOT_ROWS" -ne "$COUNTRY_ROOTS" ]; then
        echo "Error: hierarchy has $ROOT_ROWS roots but only $COUNTRY_ROOTS country roots; non-country roots are not allowed"
        exit 1
    fi
    if [ "$HIGH_LEVEL_ROOTS" -gt 0 ]; then
        echo "Error: hierarchy still contains $HIGH_LEVEL_ROOTS roots above admin level 2"
        exit 1
    fi
    if [ "$NON_COUNTRY_LEVEL2_ROOTS" -gt 0 ]; then
        echo "Error: hierarchy still contains $NON_COUNTRY_LEVEL2_ROOTS non-country level-2 roots"
        exit 1
    fi

    MISSING_COUNTRY_ROOTS=$(awk -F'\t' '
        NR==FNR { if ($4=="imported" || $4=="placeholder") country[$1]=1; next }
        ($1 in country) && $2=="" { roots[$1]=1 }
        END { for (id in country) if (!(id in roots)) c++; print c+0 }
    ' "$COUNTRIES_FILE" "$HIERARCHY_FILE")

    if [ "$MISSING_COUNTRY_ROOTS" -gt 0 ]; then
        echo "Error: $MISSING_COUNTRY_ROOTS country relations were imported but are not roots in the hierarchy"
        awk -F'\t' '
            NR==FNR { if ($4=="imported" || $4=="placeholder") country[$1]=1; next }
            ($1 in country) && $2=="" { roots[$1]=1 }
            END { for (id in country) if (!(id in roots)) print "  " id }
        ' "$COUNTRIES_FILE" "$HIERARCHY_FILE"
        exit 1
    fi

    if [ "$SKIPPED_COUNTRIES" -gt 0 ]; then
        echo "Warning: $SKIPPED_COUNTRIES country relations could not be imported (incomplete geometry in this extract):"
        awk -F'\t' '$4=="skipped"{printf "  %s %s (%s)\n", $2, $3, $5}' "$COUNTRIES_FILE"
    fi

    MISSING_COUNTRIES=()
    for entry in "${KNOWN_COUNTRIES[@]}"; do
        id="${entry%%:*}"
        name="${entry#*:}"
        status=$(awk -F'\t' -v id="$id" '$1==id{print $4; exit}' "$COUNTRIES_FILE")
        [ -n "$status" ] || continue
        if [ "$status" != "imported" ] && [ "$status" != "placeholder" ]; then
            MISSING_COUNTRIES+=("$name ($id): $status")
        elif ! awk -F'\t' -v id="$id" '$1==id && $2==""{found=1} END{exit !found}' "$HIERARCHY_FILE"; then
            MISSING_COUNTRIES+=("$name ($id): not a root")
        fi
    done

    if [ ${#MISSING_COUNTRIES[@]} -gt 0 ]; then
        echo "Error: known country relations are broken: ${MISSING_COUNTRIES[*]}"
        exit 1
    fi

echo "Hierarchy validation passed."
fi

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
    ZIP_ENTRIES=(h3_to_osm region_metadata region_geometry osm_names.tsv region_hierarchy.tsv)
    [ -f region_countries.tsv ] && ZIP_ENTRIES+=(region_countries.tsv)
    zip -r -q "$ZIP_PATH" "${ZIP_ENTRIES[@]}" -x "**/LOCK"
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