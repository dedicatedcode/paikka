#!/bin/bash

# Project Paikka - Lite PBF Filter
# Filters OSM PBF files to keep only POIs and Administrative Boundaries

# Usage function
usage() {
    echo "Usage: $0 <input_file> <output_file>"
    echo ""
    echo "Filters an OSM PBF file to keep only data relevant for PAIKKA:"
    echo "  - Points of Interest (amenity, shop, tourism, leisure, etc.)"
    echo "  - Administrative boundaries"
    echo ""
    echo "Arguments:"
    echo "  input_file   Path to the input OSM PBF file"
    echo "  output_file  Path for the filtered output PBF file"
    echo ""
    echo "Examples:"
    echo "  $0 planet-latest.osm.pbf planet-filtered.osm.pbf"
    echo "  $0 europe-latest.osm.pbf europe-paikka.osm.pbf"
    echo ""
    echo "Requirements:"
    echo "  - osmium-tool must be installed"
    echo "  - Sufficient disk space for output file"
    exit 1
}

# Check if correct number of arguments provided
if [ $# -ne 2 ]; then
    echo "Error: Incorrect number of arguments"
    echo ""
    usage
fi

INPUT_FILE="$1"
OUTPUT_FILE="$2"

# Check if input file exists
if [ ! -f "$INPUT_FILE" ]; then
    echo "Error: Input file '$INPUT_FILE' does not exist"
    exit 1
fi

# Check if osmium is available
if ! command -v osmium &> /dev/null; then
    echo "Error: osmium-tool is not installed"
    echo "Install with: sudo apt-get install osmium-tool (Ubuntu/Debian)"
    echo "Or: brew install osmium-tool (macOS)"
    exit 1
fi

echo "Starting OSM PBF filtering for PAIKKA..."
echo "Input file:  $INPUT_FILE"
echo "Output file: $OUTPUT_FILE"
echo ""
osmium tags-filter "$INPUT_FILE" \
  nwr/amenity!=bench,drinking_water,waste_basket,bicycle_parking,vending_machine,parking_entrance,fire_hydrant,recycling \
  nwr/emergency!=fire_hydrant,defibrillator \
  nw/shop \
  nw/tourism \
  nw/leisure \
  nw/office \
  nw/craft \
  nw/place \
  nw/historic \
  nw/healthcare \
  nw/public_transport \
  nw/railway=station \
  nw/aeroway \
  w/building=yes,commercial,retail,industrial,office,apartments,residential \
  r/boundary=administrative \
  r/type=multipolygon \
  -o "$OUTPUT_FILE" --overwrite

if [ $? -eq 0 ]; then
    echo ""
    echo "✓ Filter complete: $OUTPUT_FILE created"
    echo "✓ Input file size:  $(du -h "$INPUT_FILE" | cut -f1)"
    echo "✓ Output file size: $(du -h "$OUTPUT_FILE" | cut -f1)"
    
    # Calculate size reduction
    INPUT_SIZE=$(stat -c%s "$INPUT_FILE" 2>/dev/null || stat -f%z "$INPUT_FILE" 2>/dev/null)
    OUTPUT_SIZE=$(stat -c%s "$OUTPUT_FILE" 2>/dev/null || stat -f%z "$OUTPUT_FILE" 2>/dev/null)
    
    if [ -n "$INPUT_SIZE" ] && [ -n "$OUTPUT_SIZE" ] && [ "$INPUT_SIZE" -gt 0 ]; then
        REDUCTION=$(( (INPUT_SIZE - OUTPUT_SIZE) * 100 / INPUT_SIZE ))
        echo "✓ Size reduction:   ${REDUCTION}%"
    fi
else
    echo "Error: Filtering failed"
    exit 1
fi
