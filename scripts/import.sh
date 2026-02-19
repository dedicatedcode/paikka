#!/bin/bash

# PAIKKA Import Script
# Runs PAIKKA in import mode with required JVM flags

# Usage function
usage() {
    echo "Usage: $0 [jar_file] <pbf_file> [data_dir] [memory] [threads]"
    echo ""
    echo "Imports OSM PBF data into PAIKKA format"
    echo ""
    echo "Arguments:"
    echo "  jar_file    Path to the PAIKKA jar file (optional, auto-detected if not provided)"
    echo "  pbf_file    Path to the OSM PBF file to import"
    echo "  data_dir    Directory to store processed data (default: ./data)"
    echo "  memory      JVM heap size (default: 16g)"
    echo "  threads     Maximum number of import threads (default: half of CPU cores)"
    echo ""
    echo "Examples:"
    echo "  $0 planet-latest.osm.pbf"
    echo "  $0 /app/app.jar europe-latest.osm.pbf /opt/paikka/data"
    echo "  $0 germany-latest.osm.pbf ./data 16g"
    echo "  $0 germany-latest.osm.pbf ./data 16g 4"
    echo ""
    echo "Requirements:"
    echo "  - Java 21 or higher"
    echo "  - PAIKKA jar file in target/ directory or provided as parameter"
    echo "  - Sufficient RAM (recommended: 32GB+ for planet)"
    exit 1
}

# Check if at least one argument provided
if [ $# -lt 1 ]; then
    echo "Error: At least one argument required"
    echo ""
    usage
fi

# Check if first argument is a jar file
JAR_FILE=""
if [[ "$1" == *.jar ]]; then
    JAR_FILE="$1"
    shift
fi

# Now parse remaining arguments
PBF_FILE="$1"
DATA_DIR="${2:-./data}"
MEMORY="${3:-16g}"
THREADS="${4:-}"

# Check if PBF file argument is provided
if [ -z "$PBF_FILE" ]; then
    echo "Error: PBF file argument required"
    echo ""
    usage
fi

# Check if PBF file exists
if [ ! -f "$PBF_FILE" ]; then
    echo "Error: PBF file '$PBF_FILE' does not exist"
    exit 1
fi

# Find PAIKKA jar file if not provided
if [ -z "$JAR_FILE" ]; then
    JAR_FILE=$(find target -name "paikka-*.jar" -not -name "*-sources.jar" | head -1)
    
    if [ -z "$JAR_FILE" ]; then
        echo "Error: PAIKKA jar file not found in target/ directory"
        echo "Please run 'mvn clean package' first or provide jar file path as first argument"
        exit 1
    fi
fi

# Verify jar file exists
if [ ! -f "$JAR_FILE" ]; then
    echo "Error: JAR file '$JAR_FILE' does not exist"
    exit 1
fi

echo "Starting PAIKKA import..."
echo "PBF file:    $PBF_FILE"
echo "Data dir:    $DATA_DIR"
echo "Memory:      $MEMORY"
echo "JAR file:    $JAR_FILE"
if [ -n "$THREADS" ]; then
    echo "Threads:     $THREADS"
fi
echo ""

# Check available system memory
AVAILABLE_MEM_KB=$(grep MemAvailable /proc/meminfo | awk '{print $2}')
AVAILABLE_MEM_GB=$((AVAILABLE_MEM_KB / 1024 / 1024))

# Extract numeric value from MEMORY parameter
REQUESTED_MEM_GB=$(echo "$MEMORY" | sed 's/[^0-9]//g')

echo "System memory:   ${AVAILABLE_MEM_GB}GB available"
echo "Requested heap:  $MEMORY"

if [ "$REQUESTED_MEM_GB" -gt "$AVAILABLE_MEM_GB" ]; then
    echo ""
    echo "⚠️  WARNING: Requested heap size ($MEMORY) exceeds available memory (${AVAILABLE_MEM_GB}GB)"
    echo "   This may cause the process to be killed by the OOM killer."
    echo "   Consider reducing heap size or adding more RAM."
    echo ""
    read -p "Continue anyway? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Import cancelled."
        exit 1
    fi
fi

# Build JVM arguments with memory management optimizations
JVM_ARGS="-Xmx$MEMORY -Xms$MEMORY"
JVM_ARGS="$JVM_ARGS -XX:+UseG1GC"
JVM_ARGS="$JVM_ARGS -XX:MaxGCPauseMillis=200"
JVM_ARGS="$JVM_ARGS -XX:+UnlockExperimentalVMOptions"
JVM_ARGS="$JVM_ARGS -XX:+UseTransparentHugePages"
JVM_ARGS="$JVM_ARGS -XX:+ExitOnOutOfMemoryError"
JVM_ARGS="$JVM_ARGS --add-exports=java.base/jdk.internal.ref=ALL-UNNAMED"
JVM_ARGS="$JVM_ARGS --add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
JVM_ARGS="$JVM_ARGS --add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED"
JVM_ARGS="$JVM_ARGS --add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED"
JVM_ARGS="$JVM_ARGS --add-opens=jdk.compiler/com.sun.tools.javac=ALL-UNNAMED"
JVM_ARGS="$JVM_ARGS --add-opens=java.base/java.lang=ALL-UNNAMED"
JVM_ARGS="$JVM_ARGS --add-opens=java.base/java.lang.reflect=ALL-UNNAMED"
JVM_ARGS="$JVM_ARGS --add-opens=java.base/java.io=ALL-UNNAMED"
JVM_ARGS="$JVM_ARGS --add-opens=java.base/java.util=ALL-UNNAMED"
JVM_ARGS="$JVM_ARGS --enable-native-access=ALL-UNNAMED"

# Add thread configuration if specified
if [ -n "$THREADS" ]; then
    JVM_ARGS="$JVM_ARGS -Dpaikka.max-import-threads=$THREADS"
fi

# Run PAIKKA import with required JVM flags
java $JVM_ARGS \
  -jar "$JAR_FILE" \
  --import \
  --pbf-file "$PBF_FILE" \
  --data-dir "$DATA_DIR"

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo ""
    echo "✓ Import completed successfully"
elif [ $EXIT_CODE -eq 134 ]; then
    echo ""
    echo "✗ Import failed: Process was killed (likely out of memory)"
    echo "💡 Try reducing heap size or adding more RAM"
    echo "   Current heap: $MEMORY, Available: ${AVAILABLE_MEM_GB}GB"
    exit 1
else
    echo ""
    echo "✗ Import failed with exit code: $EXIT_CODE"
    exit 1
fi

if [ $EXIT_CODE -eq 0 ]; then
    echo ""
    echo "✓ Import completed successfully"
    echo "✓ Data directory: $DATA_DIR"
    
    # Clean up temporary files to save disk space
    echo ""
    echo "🧹 Cleaning up temporary files..."
    TEMP_DIR="$DATA_DIR/node_cache"
    if [ -d "$TEMP_DIR" ]; then
        echo "  Removing temporary directory: $TEMP_DIR"
        rm -rf "$TEMP_DIR"
        echo "  ✓ Temporary files cleaned up"
    else
        echo "  No temporary directory found to clean up"
    fi
    
    echo ""
    echo "✓ You can now start PAIKKA in server mode:"
    echo "  java -Xmx8g -jar $JAR_FILE --data-dir=$DATA_DIR"
    echo ""
    echo "💡 To save more disk space, you can also delete:"
    echo "  - The PBF file: $PBF_FILE"
else
    echo ""
    echo "✗ Import failed"
    exit 1
fi
