# PAIKKA

PAIKKA is a high-performance reverse geocoding service that serves as the geocoding backend for [Reitti](https://github.com/dedicatedcode/reitti), a comprehensive personal location tracking and analysis application. The name "PAIKKA" comes from Finnish, meaning "place" or "location".

## Overview

PAIKKA provides fast, scalable reverse geocoding capabilities by processing OpenStreetMap data and serving it through a REST API. It's designed to handle planet-scale datasets efficiently while maintaining low latency for geocoding requests.

## Relationship to Reitti

PAIKKA is the dedicated reverse geocoding component of the Reitti ecosystem:

- **[Reitti](https://github.com/dedicatedcode/reitti)** - The main application for personal location tracking and analysis
- **PAIKKA** - The reverse geocoding service that converts coordinates to human-readable addresses

While Reitti can use external geocoding services like Nominatim or Photon, PAIKKA provides a self-hosted, optimized solution specifically designed for Reitti's needs.

## Features

- **Planet-scale data processing** - Import and process complete OpenStreetMap datasets
- **High-performance geocoding** - Optimized for low-latency reverse geocoding requests
- **RESTful API** - Simple HTTP endpoints for geocoding operations
- **Spatial indexing** - Efficient S2-based spatial indexing for fast lookups
- **Boundary support** - Administrative boundary data for hierarchical location information
- **Geometry simplification** - Optimized geometry storage and retrieval
- **Health monitoring** - Built-in health check endpoints

## API Endpoints

- `GET /api/v1/health` - Health check endpoint
- `GET /api/v1/reverse?lat={lat}&lon={lon}[&lang={lang}]` - Reverse geocoding
- `GET /api/v1/geometry/{osmId}` - Retrieve geometry for OSM objects

## Data Preparation

### Prerequisites

- Java 21 or higher
- Maven 3.6 or higher
- At least 32GB RAM for planet-scale imports
- 500GB+ available disk space for planet data
- SSD storage recommended for optimal performance

### Downloading Planet Data

1. **Download the latest planet PBF file:**
   ```bash
   wget https://planet.openstreetmap.org/pbf/planet-latest.osm.pbf
   ```

2. **Alternative regional extracts** (for smaller datasets):
   ```bash
   # Europe extract (~30GB)
   wget https://download.geofabrik.de/europe-latest.osm.pbf
   
   # Country-specific extract (e.g., Germany ~3GB)
   wget https://download.geofabrik.de/europe/germany-latest.osm.pbf
   ```

### Filtering Planet Data (Recommended)

Since PAIKKA only needs specific OSM data (administrative boundaries and POIs), you can significantly reduce file size and import time by filtering the planet file. This can reduce a 70GB planet file to ~10-15GB.

#### Installing osmium-tool

```bash
# Ubuntu/Debian
sudo apt-get install osmium-tool

# macOS
brew install osmium-tool

# Or build from source: https://osmcode.org/osmium-tool/
```

#### Size Comparison After Filtering

| Original Dataset | Original Size | Filtered Size | Reduction |
|------------------|---------------|---------------|-----------|
| Planet           | ~70GB         | ~10-15GB      | ~80%      |
| Europe           | ~30GB         | ~4-6GB        | ~80%      |
| Germany          | ~4.4GB        | ~980MB        | ~78%      |
| Monaco           | 657K          | 223K          | ~66%      |

**Benefits of filtering:**
- Dramatically reduced import times (3-5x faster)
- Lower storage requirements
- Faster subsequent processing
- Reduced memory usage during import

### Storage Requirements

| Dataset                 | PBF Size | During Import | Processed Size | RAM Required | Import Time |
|-------------------------|----------|---------------|----------------|--------------|-------------|
| Monaco                  | 223K     | 1.3M          | ~920K          | -            | 2s          |
| Country (e.g., Germany) | ~4.4GB   | 1.2G          | ~75MB          | 8GB          | 2-4 hours   |
| Continent (Europe)      | ~30GB    |               | ~150GB         | 16GB         | 12-24 hours |
| Planet                  | ~70GB    |               | ~350GB         | 32GB+        | 2-5 days    |

## Data Import

### Building the Application

```bash
mvn clean package
```

### Running the Import

```bash
java -Xmx32g -jar target/paikka-*.jar \
  --import \
  --pbf-file=/path/to/planet-latest.osm.pbf \
  --data-dir=/path/to/data/directory
```

### Post-Import Cleanup

After a successful import, you can safely delete temporary files to reclaim disk space. The import process creates several large temporary files that are no longer needed once the data is processed into RocksDB format.

#### What Gets Deleted (The "Clean Sweep")

| File/Folder | Estimated Size | Why it's safe to delete |
|-------------|----------------|-------------------------|
| `planet.osm.pbf` | ~70 GB | The original raw data. You've already extracted what you need. |
| `filtered.osm.pbf` | ~5-15 GB | Once processing is complete, all relevant data is in RocksDB. |
| `temp/node_cache.dat` | 100+ GB | Temporary "phonebook" for coordinate lookups during import. |
| `temp/` directory | Variable | All intermediate files generated during processing. |

#### Manual Cleanup

```bash
# After successful import, clean up temporary files
rm -rf /path/to/data/temp/
rm planet-latest.osm.pbf  # Original PBF file
rm filtered.osm.pbf       # Filtered PBF file (if used)
```

This cleanup can reduce storage usage from 200GB+ during import to ~20GB for the final production data.

### Import Parameters

- `--import` - Enable import mode
- `--pbf-file` - Path to the OSM PBF file
- `--data-dir` - Directory where processed data will be stored (default: `./data`)

### Expected Import Times

Import times vary significantly based on hardware and dataset size:

- **SSD + 32GB RAM**: Planet import ~2-3 days
- **HDD + 16GB RAM**: Planet import ~4-5 days
- **Country extract**: 2-4 hours regardless of hardware

### Monitoring Import Progress

The import process logs progress regularly:

```
INFO  - Processing nodes: 1,000,000 / 8,500,000,000 (0.01%)
INFO  - Processing ways: 500,000 / 900,000,000 (0.06%)
INFO  - Processing relations: 10,000 / 10,000,000 (0.10%)
INFO  - Building spatial index...
INFO  - Import completed successfully
```

## Distributed Data Preparation

For production deployments, you can prepare data on a powerful machine and transfer it to the production server.

### Preparing Data on Build Machine

1. **Run import on powerful machine:**
   ```bash
   java -Xmx64g -jar paikka-*.jar \
     --import \
     --pbf-file=planet-latest.osm.pbf \
     --data-dir=/build/data
   ```

2. **Create compressed archive:**
   ```bash
   tar -czf paikka-data.tar.gz -C /build data/
   ```

### Transferring to Production

1. **Transfer data archive:**
   ```bash
   scp paikka-data.tar.gz production-server:/opt/paikka/
   ```

2. **Extract on production server:**
   ```bash
   cd /opt/paikka
   tar -xzf paikka-data.tar.gz
   ```

3. **Start PAIKKA service:**
   ```bash
   java -Xmx8g -jar paikka-*.jar --data-dir=/opt/paikka/data
   ```

### Hot Data Replacement

To update data without downtime:

1. **Prepare new data in temporary directory:**
   ```bash
   java -Xmx32g -jar paikka-*.jar \
     --import \
     --pbf-file=planet-latest.osm.pbf \
     --data-dir=/opt/paikka/data-new
   ```

2. **Stop PAIKKA service:**
   ```bash
   systemctl stop paikka
   ```

3. **Replace data directory:**
   ```bash
   mv /opt/paikka/data /opt/paikka/data-old
   mv /opt/paikka/data-new /opt/paikka/data
   ```

4. **Start PAIKKA service:**
   ```bash
   systemctl start paikka
   ```

5. **Verify service health:**
   ```bash
   curl http://localhost:8080/api/v1/health
   ```

6. **Clean up old data:**
   ```bash
   rm -rf /opt/paikka/data-old
   ```

## Running the Service

### Development Mode

```bash
mvn spring-boot:run
```

### Production Mode

```bash
java -Xmx8g -jar target/paikka-*.jar --data-dir=/path/to/data
```

### Configuration

Configure the service using environment variables or application properties:

```properties
# Server configuration
server.port=8080

# Data directory
paikka.data-dir=/opt/paikka/data

# S2 spatial indexing level (10-15, higher = more precise but larger index)
paikka.s2-level=12

# Maximum nodes to process in memory
paikka.max-nodes=10000000
```

### Sample Requests

```bash
# Health check
curl 'http://localhost:8080/api/v1/health'

# Reverse geocoding (Helsinki, Finland)
curl 'http://localhost:8080/api/v1/reverse?lat=60.1699&lon=24.9384'

# Reverse geocoding with language preference
curl 'http://localhost:8080/api/v1/reverse?lat=60.1699&lon=24.9384&lang=fi'

# Get geometry for OSM object
curl 'http://localhost:8080/api/v1/geometry/12345'
```

## Integration with Reitti

To use PAIKKA with Reitti, configure the geocoding service in Reitti's settings:

1. Start PAIKKA service on your server
2. In Reitti, go to Settings → Geocoding
3. Add a new geocoding service:
   - **Name**: PAIKKA
   - **URL**: `http://your-paikka-server:8080/api/v1/reverse?lat={lat}&lon={lon}`
   - **Priority**: Set as primary service

## Performance Tuning

### JVM Settings

For optimal performance, tune JVM settings based on your hardware:

```bash
# For 16GB RAM server
java -Xmx12g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -jar paikka-*.jar

# For 32GB RAM server  
java -Xmx24g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -jar paikka-*.jar
```

### Storage Optimization

- Use SSD storage for data directory
- Ensure sufficient disk space (3-5x the PBF file size)
- Consider using separate disks for data and logs

## Getting Support

There are multiple ways of getting support:

- Create a [new issue](https://github.com/dedicatedcode/reitti/issues/new/choose) in the main Reitti repository
- Tag me on [Lemmy](https://discuss.tchncs.de/u/danielgraf)
- Join **#reitti** on [irc.dedicatedcode.com](https://irc.dedicatedcode.com)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request to the main [Reitti repository](https://github.com/dedicatedcode/reitti).

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## About

PAIKKA is developed as part of the Reitti ecosystem by [dedicatedcode](https://github.com/dedicatedcode). It provides the geocoding infrastructure that powers location-based features in Reitti.
