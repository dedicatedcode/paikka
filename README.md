# PAIKKA

PAIKKA is a specialized reverse geocoding service designed to provide high-performance location resolution for [Reitti](https://github.com/dedicatedcode/reitti). The name comes from the Finnish word for "place" and is pronounced [ˈpɑi̯kːɑ].

## Overview

PAIKKA provides fast, scalable reverse geocoding capabilities by processing OpenStreetMap data and serving it through a REST API and web interface. It's designed to handle planet-scale datasets efficiently while maintaining low latency for geocoding requests.

## Why PAIKKA?

Standard geocoding solutions often fall short for specific personal tracking needs. PAIKKA was built to solve these challenges:

- **Meaningful Results:** Optimized for the specific usage patterns and location data used by Reitti.
- **Boundary Intelligence:** Unlike many light geocoders, PAIKKA includes administrative boundaries on nodes when available.
- **Resource Efficient:** A computationally light solution that doesn't require massive infrastructure.
- **Portable Data:** Designed so prepared exports can be easily copied to and served from lightweight machines.

## Relationship to Reitti

[Reitti](https://github.com/dedicatedcode/reitti) is a comprehensive personal location tracking and analysis application that helps you understand your movement patterns and significant places. PAIKKA acts as the dedicated "location engine" for Reitti, converting raw GPS coordinates into human-readable context.

## Limitations

- Boundary data is dependent on availability within the source nodes.
- Optimized for read-heavy serving; updates require re-importing prepared data.
- Focused strictly on reverse geocoding (coordinates to place).

## Features

- **Planet-scale data processing** - Import and process complete OpenStreetMap datasets
- **High-performance geocoding** - Fast and efficient reverse geocoding
- **RESTful API** - Simple HTTP endpoints for geocoding operations
- **Web Dashboard** - Administrative interface for monitoring and statistics
- **Boundary support** - Administrative boundary data for hierarchical location information
- **Health monitoring** - Built-in health check endpoints
- **Secure** - Password-protected admin interface with security features

## API Endpoints

### Public API
- `GET /api/v1/health` - Health check endpoint
- `GET /api/v1/reverse?lat={lat}&lon={lon}[&lang={lang}]` - Reverse geocoding
- `GET /api/v1/geometry/{osmId}` - Retrieve geometry for OSM objects

### Web Interface
- `GET /` - About page with project information
- `GET /login` - Admin login page
- `GET /admin/stats` - Admin dashboard (requires authentication)

## Data Preparation

### Prerequisites

- Java 21 or higher
- Maven 3.6 or higher
- osmium-tool (for filtering OSM data)

### Quick Start

PAIKKA includes helper scripts to simplify data preparation:

1. **Filter OSM data** (recommended to reduce file size):
   ```bash
   ./scripts/filter_osm.sh input.osm.pbf filtered.osm.pbf
   ```

2. **Import the data**:
   ```bash
   ./scripts/import.sh filtered.osm.pbf [data_dir] [memory] [threads]
   ```

### Examples

```bash
# Filter and import a country extract
./scripts/filter_osm.sh germany-latest.osm.pbf germany-filtered.osm.pbf
./scripts/import.sh germany-filtered.osm.pbf ./data 16g

# Import planet data with custom settings
./scripts/filter_osm.sh planet-latest.osm.pbf planet-filtered.osm.pbf
./scripts/import.sh planet-filtered.osm.pbf /opt/paikka/data 32g 8
```

The scripts handle all the technical details including memory management, JVM optimization, and cleanup.

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
paikka.s2-level=14

# Maximum nodes to process in memory
paikka.max-nodes=50000000

# Import configuration
paikka.max-import-threads=10

# API response limits
paikka.max-results=500
paikka.default-results=10

# Base URL for the service
paikka.base-url=http://localhost:8080

# Statistics database path
paikka.stats-db-path=./data/stats.db

# Admin password
paikka.admin.password=your-secure-password
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

### Web Interface

- Visit `http://localhost:8080/` for the about page
- Visit `http://localhost:8080/admin/stats` to access the admin dashboard (login required)

## Integration with Reitti

To use PAIKKA with Reitti, configure the geocoding service in Reitti's settings:

1. Start PAIKKA service on your server
2. Set the admin password in your configuration
3. In Reitti, go to Settings → Geocoding
4. Add a new geocoding service:
   - **Name**: PAIKKA
   - **URL**: `http://your-paikka-server:8080/api/v1/reverse?lat={lat}&lon={lon}`
   - **Priority**: Set as primary service

## Security

PAIKKA is secure and includes password-protected admin interface with proper security features for production deployment.

## Getting Support

There are multiple ways of getting support:

- Create a [new issue](https://github.com/dedicatedcode/reitti/issues/new/choose) in the main Reitti repository
- Tag me on [Lemmy](https://discuss.tchncs.de/u/danielgraf)
- Join **#reitti** on [irc.dedicatedcode.com](https://irc.dedicatedcode.com)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request to the main [Reitti repository](https://github.com/dedicatedcode/reitti).

## Technology Stack

- **Java 21** - Modern Java runtime with performance improvements
- **Spring Boot** - Application framework with embedded web server
- **RocksDB** - High-performance embedded database for spatial data
- **FlatBuffers** - Efficient serialization for geocoding data
- **S2 Geometry** - Spatial indexing and geometric operations
- **Leaflet** - Interactive maps for the web interface
- **Thymeleaf** - Server-side templating for HTML pages

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## About

PAIKKA is developed as part of the Reitti ecosystem by [dedicatedcode](https://github.com/dedicatedcode). It provides the geocoding infrastructure that powers location-based features in Reitti.
