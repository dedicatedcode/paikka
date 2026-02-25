![](.github/banner.png)

PAIKKA is a specialized reverse geocoding service designed to provide high-performance location resolution for [Reitti](https://github.com/dedicatedcode/reitti). The name comes from the Finnish word for "place" and is pronounced [ˈpɑi̯kːɑ].

## Overview

PAIKKA provides fast, scalable reverse geocoding capabilities by processing OpenStreetMap data and serving it through a REST API and web interface. It's designed to handle planet-scale datasets efficiently while maintaining low latency for geocoding requests.

## Why PAIKKA?

Standard geocoding solutions often fall short for specific personal tracking needs. PAIKKA was built to solve these challenges:

- **Meaningful Results:** Optimized for the specific usage patterns and location data needed for Reitti.
- **Boundary Intelligence:** Unlike many light geocoders, PAIKKA includes administrative boundaries on nodes when available.
- **Resource Efficient:** A computationally light solution that doesn't require massive infrastructure.
- **Portable Data:** Designed so prepared exports can be easily copied to and served from lightweight machines.

## Relationship to Reitti

[Reitti](https://github.com/dedicatedcode/reitti) is a comprehensive personal location tracking and analysis application that helps you understand your movement patterns and significant places. PAIKKA acts as the dedicated "location engine" for Reitti, converting raw GPS coordinates into human-readable context.

## Limitations

- Boundary data is dependent on availability within the source nodes.
- Optimized for read-heavy serving; updates require re-importing prepared data.
- Focused strictly on reverse geocoding (coordinates to place).
- **Highly Opinionated POI Selection:** PAIKKA is deliberately selective about which Points of Interest it imports. It only includes POIs with the following OSM tags:
  - `amenity` (e.g., restaurant, school, hospital, fuel, atm)
  - `shop` (e.g., supermarket, clothes, bakery)
  - `tourism` (e.g., hotel, museum, attraction)
  - `leisure` (e.g., fitness_centre, playground, park)
  - `office` (e.g., company, government, insurance)
  - `craft` (e.g., carpenter, electrician, painter)
  - `healthcare` (e.g., hospital, pharmacy, clinic)
  - `emergency` (e.g., ambulance_station, fire_hydrant)
  - `historic` (e.g., monument, archaeological_site)
  - `natural` (e.g., peak, cave, waterfall)
  - `man_made` (e.g., tower, pier, windmill)
  - `place` (e.g., city, town, village, neighborhood)
  - `sport` (e.g., tennis, swimming, fitness)
  - `public_transport` (e.g., stop_position, station)
  - `railway` (stations only)
  - `aeroway` (e.g., aerodrome, helipad)
  - `building` (commercial, retail, industrial, office – but not residential or generic "yes")
  
  Many common OSM features are explicitly excluded, such as:
  - Natural features like trees and grass
  - Man-made structures like electric-poles, trash cans
  - Amenities like benches, drinking water, and picnic tables
  - Swimming pools and fountains
  
  If you need comprehensive POI coverage including all OSM features, **Nominatim** would be a better fit for your use case. While it would be technically possible to import the complete unfiltered dataset, this was never the intended design and has not been tested.

## Features

- **High performance geocoding** – Fast and efficient reverse geocoding
- **RESTful API** – Simple HTTP endpoints for geocoding operations
- **Web Dashboard** – Administrative interface for monitoring and statistics
- **Boundary support** – Administrative boundary data for hierarchical location information
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

- Java 25 or higher
- osmium-tool (for filtering OSM data)

### Obtaining OSM Data

Before you can import data into PAIKKA, you need to obtain an OSM PBF file. Here are the main sources:

#### Geofabrik (Recommended for regional extracts)

[Geofabrik](https://download.geofabrik.de/) provides regularly updated OSM extracts for various regions. Their data is already clipped to geographic boundaries, making it ideal for country or regional imports.

**Popular downloads:**
- [Europe](https://download.geofabrik.de/europe.html) – Individual countries and regions
- [North America](https://download.geofabrik.de/north-america.html) – Countries and states
- [Asia](https://download.geofabrik.de/asia.html) – Countries and regions
- [Planet file](https://download.geofabrik.de/planet.html) – Complete global dataset

**Example:** To download Germany:
```bash
wget https://download.geofabrik.de/europe/germany-latest.osm.pbf
```

#### Planet file (Full global dataset)

For planet-scale imports, download the full planet file:
- [Planet PBF](https://planet.openstreetmap.org/pbf/) – Updated weekly
- [Planet Mirrors](https://wiki.openstreetmap.org/wiki/Planet.osm#Planet.osm_mirrors) – Various download mirrors

**Warning:** The planet file is extremely large (tens of gigabytes). Processing it requires significant time and resources. For most use cases, a regional extract from Geofabrik is sufficient.

#### OSM.org (Direct exports)

The [OpenStreetMap website](https://www.openstreetmap.org/export/) allows you to export custom areas using their "Export" feature. This is useful if you need a specific region that isn't available as a pre-made extract.

### Sizing Considerations


| Dataset    | original | filtered... | time taken | reduction | during import | imported | time taken | reduction |
|------------|----------|-------------|------------|-----------|---------------|----------|------------|-----------|
| Planet     | 86 GB    | 33 GB       | 40 min     | ~60%      | ~ 31 GB       | 8.15 GB  | ~ 16 h     | ~90%      |
| Germany    | 4.7 GB   | 1.7 GB      | 2 min      | ~61%      | ~ 3 GB        | 875MB    | ~ 50 min   | ~80%      |
| Netherland | 1.4 GB   | 390 MB      | 1 min      | ~61%      | ~ 1 GB        | 366 MB   | ~ 2 min    | ~75%      |

### Quick Start

PAIKKA includes helper scripts to simplify data preparation:

1. **Filter OSM data** (recommended to reduce file size):
   ```bash
   ./scripts/filter_osm.sh input.osm.pbf filtered.osm.pbf
   ```

2. **Import the data**:
   ```bash
   ./scripts/import.sh filtered.osm.pbf
   ```

### Running with Docker

Alternatively, you can use Docker to prepare and import data. The container includes the required scripts and osmium-tool.

#### Prerequisites

- Docker installed
- A PBF file with OSM data

#### Prepare the data (filter OSM data)

Run the `prepare` script to filter the OSM data. Mount a local directory containing your input PBF file and specify the output filename:

```bash
docker run -v /path/to/your/data:/data dedicatedcode/paikka prepare input.osm.pbf filtered.osm.pbf
```

- Replace `/path/to/your/data` with the absolute path to your local directory
- `input.osm.pbf` is your source OSM data file and should be located inside the mounted directory
- `filtered.osm.pbf` is the output filtered file that will be created in your mounted directory

#### Import the data

Run the `import` script to import the filtered data into the data directory:

```bash
docker run -ti -v /path/to/your/data:/data dedicatedcode/paikka:latest import filtered.osm.pbf
```

- `filtered.osm.pbf` is the filtered PBF file from the previous step
- `/data` is the target directory inside the container (mounted from your local directory)

After import completes, your data directory will contain the processed data files ready for the service.

#### Running the service with Docker

To run the PAIKKA service itself:

```bash
docker run -d \
  -v /path/to/your/data:/data \
  -p 8080:8080 \
  -e ADMIN_PASSWORD=your-secure-password \
  paikka
```

The data directory is mounted at `/data` inside the container, and the service runs on port 8080.

#### Docker Environment Variables

When running with Docker, you can configure the service using environment variables:

| Environment Variable | Purpose | Default Value |
|---------------------|---------|---------------|
| `DATA_DIR` | Directory where processed data is stored | `./data` |
| `MAX_IMPORT_THREADS` | Maximum number of threads for data import | `10` |
| `MAX_RESULTS` | Maximum number of results returned by API | `500` |
| `DEFAULT_RESULTS` | Default number of results when not specified | `10` |
| `BASE_URL` | Base URL for the service (used in responses) | `http://localhost:8080` |
| `STATS_DB_PATH` | Path to the statistics database | `./data/stats.db` |
| `ADMIN_PASSWORD` | Password for admin interface access | _(empty)_ |
| `APP_LOG_LEVEL` | Log level for the application | `INFO` |

Example with custom configuration:

```bash
docker run -d \
  -v /path/to/your/data:/data \
  -p 8080:8080 \
  -e DATA_DIR=/data \
  -e MAX_RESULTS=1000 \
  -e DEFAULT_RESULTS=20 \
  -e ADMIN_PASSWORD=your-secure-password \
  -e APP_LOG_LEVEL=DEBUG \
  paikka
```

### Examples

```bash
# Filter and import a country extract
./scripts/filter_osm.sh germany-latest.osm.pbf germany-filtered.osm.pbf
./scripts/import.sh germany-filtered.osm.pbf  --data-dir ./data --memory 16g

# Import planet data with custom settings
./scripts/filter_osm.sh planet-latest.osm.pbf planet-filtered.osm.pbf
./scripts/import.sh planet-filtered.osm.pbf --data-dir /opt/paikka/data --memory 32g --threads 8
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

TBA

## Getting Support

There are multiple ways of getting support:

- Create a [new issue](https://github.com/dedicatedcode/paikka/issues/new/choose) in the repository
- Tag me on [Lemmy](https://discuss.tchncs.de/u/danielgraf)
- Join **#reitti** on [irc.dedicatedcode.com](https://irc.dedicatedcode.com)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request to [repository](https://github.com/dedicatedcode/paikka).

## Technology Stack

- **Java 25** – Modern Java runtime with performance improvements
- **Spring Boot** – Application framework with embedded web server
- **RocksDB** – High-performance embedded database for spatial data
- **FlatBuffers** – Efficient serialization for geocoding data
- **S2 Geometry** – Spatial indexing and geometric operations
- **Leaflet** – Interactive maps for the web interface
- **Thymeleaf** – Server-side templating for HTML pages

## License

This project is licensed under the MIT License – see the [LICENSE](LICENSE) file for details.

## About

PAIKKA is developed as part of the Reitti ecosystem by [dedicatedcode](https://github.com/dedicatedcode). It provides the geocoding infrastructure that powers location-based features in Reitti.
