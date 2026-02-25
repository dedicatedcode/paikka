/*
 *  This file is part of paikka.
 *
 *  Paikka is free software: you can redistribute it and/or
 *  modify it under the terms of the GNU Affero General Public License
 *  as published by the Free Software Foundation, either version 3 or
 *  any later version.
 *
 *  Paikka is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied
 *  warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *  See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License
 * along with Paikka. If not, see <https://www.gnu.org/licenses/>.
 */

package com.dedicatedcode.paikka.service;

import com.dedicatedcode.paikka.config.PaikkaConfiguration;
import com.dedicatedcode.paikka.dto.GeoJsonGeometry;
import com.dedicatedcode.paikka.dto.POIResponse;
import com.dedicatedcode.paikka.exception.POINotFoundException;
import com.dedicatedcode.paikka.flatbuffers.*;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKBReader;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Service for reverse geocoding operations - finding POIs near coordinates.
 */
@Service
@ConditionalOnProperty(name = "paikka.import-mode", havingValue = "false", matchIfMissing = true)
public class ReverseGeocodingService {
    
    private static final Logger logger = LoggerFactory.getLogger(ReverseGeocodingService.class);
    private static final double SEARCH_RADIUS_KM = 10.0;
    private static final int MAX_SEARCH_RINGS = 3;

    private final PaikkaConfiguration config;
    private final S2Helper s2Helper;
    private RocksDB shardsDb;
    
    public ReverseGeocodingService(PaikkaConfiguration config, S2Helper s2Helper) {
        this.config = config;
        this.s2Helper = s2Helper;
        initializeRocksDB();
    }
    
    private void initializeRocksDB() {
        if (shardsDb != null) {
            return;
        }
        
        try {
            RocksDB.loadLibrary();
            Path shardsDbPath = Paths.get(config.getDataDir(), "poi_shards");
            
            // Check if the database directory exists
            if (!shardsDbPath.toFile().exists()) {
                logger.warn("POI shards database not found at: {}", shardsDbPath);
                return;
            }
            
            Options options = new Options().setCreateIfMissing(false);
            this.shardsDb = RocksDB.openReadOnly(options, shardsDbPath.toString());
            logger.info("Successfully initialized RocksDB for POI shards");
        } catch (Exception e) {
            logger.warn("Failed to initialize RocksDB for POI shards: {}", e.getMessage());
            this.shardsDb = null;
        }
    }
    
    public synchronized void reloadDatabase() {
        logger.info("Reloading POI shards database...");
        
        if (shardsDb != null) {
            try {
                shardsDb.close();
                logger.info("Closed existing POI shards database connection");
            } catch (Exception e) {
                logger.warn("Error closing existing POI shards database: {}", e.getMessage());
            }
            shardsDb = null;
        }
        
        initializeRocksDB();
        
        if (shardsDb != null) {
            logger.info("POI shards database reloaded successfully");
        } else {
            logger.warn("POI shards database reload completed but database is not available");
        }
    }
    
    /**
     * Find the nearest POI to the given coordinates.
     */
    public POIResponse findNearestPOI(double lat, double lon, String lang) {
        List<POIResponse> results = findNearbyPOIs(lat, lon, lang, 1);
        if (results.isEmpty()) {
            throw new POINotFoundException(String.format("No POI found within search radius for coordinates lat=%.6f, lon=%.6f", lat, lon));
        }
        return results.getFirst();
    }
    
    /**
     * Find nearby POIs to the given coordinates.
     * 
     * @param lat Latitude in degrees
     * @param lon Longitude in degrees
     * @param lang Language code for localized names
     * @param limit Maximum number of results to return
     * @return List of POI information with hierarchy
     */
    public List<POIResponse> findNearbyPOIs(double lat, double lon, String lang, int limit) {
        logger.debug("Finding nearby POIs for lat={}, lon={}, lang={}, limit={}", lat, lon, lang, limit);
        
        try {
            if (shardsDb == null) {
                throw new RuntimeException("RocksDB not available for reverse geocoding - database not initialized");
            }
            
            // Start with center shard
            long centerShardId = s2Helper.getShardId(lat, lon);
            Set<Long> searchedShards = new HashSet<>();
            
            // Load POIs from center shard first
            List<POIData> allPOIs = new ArrayList<>(loadPOIsFromShard(centerShardId));
            searchedShards.add(centerShardId);
            
            // Get expanding rings of neighbor shards
            Map<Integer, Set<Long>> neighborRings = s2Helper.getExpandingNeighborRings(centerShardId, MAX_SEARCH_RINGS);
            
            // Search ring by ring until we have enough results or hit max radius
            for (int ring = 1; ring <= MAX_SEARCH_RINGS; ring++) {
                Set<Long> ringShards = neighborRings.get(ring);
                if (ringShards == null || ringShards.isEmpty()) {
                    logger.debug("No more shards in ring {}, stopping expansion", ring);
                    break;
                }
                
                // Load POIs from this ring
                List<POIData> ringPOIs = new ArrayList<>();
                for (long shardId : ringShards) {
                    if (!searchedShards.contains(shardId)) {
                        ringPOIs.addAll(loadPOIsFromShard(shardId));
                        searchedShards.add(shardId);
                    }
                }
                
                allPOIs.addAll(ringPOIs);
                logger.debug("Ring {}: searched {} shards, found {} POIs, total POIs: {}", 
                    ring, ringShards.size(), ringPOIs.size(), allPOIs.size());
                
                // Check if we have enough candidates within radius
                List<POIData> candidatesWithinRadius = findClosestPOIs(allPOIs, lat, lon, limit * 2); // Get more candidates than needed
                if (candidatesWithinRadius.size() >= limit) {
                    logger.debug("Found sufficient POIs ({}) within radius after ring {}, stopping expansion", 
                        candidatesWithinRadius.size(), ring);
                    break;
                }
            }
            
            // Find the closest POIs up to the limit
            List<POIData> closestPOIs = findClosestPOIs(allPOIs, lat, lon, limit);
            
            // Convert POIs to response format
            List<POIResponse> results = new ArrayList<>();
            for (POIData poi : closestPOIs) {
                results.add(convertPOIToResponse(poi, lat, lon, lang));
            }
            
            logger.debug("Final result: {} POIs found from {} searched shards", results.size(), searchedShards.size());
            return results;
            
        } catch (RocksDBException e) {
            logger.error("RocksDB error during reverse geocoding for lat={}, lon={}", lat, lon, e);
            throw new RuntimeException("Database error during reverse geocoding", e);
        } catch (Exception e) {
            logger.error("Error during reverse geocoding for lat={}, lon={}", lat, lon, e);
            throw new RuntimeException("Internal error during reverse geocoding", e);
        }
    }
    
    private List<POIData> loadPOIsFromShard(long shardId) throws RocksDBException {
        if (shardsDb == null) {
            logger.debug("RocksDB not initialized");
            return Collections.emptyList();
        }
        
        byte[] key = s2Helper.longToByteArray(shardId);
        byte[] data = shardsDb.get(key);
        
        if (data == null) {
            logger.debug("No data found for shard: {}", shardId);
            return Collections.emptyList();
        }
        
        ByteBuffer buffer = ByteBuffer.wrap(data);
        POIList poiList = POIList.getRootAsPOIList(buffer);
        List<POIData> pois = new ArrayList<>();
        
        logger.debug("Found {} POIs in shard {}", poiList.poisLength(), shardId);
        
        for (int i = 0; i < poiList.poisLength(); i++) {
            POI poi = poiList.pois(i);
            logger.debug("POI {}: type={}, subtype={}, names={}, hierarchy={}", 
                poi.id(), poi.type(), poi.subtype(), poi.namesLength(), poi.hierarchyLength());
            
            // Copy names immediately while FlatBuffer is valid
            Map<String, String> names = new HashMap<>();
            for (int j = 0; j < poi.namesLength(); j++) {
                Name name = poi.names(j);
                String lang = name.lang();
                String text = name.text();
                if (lang != null && text != null) {
                    names.put(lang, text);
                    logger.debug("Copied name: lang='{}', text='{}'", lang, text);
                }
            }
            
            // Copy address if present
            AddressData addressData = null;
            if (poi.address() != null) {
                Address address = poi.address();
                String street = address.street();
                String houseNumber = address.houseNumber();
                String postcode = address.postcode();
                String city = address.city();
                String country = address.country();
                
                // Only create AddressData if at least one field is present
                if (street != null || houseNumber != null || postcode != null || city != null || country != null) {
                    addressData = new AddressData(
                        street,
                        houseNumber,
                        postcode,
                        city,
                        country
                    );
                    logger.debug("Copied address: street='{}', houseNumber='{}', postcode='{}', city='{}', country='{}'",
                        street, houseNumber, postcode, city, country);
                }
            }
            
            // Copy hierarchy immediately while FlatBuffer is valid
            List<HierarchyData> hierarchy = new ArrayList<>();
            for (int j = 0; j < poi.hierarchyLength(); j++) {
                HierarchyItem item = poi.hierarchy(j);
                String itemType = item.type();
                String itemName = item.name();
                hierarchy.add(new HierarchyData(
                    item.level(),
                    itemType != null ? itemType : "unknown",
                    itemName != null ? itemName : "Unknown",
                    item.osmId()
                ));
                logger.debug("Copied hierarchy: level={}, type={}, name={}", 
                    item.level(), itemType, itemName);
            }
            
            // Copy boundary geometry if present
            byte[] boundaryData = null;
            if (poi.boundary() != null) {
                com.dedicatedcode.paikka.flatbuffers.Geometry boundary = poi.boundary();
                if (boundary.dataLength() > 0) {
                    boundaryData = new byte[boundary.dataLength()];
                    for (int k = 0; k < boundary.dataLength(); k++) {
                        boundaryData[k] = (byte) boundary.data(k);
                    }
                    logger.debug("Copied boundary geometry: {} bytes", boundaryData.length);
                }
            }
            
            // Create POIData with copied data
            POIData poiData = new POIData(
                poi.id(),
                poi.lat(),
                poi.lon(),
                poi.type(),
                poi.subtype(),
                names,
                addressData,
                hierarchy,
                boundaryData
            );
            
            pois.add(poiData);
        }
        
        return pois;
    }
    
    private List<POIData> findClosestPOIs(List<POIData> pois, double targetLat, double targetLon, int limit) {
        List<POIWithDistance> poisWithDistance = new ArrayList<>();
        
        for (POIData poi : pois) {
            double distance = calculateDistance(targetLat, targetLon, poi.lat(), poi.lon());
            
            if (distance <= SEARCH_RADIUS_KM) {
                poisWithDistance.add(new POIWithDistance(poi, distance));
            }
        }
        
        // Sort by distance (closest first)
        poisWithDistance.sort(Comparator.comparingDouble(POIWithDistance::distance));
        
        // Return up to the requested limit
        return poisWithDistance.stream()
                .limit(limit)
                .map(POIWithDistance::poi)
                .collect(Collectors.toList());
    }
    
    private double calculateDistance(double lat1, double lon1, double lat2, double lon2) {
        // Simple Euclidean distance calculation - sufficient for small areas (~2km shards)
        // Approximate conversion: 1 degree latitude ≈ 111 km, longitude varies by latitude
        double latDiff = lat1 - lat2;
        double lonDiff = (lon1 - lon2) * Math.cos(Math.toRadians((lat1 + lat2) / 2));
        
        // Convert to kilometers (approximately)
        double latDistKm = latDiff * 111.0;
        double lonDistKm = lonDiff * 111.0;
        
        return Math.sqrt(latDistKm * latDistKm + lonDistKm * lonDistKm);
    }
    
    private POIResponse convertPOIToResponse(POIData poi, double queryLat, double queryLon, String lang) {
        POIResponse response = new POIResponse();
        
        // Basic POI information
        response.setId(poi.id());
        response.setLat(poi.lat());
        response.setLon(poi.lon());
        response.setType(poi.type());
        response.setSubtype(poi.subtype());
        
        // Distance from query point
        double distance = calculateDistance(queryLat, queryLon, poi.lat(), poi.lon());
        response.setDistanceKm(Math.round(distance * 100.0) / 100.0);

        Map<String, String> poiNames = poi.names();
        String displayName = null;

        logger.debug("POI {} has {} names", poi.id(), poiNames.size());
        
        if (poiNames.containsKey(lang)) {
            displayName = poiNames.get(lang);
        } else if (poiNames.containsKey("default")) {
            displayName = poiNames.get("default");
        } else if (!poiNames.isEmpty()) {
            displayName = poiNames.values().iterator().next();
        }
        
        response.setNames(poiNames);
        response.setDisplayName(displayName);
        
        // Address information
        AddressData addressData = poi.address();
        if (addressData != null) {
            Map<String, String> addressMap = new HashMap<>();
            if (addressData.street() != null) addressMap.put("street", addressData.street());
            if (addressData.houseNumber() != null) addressMap.put("house_number", addressData.houseNumber());
            if (addressData.postcode() != null) addressMap.put("postcode", addressData.postcode());
            if (addressData.city() != null) addressMap.put("city", addressData.city());
            if (addressData.country() != null) addressMap.put("country", addressData.country());
            
            if (!addressMap.isEmpty()) {
                response.setAddress(addressMap);
                logger.debug("POI {} has address: {}", poi.id(), addressMap);
            }
        }
        
        // Hierarchy information
        List<POIResponse.HierarchyItem> hierarchy = new ArrayList<>();
        logger.debug("POI {} has {} hierarchy items", poi.id(), poi.hierarchy().size());

        for (HierarchyData item : poi.hierarchy()) {
            logger.debug("Hierarchy: level={}, type={}, name={}, osmId={}",
                         item.level(), item.type(), item.name(), item.osmId());
            
            String geometryUrl = buildGeometryUrl(item.osmId());
            hierarchy.add(new POIResponse.HierarchyItem(item.level(), item.type(), item.name(), item.osmId(), geometryUrl));
        }
        response.setHierarchy(hierarchy);
        
        // Boundary information (if available)
        if (poi.boundary() != null && poi.boundary().length > 0) {
            try {
                WKBReader wkbReader = new WKBReader();
                Geometry geometry = wkbReader.read(poi.boundary());
                
                // Create a simple GeoJSON representation
                GeoJsonGeometry geoJsonGeometry = convertJtsToGeoJson(geometry);
                response.setBoundary(geoJsonGeometry);
                
                logger.debug("POI {} has boundary geometry converted to GeoJSON", poi.id());
            } catch (Exception e) {
                logger.warn("Failed to convert boundary geometry to GeoJSON for POI {}: {}", poi.id(), e.getMessage());
                // Set null boundary on error
                response.setBoundary(null);
            }
        }
        
        // Query information
        response.setQuery(new POIResponse.QueryInfo(queryLat, queryLon, lang));
        
        return response;
    }
    

    private record POIData(long id, float lat, float lon, String type, String subtype, Map<String, String> names,
                           AddressData address, List<HierarchyData> hierarchy, byte[] boundary) {
    }

    private record AddressData(String street, String houseNumber, String postcode, String city, String country) {
    }

    private record HierarchyData(int level, String type, String name, long osmId) {
    }

    private record POIWithDistance(POIData poi, double distance) {
    }
    
    /**
     * Convert JTS Geometry to GeoJSON format.
     * This is a simplified conversion that handles basic geometry types.
     */
    private GeoJsonGeometry convertJtsToGeoJson(Geometry geometry) {
        String geometryType = geometry.getGeometryType();
        Object coordinates = null;
        
        switch (geometryType) {
            case "Point":
                coordinates = new double[]{geometry.getCoordinate().x, geometry.getCoordinate().y};
                break;
            case "Polygon":
                // For polygons, we need to extract the exterior ring coordinates
                // This is a simplified implementation
                coordinates = extractPolygonCoordinates(geometry);
                break;
            case "MultiPolygon":
                // For multipolygons, extract all polygon coordinates
                coordinates = extractMultiPolygonCoordinates(geometry);
                break;
            default:
                // For other geometry types, just indicate the type
                logger.debug("Unsupported geometry type for GeoJSON conversion: {}", geometryType);
                return new GeoJsonGeometry("Unknown", null);
        }
        
        return new GeoJsonGeometry(geometryType, coordinates);
    }
    
    private Object extractPolygonCoordinates(Geometry polygon) {
        try {
            // Get exterior ring coordinates
            org.locationtech.jts.geom.Coordinate[] coords = polygon.getCoordinates();
            double[][] ring = new double[coords.length][2];
            
            for (int i = 0; i < coords.length; i++) {
                ring[i][0] = coords[i].x; // longitude
                ring[i][1] = coords[i].y; // latitude
            }
            
            // GeoJSON polygon format: [[[x,y],[x,y],...]]
            return new double[][][]{ring};
        } catch (Exception e) {
            logger.warn("Failed to extract polygon coordinates: {}", e.getMessage());
            return null;
        }
    }
    
    private Object extractMultiPolygonCoordinates(Geometry multiPolygon) {
        try {
            List<double[][][]> polygons = new ArrayList<>();
            
            for (int i = 0; i < multiPolygon.getNumGeometries(); i++) {
                Geometry polygon = multiPolygon.getGeometryN(i);
                Object polyCoords = extractPolygonCoordinates(polygon);
                if (polyCoords instanceof double[][][]) {
                    polygons.add((double[][][]) polyCoords);
                }
            }
            
            return polygons.toArray(new double[0][][][]);
        } catch (Exception e) {
            logger.warn("Failed to extract multipolygon coordinates: {}", e.getMessage());
            return null;
        }
    }
    
    /**
     * Build the geometry URL using the configured base URL and current data version.
     */
    private String buildGeometryUrl(long osmId) {
        String baseUrl = config.getQueryConfiguration().getBaseUrl();

        // Normalize base URL by removing trailing slash if present
        if (baseUrl != null && baseUrl.endsWith("/")) {
            baseUrl = baseUrl.substring(0, baseUrl.length() - 1);
        } else if (baseUrl == null || baseUrl.isEmpty()) {
            baseUrl = ""; // Default to empty string for relative path if no base URL is configured
        }

        return String.format("%s/api/v1/geometry/%s/%d", baseUrl, "latest", osmId);
    }
    
}
