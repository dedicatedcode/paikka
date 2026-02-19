package com.dedicatedcode.paikka.service;

import com.dedicatedcode.paikka.config.PaikkaConfiguration;
import com.dedicatedcode.paikka.dto.GeoJsonGeometry;
import com.dedicatedcode.paikka.exception.POINotFoundException;
import com.dedicatedcode.paikka.flatbuffers.Boundary;
import com.dedicatedcode.paikka.flatbuffers.Geometry;
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
import java.util.ArrayList;
import java.util.List;

/**
 * Service for boundary geometry operations.
 */
@Service
@ConditionalOnProperty(name = "paikka.import-mode", havingValue = "false", matchIfMissing = true)
public class BoundaryService {
    
    private static final Logger logger = LoggerFactory.getLogger(BoundaryService.class);
    
    private final PaikkaConfiguration config;
    private RocksDB boundariesDb;
    
    public BoundaryService(PaikkaConfiguration config) {
        this.config = config;
        initializeRocksDB();
    }
    
    private void initializeRocksDB() {
        if (boundariesDb != null) {
            return; // Already initialized
        }
        
        try {
            RocksDB.loadLibrary();
            Path boundariesDbPath = Paths.get(config.getDataDir(), "boundaries");
            
            // Check if the database directory exists
            if (!boundariesDbPath.toFile().exists()) {
                logger.warn("Boundaries database not found at: {}", boundariesDbPath);
                return;
            }
            
            Options options = new Options().setCreateIfMissing(false);
            this.boundariesDb = RocksDB.openReadOnly(options, boundariesDbPath.toString());
            logger.info("Successfully initialized RocksDB for boundaries");
        } catch (Exception e) {
            logger.warn("Failed to initialize RocksDB for boundaries: {}", e.getMessage());
            this.boundariesDb = null;
        }
    }
    
    /**
     * Get boundary geometry by OSM ID.
     * 
     * @param osmId OSM relation ID
     * @return Boundary geometry in GeoJSON format
     * @throws POINotFoundException if no boundary is found for the given OSM ID
     */
    public GeoJsonGeometry getBoundaryGeometry(long osmId) {
        logger.debug("Looking up boundary geometry for OSM ID: {}", osmId);
        
        if (boundariesDb == null) {
            throw new RuntimeException("Boundaries database not available");
        }
        
        try {
            // Convert OSM ID to byte array key
            byte[] key = longToByteArray(osmId);
            byte[] data = boundariesDb.get(key);
            
            if (data == null) {
                throw new POINotFoundException("No boundary found for OSM ID: " + osmId);
            }
            
            // Parse the boundary data
            ByteBuffer buffer = ByteBuffer.wrap(data);
            Boundary boundary = Boundary.getRootAsBoundary(buffer);
            
            if (boundary.geometry() == null) {
                throw new POINotFoundException("No geometry data found for OSM ID: " + osmId);
            }
            
            // Extract geometry data
            Geometry geometry = boundary.geometry();
            byte[] geometryData = new byte[geometry.dataLength()];
            for (int i = 0; i < geometry.dataLength(); i++) {
                geometryData[i] = (byte) geometry.data(i);
            }
            
            // Convert WKB to GeoJSON
            WKBReader wkbReader = new WKBReader();
            org.locationtech.jts.geom.Geometry jtsGeometry = wkbReader.read(geometryData);
            
            return convertJtsToGeoJson(jtsGeometry);
            
        } catch (RocksDBException e) {
            logger.error("RocksDB error retrieving boundary for OSM ID {}: {}", osmId, e.getMessage());
            throw new RuntimeException("Database error retrieving boundary", e);
        } catch (POINotFoundException e) {
            throw e; // Re-throw as-is
        } catch (Exception e) {
            logger.error("Error retrieving boundary for OSM ID {}: {}", osmId, e.getMessage());
            throw new RuntimeException("Internal error retrieving boundary", e);
        }
    }
    
    /**
     * Convert long to byte array for RocksDB key.
     */
    private byte[] longToByteArray(long value) {
        return new byte[] {
            (byte) (value >>> 56),
            (byte) (value >>> 48),
            (byte) (value >>> 40),
            (byte) (value >>> 32),
            (byte) (value >>> 24),
            (byte) (value >>> 16),
            (byte) (value >>> 8),
            (byte) value
        };
    }
    
    /**
     * Convert JTS Geometry to GeoJSON format.
     * This is a simplified conversion that handles basic geometry types.
     */
    private GeoJsonGeometry convertJtsToGeoJson(org.locationtech.jts.geom.Geometry geometry) {
        String geometryType = geometry.getGeometryType();
        Object coordinates = null;
        
        switch (geometryType) {
            case "Point":
                coordinates = new double[]{geometry.getCoordinate().x, geometry.getCoordinate().y};
                break;
            case "Polygon":
                coordinates = extractPolygonCoordinates(geometry);
                break;
            case "MultiPolygon":
                coordinates = extractMultiPolygonCoordinates(geometry);
                break;
            default:
                logger.debug("Unsupported geometry type for GeoJSON conversion: {}", geometryType);
                return new GeoJsonGeometry("Unknown", null);
        }
        
        return new GeoJsonGeometry(geometryType, coordinates);
    }
    
    private Object extractPolygonCoordinates(org.locationtech.jts.geom.Geometry polygon) {
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
    
    private Object extractMultiPolygonCoordinates(org.locationtech.jts.geom.Geometry multiPolygon) {
        try {
            List<double[][][]> polygons = new ArrayList<>();
            
            for (int i = 0; i < multiPolygon.getNumGeometries(); i++) {
                org.locationtech.jts.geom.Geometry polygon = multiPolygon.getGeometryN(i);
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
}
