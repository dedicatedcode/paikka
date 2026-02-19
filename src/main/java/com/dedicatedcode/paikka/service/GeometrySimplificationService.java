package com.dedicatedcode.paikka.service;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.simplify.DouglasPeuckerSimplifier;
import org.springframework.stereotype.Service;

/**
 * Service for geometry simplification using Douglas-Peucker algorithm.
 * This is critical for keeping storage manageable while preserving shape accuracy.
 */
@Service
public class GeometrySimplificationService {
    
    // Tolerance guidelines from implementation blueprint
    private static final double COUNTRY_TOLERANCE = 50.0; // 50 meters for country borders
    private static final double STATE_TOLERANCE = 10.0;   // 10 meters for state/city
    private static final double POI_TOLERANCE = 2.0;      // 2 meters for POI boundaries
    private static final double DEFAULT_TOLERANCE = 5.0;  // 5 meters default
    
    /**
     * Simplify geometry using Douglas-Peucker algorithm with default tolerance.
     * 
     * @param geometry Input geometry to simplify
     * @return Simplified geometry
     */
    public Geometry simplify(Geometry geometry) {
        return simplify(geometry, DEFAULT_TOLERANCE);
    }
    
    /**
     * Simplify geometry using Douglas-Peucker algorithm with specified tolerance.
     * 
     * @param geometry Input geometry to simplify
     * @param tolerance Tolerance in degrees (not meters)
     * @return Simplified geometry
     */
    public Geometry simplify(Geometry geometry, double tolerance) {
        if (geometry == null) {
            return null;
        }
        
        // Re-enable simplification with very conservative approach
        try {
            Geometry simplified = DouglasPeuckerSimplifier.simplify(geometry, tolerance);
            
            // Safety check: if simplification results in empty geometry, return original
            if (simplified == null || simplified.isEmpty() || simplified.getNumGeometries() == 0) {
                return geometry; // Return original geometry to prevent data loss
            }
            
            return simplified;
        } catch (Exception e) {
            // If simplification fails, return original geometry
            return geometry;
        }
    }
    
    /**
     * Simplify geometry based on administrative level.
     * 
     * @param geometry Input geometry to simplify
     * @param adminLevel Administrative level (2=country, 4=state, etc.)
     * @return Simplified geometry with appropriate tolerance
     */
    public Geometry simplifyByAdminLevel(Geometry geometry, int adminLevel) {
        if (geometry == null) {
            return null;
        }
        
        double tolerance = switch (adminLevel) {
            case 2 -> COUNTRY_TOLERANCE;  // Country
            case 4, 6 -> STATE_TOLERANCE; // State/Region
            default -> DEFAULT_TOLERANCE;
        };
        
        return simplify(geometry, tolerance);
    }
    
    /**
     * Simplify POI boundary geometry.
     * 
     * @param geometry Input POI geometry
     * @return Simplified geometry with POI-appropriate tolerance
     */
    public Geometry simplifyPoiBoundary(Geometry geometry) {
        return simplify(geometry, POI_TOLERANCE);
    }
    
    /**
     * Check if geometry should be simplified based on coordinate count.
     * 
     * @param geometry Geometry to check
     * @param threshold Minimum coordinate count to trigger simplification
     * @return true if geometry should be simplified
     */
    public boolean shouldSimplify(Geometry geometry, int threshold) {
        return geometry != null && geometry.getCoordinates().length > threshold;
    }
}
