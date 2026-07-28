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

package com.dedicatedcode.paikka.service.importer;

import com.dedicatedcode.paikka.config.PaikkaConfiguration;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.simplify.DouglasPeuckerSimplifier;
import org.springframework.stereotype.Service;

/**
 * Service for geometry simplification using Douglas-Peucker algorithm.
 * This is critical for keeping storage manageable while preserving shape accuracy.
 */
@Service
public class GeometrySimplificationService {

    private final double continentTolerance;
    private final double countryTolerance;
    private final double stateTolerance;
    private final double poiTolerance;
    private final double defaultTolerance;

    public GeometrySimplificationService(PaikkaConfiguration paikkaConfiguration) {
        this.continentTolerance = paikkaConfiguration.getSimplificationConfiguration().getContinentTolerance();
        this.countryTolerance = paikkaConfiguration.getSimplificationConfiguration().getCountryTolerance();
        this.stateTolerance = paikkaConfiguration.getSimplificationConfiguration().getStateTolerance();
        this.poiTolerance = paikkaConfiguration.getSimplificationConfiguration().getPoiTolerance();
        this.defaultTolerance = paikkaConfiguration.getSimplificationConfiguration().getDefaultTolerance();
    }
    /**
     * Simplify geometry using Douglas-Peucker algorithm with default tolerance.
     * 
     * @param geometry Input geometry to simplify
     * @return Simplified geometry
     */
    public Geometry simplify(Geometry geometry) {
        return simplify(geometry, defaultTolerance);
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
            case 1 -> continentTolerance; // Continent / Supranational
            case 2 -> countryTolerance;   // Country
            case 4, 6 -> stateTolerance;  // State/Region
            default -> defaultTolerance;
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
        return simplify(geometry, poiTolerance);
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
