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

import com.dedicatedcode.paikka.service.importer.GeometrySimplificationService;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@SpringJUnitConfig
class GeometrySimplificationServiceTest {
    
    private final GeometrySimplificationService service = new GeometrySimplificationService();
    private final GeometryFactory geometryFactory = new GeometryFactory();
    
    @Test
    void testSimplifyWithDefaultTolerance() {
        Geometry polygon = createTestPolygon();
        
        Geometry simplified = service.simplify(polygon);
        
        assertNotNull(simplified);
        assertTrue(simplified.isValid());
        // Simplified geometry should have same or fewer coordinates
        assertTrue(simplified.getCoordinates().length <= polygon.getCoordinates().length);
    }
    
    @Test
    void testSimplifyWithCustomTolerance() {
        Geometry polygon = createTestPolygon();
        double tolerance = 10.0;
        
        Geometry simplified = service.simplify(polygon, tolerance);
        
        assertNotNull(simplified);
        assertTrue(simplified.isValid());
        assertTrue(simplified.getCoordinates().length <= polygon.getCoordinates().length);
    }
    
    @Test
    void testSimplifyByAdminLevel() {
        Geometry polygon = createTestPolygon();
        
        // Test country level (should use high tolerance)
        Geometry countrySimplified = service.simplifyByAdminLevel(polygon, 2);
        assertNotNull(countrySimplified);
        assertTrue(countrySimplified.isValid());
        
        // Test state level
        Geometry stateSimplified = service.simplifyByAdminLevel(polygon, 4);
        assertNotNull(stateSimplified);
        assertTrue(stateSimplified.isValid());
        
        // Test default level
        Geometry defaultSimplified = service.simplifyByAdminLevel(polygon, 9);
        assertNotNull(defaultSimplified);
        assertTrue(defaultSimplified.isValid());
    }
    
    @Test
    void testSimplifyPoiBoundary() {
        Geometry polygon = createTestPolygon();
        
        Geometry simplified = service.simplifyPoiBoundary(polygon);
        
        assertNotNull(simplified);
        assertTrue(simplified.isValid());
        assertTrue(simplified.getCoordinates().length <= polygon.getCoordinates().length);
    }
    
    @Test
    void testSimplifyNullGeometry() {
        Geometry result = service.simplify(null);
        assertNull(result);
        
        result = service.simplifyByAdminLevel(null, 2);
        assertNull(result);
        
        result = service.simplifyPoiBoundary(null);
        assertNull(result);
    }
    
    @Test
    void testShouldSimplify() {
        Geometry polygon = createTestPolygon();
        
        // Should simplify if coordinate count exceeds threshold
        assertTrue(service.shouldSimplify(polygon, 3));
        
        // Should not simplify if coordinate count is below threshold
        assertFalse(service.shouldSimplify(polygon, 100));
        
        // Should not simplify null geometry
        assertFalse(service.shouldSimplify(null, 10));
    }
    
    private Geometry createTestPolygon() {
        // Create a simple polygon with multiple coordinates
        Coordinate[] coordinates = new Coordinate[] {
            new Coordinate(0, 0),
            new Coordinate(1, 0),
            new Coordinate(1.5, 0.1),
            new Coordinate(2, 0),
            new Coordinate(2, 1),
            new Coordinate(1.9, 1.1),
            new Coordinate(1, 1),
            new Coordinate(0, 1),
            new Coordinate(0, 0) // Close the ring
        };
        
        LinearRing ring = geometryFactory.createLinearRing(coordinates);
        return geometryFactory.createPolygon(ring);
    }
}
