package com.dedicatedcode.paikka.service;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@IntegrationTest
@TestPropertySource(properties = "paikka.data-dir=/home/daniel/projects/paikka/data-boundaries")
class BoundaryLookupServiceTest {

    @Autowired
    private BoundaryLookupService boundaryLookupService;

    @Test
    void shouldLookupBoundariesForLuebeck() {
        // Coordinates for Luebeck
        double lat = 53.86422;
        double lng = 10.69120;

        List<BoundaryLookupService.BoundaryInfo> results = boundaryLookupService.lookup(lat, lng);

        assertFalse(results.isEmpty(), "Should find at least one boundary for Luebeck coordinates");

        List<Long> osmIds = results.stream().map(BoundaryLookupService.BoundaryInfo::osmId).toList();

        // Lübeck (Ebene 6)
        assertTrue(osmIds.contains(62422L), "Should contain Lübeck (OSM ID 62422)");
        // Schleswig-Holstein
        assertTrue(osmIds.contains(4388L), "Should contain Schleswig-Holstein (OSM ID 4388)");
        // Germany
        assertTrue(osmIds.contains(51477L), "Should contain Germany (OSM ID 51477)");

        // Verify that we have some total cells calculated
        assertTrue(results.stream().anyMatch(b -> b.totalCells() > 0), "At least one boundary should have a positive total cell count");
    }
}
