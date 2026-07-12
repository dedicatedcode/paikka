package com.dedicatedcode.paikka.service;

import com.dedicatedcode.paikka.IntegrationTest;
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
        assertTrue(osmIds.contains(367855L), "Should contain Innenstadt (OSM ID 367855)");
        assertTrue(osmIds.contains(27027L), "Should contain Lübeck (OSM ID 62422)");
        // Schleswig-Holstein
        assertTrue(osmIds.contains(51529L), "Should contain Schleswig-Holstein (OSM ID 51529)");
        // Germany
        assertTrue(osmIds.contains(51477L), "Should contain Germany (OSM ID 51477)");

        // Verify that we have some total cells calculated
        assertTrue(results.stream().anyMatch(b -> b.totalCells() > 0), "At least one boundary should have a positive total cell count");
    }

    @Test
    void shouldLookupBoundariesForPoelzig() {
        // Coordinates for Pölzig
        double lat = 50.957171;
        double lng = 12.208514;

        List<BoundaryLookupService.BoundaryInfo> results = boundaryLookupService.lookup(lat, lng);

        assertFalse(results.isEmpty(), "Should find at least one boundary for Pölzig coordinates");

        List<Long> osmIds = results.stream().map(BoundaryLookupService.BoundaryInfo::osmId).toList();

        // Pölzig (Ebene 8)
        assertTrue(osmIds.contains(2532078L), "Should contain (OSM ID 2532078)");
        // Am Brahmetal (Ebene 7)
        assertTrue(osmIds.contains(2907045L), "Should contain (OSM ID 2907045)");
        // Greiz (Ebene 6)
        assertTrue(osmIds.contains(62445L), "Should contain (OSM ID 62445)");
        // Thüringen (Ebene 4)
        assertTrue(osmIds.contains(62366L), "Should contain (OSM ID 62366)");
        // Germany
        assertTrue(osmIds.contains(51477L), "Should contain Germany (OSM ID 51477)");

        // Verify that we have some total cells calculated
        assertTrue(results.stream().anyMatch(b -> b.totalCells() > 0), "At least one boundary should have a positive total cell count");
    }
}
