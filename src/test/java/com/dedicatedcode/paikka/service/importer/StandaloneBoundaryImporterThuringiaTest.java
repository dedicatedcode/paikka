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
import org.junit.jupiter.api.*;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.WKBReader;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for proper hole-in-polygon assignment in the standalone boundary importer.
 * Uses thueringen-260804.boundaries.osm.pbf which has multipolygon relations
 * with disjoint outer rings and inner rings (holes). This verifies the fix
 * for the bug where holes were incorrectly assigned to all outer rings,
 * causing rendering artifacts (large open triangles / lines).
 */
class StandaloneBoundaryImporterThuringiaTest {

    private static Path tempOutputDir;
    private static Path tempPbfFile;

    @BeforeAll
    static void setUp() throws Exception {
        tempOutputDir = Files.createTempDirectory("paikka-boundary-thueringen-test");
        tempPbfFile = Files.createTempFile("paikka-boundary-thueringen-test", ".pbf");

        try (InputStream is = StandaloneBoundaryImporter.class.getClassLoader()
                .getResourceAsStream("thueringen-260804.boundaries.osm.pbf")) {
            assertNotNull(is, "thueringen-260804.boundaries.osm.pbf not found in test resources");
            Files.copy(is, tempPbfFile, StandardCopyOption.REPLACE_EXISTING);
        }

        PaikkaConfiguration config = new PaikkaConfiguration();

        PaikkaConfiguration.ImportConfiguration importCfg = new PaikkaConfiguration.ImportConfiguration();
        importCfg.setThreads(4);
        config.setImportConfiguration(importCfg);

        PaikkaConfiguration.SimplificationConfiguration simplCfg = new PaikkaConfiguration.SimplificationConfiguration();
        simplCfg.setContinentTolerance(0.005);
        simplCfg.setCountryTolerance(0.00045);
        simplCfg.setStateTolerance(0.00009);
        simplCfg.setPoiTolerance(0.000018);
        simplCfg.setDefaultTolerance(0.000045);
        config.setSimplificationConfiguration(simplCfg);

        GeometrySimplificationService simplService = new GeometrySimplificationService(config);
        StandaloneBoundaryImporter importer = new StandaloneBoundaryImporter(simplService, config);
        importer.importBoundaries(Collections.singletonList(tempPbfFile.toString()), tempOutputDir.toString());
    }

    @AfterAll
    static void tearDown() {
        if (tempOutputDir != null && Files.exists(tempOutputDir)) {
            deleteDirectory(tempOutputDir.toFile());
        }
        if (tempPbfFile != null && Files.exists(tempPbfFile)) {
            tempPbfFile.toFile().delete();
        }
    }

    // ===== infrastructure =====

    @Test
    void testOutputDatabasesExist() {
        assertTrue(Files.exists(tempOutputDir.resolve("h3_to_osm")), "h3_to_osm directory should exist");
        assertTrue(Files.exists(tempOutputDir.resolve("region_metadata")), "region_metadata directory should exist");
        assertTrue(Files.exists(tempOutputDir.resolve("region_geometry")), "region_geometry directory should exist");
        assertTrue(Files.exists(tempOutputDir.resolve("osm_names.tsv")), "osm_names.tsv should exist");
    }

    @Test
    void testAllGeometryValid() throws Exception {
        Path geomDbPath = tempOutputDir.resolve("region_geometry");
        WKBReader wkbReader = new WKBReader();

        try (Options opts = new Options().setCreateIfMissing(false);
             RocksDB db = RocksDB.open(opts, geomDbPath.toString())) {

            var it = db.newIterator();
            it.seekToFirst();
            int checked = 0;
            List<Long> invalidOsmIds = new ArrayList<>();

            while (it.isValid()) {
                long osmId = ByteBuffer.wrap(it.key()).order(ByteOrder.BIG_ENDIAN).getLong();
                byte[] wkb = it.value();
                Geometry geom = wkbReader.read(wkb);

                if (!geom.isValid()) {
                    invalidOsmIds.add(osmId);
                }
                checked++;
                it.next();
            }

            System.out.println("Checked " + checked + " geometries for validity");
            assertTrue(checked > 0, "Should have at least one geometry entry");

            if (!invalidOsmIds.isEmpty()) {
                System.out.println("Invalid geometries (OSM IDs): " + invalidOsmIds);
            }
            assertTrue(invalidOsmIds.isEmpty(),
                    "All stored geometries should be valid. Invalid OSM IDs: " + invalidOsmIds);
        }
    }

    @Test
    void testHolesAreContainedInOuterPolygons() throws Exception {
        Path geomDbPath = tempOutputDir.resolve("region_geometry");
        WKBReader wkbReader = new WKBReader();

        try (Options opts = new Options().setCreateIfMissing(false);
             RocksDB db = RocksDB.open(opts, geomDbPath.toString())) {

            var it = db.newIterator();
            it.seekToFirst();
            int checked = 0;
            List<String> violations = new ArrayList<>();

            while (it.isValid()) {
                long osmId = ByteBuffer.wrap(it.key()).order(ByteOrder.BIG_ENDIAN).getLong();
                byte[] wkb = it.value();
                Geometry geom = wkbReader.read(wkb);

                violations.addAll(checkHoleContainment(osmId, geom));
                checked++;
                it.next();
            }

            System.out.println("Checked " + checked + " geometries for hole containment");
            assertTrue(checked > 0, "Should have at least one geometry entry");

            if (!violations.isEmpty()) {
                for (String v : violations) {
                    System.out.println("VIOLATION: " + v);
                }
            }
            assertTrue(violations.isEmpty(),
                    "All inner rings (holes) must be contained within their outer polygon shell. Violations:\n"
                    + String.join("\n", violations));
        }
    }

    @Test
    void testGeometryHasExpectedTopology() throws Exception {
        /*
         * Thuringia (OSM relation 62366) is a multipolygon: one main body plus
         * exclaves. The stored WKB should be a Polygon or MultiPolygon where
         * every inner ring (hole) is properly contained within an outer ring.
         *
         * Assertion to implement:
         * 1. Load geometry for Thuringia relation by OSM ID
         * 2. Verify it's a Polygon or MultiPolygon
         * 3. For each polygon, verify all interior rings are within the exterior ring
         * 4. Verify there are no "floating" holes (holes with no containing shell)
         */

        Path geomDbPath = tempOutputDir.resolve("region_geometry");
        long thueringenOsmId = 62366L;
        WKBReader wkbReader = new WKBReader();

        try (Options opts = new Options().setCreateIfMissing(false);
             RocksDB db = RocksDB.open(opts, geomDbPath.toString())) {

            byte[] key = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(thueringenOsmId).array();
            byte[] wkb = db.get(key);

            assertNotNull(wkb, "Thuringia (OSM ID " + thueringenOsmId + ") should have stored geometry");
            assertTrue(wkb.length > 0, "Thuringia WKB should not be empty");

            Geometry geom = wkbReader.read(wkb);
            System.out.println("Thuringia geometry: type=" + geom.getGeometryType()
                    + " valid=" + geom.isValid()
                    + " numGeometries=" + geom.getNumGeometries()
                    + " numPoints=" + geom.getNumPoints());

            assertTrue(geom.isValid(), "Thuringia geometry should be valid");
            assertTrue(geom instanceof Polygon || geom instanceof MultiPolygon,
                    "Thuringia geometry should be a Polygon or MultiPolygon, got: " + geom.getGeometryType());
        }
    }

    // ===== helpers =====

    private static final GeometryFactory GEOM_CHECK_FACTORY = new GeometryFactory();

    /**
     * Checks that every interior ring of every polygon is properly contained
     * within that polygon's exterior ring. Returns a list of violation messages,
     * empty if all is well.
     */
    private static List<String> checkHoleContainment(long osmId, Geometry geom) {
        List<String> violations = new ArrayList<>();
        int numParts = geom.getNumGeometries();
        for (int g = 0; g < numParts; g++) {
            Geometry part = geom.getGeometryN(g);
            if (!(part instanceof Polygon polygon))
                continue;

            LinearRing shell = polygon.getExteriorRing();
            Polygon shellPoly = GEOM_CHECK_FACTORY.createPolygon(shell);
            for (int h = 0; h < polygon.getNumInteriorRing(); h++) {
                LinearRing hole = polygon.getInteriorRingN(h);
                Polygon holePoly = GEOM_CHECK_FACTORY.createPolygon(hole);
                if (!shellPoly.contains(holePoly)) {
                    violations.add(String.format(
                            "OSM %d: interior ring %d is not contained within its exterior ring (hole centroid: %.6f,%.6f)",
                            osmId, h, hole.getCentroid().getX(), hole.getCentroid().getY()));
                }
            }
        }
        return violations;
    }

    private static void deleteDirectory(java.io.File dir) {
        if (dir.isDirectory()) {
            java.io.File[] children = dir.listFiles();
            if (children != null) {
                for (java.io.File child : children) {
                    deleteDirectory(child);
                }
            }
        }
        dir.delete();
    }
}
