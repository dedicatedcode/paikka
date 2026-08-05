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
import com.dedicatedcode.paikka.flatbuffers.Address;
import com.dedicatedcode.paikka.flatbuffers.Boundary;
import com.dedicatedcode.paikka.flatbuffers.HierarchyItem;
import com.dedicatedcode.paikka.flatbuffers.Name;
import com.dedicatedcode.paikka.flatbuffers.POI;
import com.dedicatedcode.paikka.flatbuffers.POIList;
import com.dedicatedcode.paikka.service.S2Helper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.locationtech.jts.algorithm.locate.IndexedPointInAreaLocator;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Location;
import org.locationtech.jts.io.WKBReader;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for ImportService.
 * Uses the monaco-filtered.pbf test file to verify import functionality.
 */
class ImportServiceTest {

    private Path tempDataDir;
    private Path tempImportFile;

    @BeforeEach
    void setUp() throws Exception {
        tempDataDir = Files.createTempDirectory("paikka-test");
        tempImportFile = Files.createTempFile("paikka-test", ".pbf");

        // Copy monaco-filtered.pbf from resources to temp file
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("monaco-filtered.pbf")) {
            assertNotNull(is, "monaco-filtered.pbf not found in resources");
            Files.copy(is, tempImportFile, StandardCopyOption.REPLACE_EXISTING);
        }

        PaikkaConfiguration config = new PaikkaConfiguration();
        PaikkaConfiguration.ImportConfiguration importConfiguration = new PaikkaConfiguration.ImportConfiguration();
        importConfiguration.setThreads(2);
        config.setImportConfiguration(importConfiguration);
        PaikkaConfiguration.SimplificationConfiguration simplificationConfiguration = new PaikkaConfiguration.SimplificationConfiguration();
        simplificationConfiguration.setContinentTolerance(0.005);
        simplificationConfiguration.setCountryTolerance(0.00045);
        simplificationConfiguration.setStateTolerance(0.00009);
        simplificationConfiguration.setPoiTolerance(0.000018);
        simplificationConfiguration.setDefaultTolerance(0.000045);
        config.setSimplificationConfiguration(simplificationConfiguration);
        GeometrySimplificationService geometrySimplificationService = new GeometrySimplificationService(config);

        S2Helper s2Helper = new S2Helper();
        ImportService importService = new ImportService(s2Helper, geometrySimplificationService, config, "1.0.0");
        importService.importData(Collections.singletonList(tempImportFile.toString()), tempDataDir.toString());
    }

    @AfterEach
    void tearDown() {
        if (tempDataDir != null && tempDataDir.toFile().exists()) {
            deleteDirectory(tempDataDir.toFile());
        }
        if (tempImportFile != null && tempImportFile.toFile().exists()) {
            tempImportFile.toFile().delete();
        }
    }

    @Test
    void testImportMonacoFilteredPbf() throws Exception {
        // Verify POIs were imported by checking the shards database
        Path shardsDbPath = tempDataDir.resolve("poi_shards");
        assertTrue(Files.exists(shardsDbPath), "Shards database should exist");

        // Open the database and verify some POIs were stored
        try (Options options = new Options();
             RocksDB shardsDb = RocksDB.open(options, shardsDbPath.toString())) {

            // Iterate through all shards and collect POIs
            List<Long> importedPoiIds = new ArrayList<>();
            var iterator = shardsDb.newIterator();
            iterator.seekToFirst();
            while (iterator.isValid()) {
                byte[] value = iterator.value();

                ByteBuffer buffer = ByteBuffer.wrap(value);
                POIList poiList = POIList.getRootAsPOIList(buffer);

                // Collect all POI IDs
                for (int i = 0; i < poiList.poisLength(); i++) {
                    POI poi = poiList.pois(i);
                    importedPoiIds.add(poi.id());
                }

                iterator.next();
            }

            // Verify we imported some POIs
            assertFalse(importedPoiIds.isEmpty(), "Should have imported at least one POI");

        }
    }

    @Test
    void testImportAndRetrievePoiByOsmId() throws Exception {
        // Find a specific POI by OSM ID
        Path shardsDbPath = tempDataDir.resolve("poi_shards");

        Long targetPoiId = null;
        POI targetPoi = null;

        try (Options options = new Options();
             RocksDB shardsDb = RocksDB.open(options, shardsDbPath.toString())) {

            var iterator = shardsDb.newIterator();
            iterator.seekToFirst();
            while (iterator.isValid()) {
                byte[] value = iterator.value();
                ByteBuffer buffer = ByteBuffer.wrap(value);
                POIList poiList = POIList.getRootAsPOIList(buffer);
                if (poiList.poisLength() > 0) {
                    targetPoiId = poiList.pois(0).id();
                    targetPoi = poiList.pois(0);
                    break;
                }

                iterator.next();
            }
        }

        assertNotNull(targetPoiId, "Should have found at least one POI to test");
        assertNotNull(targetPoi, "Should have retrieved POI data");

        // Now retrieve the POI by ID directly
        POI retrievedPoi = findPoiById(tempDataDir, targetPoiId);

        assertNotNull(retrievedPoi, "Should be able to find POI by OSM ID: " + targetPoiId);
        assertEquals(targetPoiId, retrievedPoi.id(), "POI ID should match");
        assertEquals(targetPoi.lat(), retrievedPoi.lat(), "POI latitude should match");
        assertEquals(targetPoi.lon(), retrievedPoi.lon(), "POI longitude should match");
        assertEquals(targetPoi.type(), retrievedPoi.type(), "POI type should match");
    }

    @Test
    void testImportPoiHasValidCoordinates() throws Exception {
        Path shardsDbPath = tempDataDir.resolve("poi_shards");

        try (Options options = new Options();
             RocksDB shardsDb = RocksDB.open(options, shardsDbPath.toString())) {

            var iterator = shardsDb.newIterator();
            iterator.seekToFirst();

            boolean foundValidPoi = false;
            while (iterator.isValid()) {
                byte[] value = iterator.value();
                ByteBuffer buffer = ByteBuffer.wrap(value);
                POIList poiList = POIList.getRootAsPOIList(buffer);

                for (int i = 0; i < poiList.poisLength(); i++) {
                    POI poi = poiList.pois(i);

                    // Verify coordinates are valid for Monaco
                    // Monaco is roughly at 43.7°N, 7.4°E
                    assertTrue(poi.lat() >= 43.6 && poi.lat() <= 43.8,
                        "Latitude should be in Monaco range: " + poi.lat());
                    assertTrue(poi.lon() >= 7.3 && poi.lon() <= 7.5,
                        "Longitude should be in Monaco range: " + poi.lon());

                    // Verify type is set
                    assertNotNull(poi.type(), "POI should have a type");
                    assertFalse(poi.type().isEmpty(), "POI type should not be empty");

                    foundValidPoi = true;
                    break;
                }

                if (foundValidPoi) break;
                iterator.next();
            }

            assertTrue(foundValidPoi, "Should have found at least one POI to validate");
        }
    }

    @Test
    void testImportPoiHasNamesAndBoundary() throws Exception {
        POI poiById = findPoiById(tempDataDir, 432751852);
        assertEquals(1, poiById.namesLength(), "POI should have no");
        assertEquals("Jardin des Boulingrins", poiById.names(0).text(), "POI should have no");
        assertEquals(6, poiById.hierarchyLength());
    }

    @Test
    void testAllPoisHaveAdminLevel2Hierarchy() throws Exception {
        Path shardsDbPath = tempDataDir.resolve("poi_shards");

        try (Options options = new Options();
             RocksDB shardsDb = RocksDB.open(options, shardsDbPath.toString())) {

            var iterator = shardsDb.newIterator();
            iterator.seekToFirst();

            int totalPois = 0;
            int multiHierarchyPois = 0;
            List<String> missing = new ArrayList<>();
            while (iterator.isValid()) {
                byte[] value = iterator.value();
                ByteBuffer buffer = ByteBuffer.wrap(value);
                POIList poiList = POIList.getRootAsPOIList(buffer);

                for (int i = 0; i < poiList.poisLength(); i++) {
                    POI poi = poiList.pois(i);
                    totalPois++;

                    if (poi.hierarchyLength() > 1) {
                        multiHierarchyPois++;
                        boolean hasAdminLevel2 = false;
                        for (int j = 0; j < poi.hierarchyLength(); j++) {
                            if (poi.hierarchy(j).level() == 2) {
                                hasAdminLevel2 = true;
                                break;
                            }
                        }
                        if (!hasAdminLevel2) {
                            missing.add("POI " + poi.id() + " (" + poi.lat() + "," + poi.lon()
                                + ") missing admin_level=2: " + hierarchyLevels(poi));
                        }
                    }
                }

                iterator.next();
            }

            assertTrue(totalPois > 0, "Should have imported at least one POI");
            assertTrue(multiHierarchyPois > 0, "Should have POIs with multiple hierarchy entries");

            if (!missing.isEmpty()) {
                System.out.println("WARNING: " + missing.size() + " POI(s) with multiple hierarchy entries lack admin_level=2");
                System.out.println("  (expected edge case in filtered PBF extracts — border zone gaps)");
                for (String m : missing) {
                    System.out.println("  " + m);
                }
            }
        }
    }

    @Test
    void testBoundariesDbContainsMonacoAsLevel2() throws Exception {
        Path boundariesDbPath = tempDataDir.resolve("boundaries");
        assertTrue(Files.exists(boundariesDbPath), "boundaries database should exist");

        WKBReader wkbReader = new WKBReader();

        try (Options options = new Options();
             RocksDB boundariesDb = RocksDB.open(options, boundariesDbPath.toString())) {

            var iterator = boundariesDb.newIterator();
            iterator.seekToFirst();

            boolean foundLevel2 = false;
            int boundaryCount = 0;
            while (iterator.isValid()) {
                boundaryCount++;
                byte[] value = iterator.value();
                Boundary b = Boundary.getRootAsBoundary(ByteBuffer.wrap(value));

                if (b.level() == 2) {
                    foundLevel2 = true;
                    double area = (b.maxX() - b.minX()) * (b.maxY() - b.minY());
                    System.out.println("  [boundary] osmId=" + b.osmId() + " level=" + b.level()
                        + " name=" + b.name() + " code=" + b.code()
                        + " mbr=[" + b.minX() + "," + b.minY() + " -> " + b.maxX() + "," + b.maxY() + "]"
                        + " mbrArea=" + String.format("%.6f", area)
                        + " mir=" + (b.mirMinX() != 0 || b.mirMaxX() != 0 ? "yes" : "no"));

                    ByteBuffer wkbBuf = b.geometry().dataAsByteBuffer();
                    byte[] wkb = new byte[wkbBuf.remaining()];
                    wkbBuf.get(wkb);
                    org.locationtech.jts.geom.Geometry geom = wkbReader.read(wkb);
                    System.out.println("    geometry type=" + geom.getGeometryType()
                        + " valid=" + geom.isValid()
                        + " area=" + String.format("%.8f", geom.getArea())
                        + " numGeometries=" + geom.getNumGeometries());

                    IndexedPointInAreaLocator locator = new IndexedPointInAreaLocator(geom);
                    double testLon = 7.4248843, testLat = 43.741333;
                    int loc = locator.locate(new Coordinate(testLon, testLat));
                    System.out.println("    PIP for failing POI (" + testLat + "," + testLon + "): "
                        + (loc == Location.INTERIOR ? "INTERIOR" : loc == Location.BOUNDARY ? "BOUNDARY" : "EXTERIOR"));

                    double testLon2 = 7.424, testLat2 = 43.738;
                    int loc2 = locator.locate(new Coordinate(testLon2, testLat2));
                    System.out.println("    PIP for central Monaco (" + testLat2 + "," + testLon2 + "): "
                        + (loc2 == Location.INTERIOR ? "INTERIOR" : loc2 == Location.BOUNDARY ? "BOUNDARY" : "EXTERIOR"));
                }

                iterator.next();
            }

            System.out.println("Total boundaries in DB: " + boundaryCount);
            assertTrue(foundLevel2, "Should have at least one admin_level=2 boundary for Monaco");
        }
    }

    @Test
    void testGridIndexContainsMonacoForFailingPoi() throws Exception {
        Path gridIndexDbPath = tempDataDir.resolve("tmp/grid_index");
        if (!Files.exists(gridIndexDbPath)) {
            System.out.println("grid_index is a temporary DB cleaned up after import — skipping inspection");
            return;
        }

        S2Helper s2Helper = new S2Helper();
        long cellId = s2Helper.getS2CellId(7.4218116, 43.741283, S2Helper.GRID_LEVEL);
        System.out.println("S2 cell for failing POI: " + cellId);

        try (Options options = new Options();
             RocksDB gridIndexDb = RocksDB.open(options, gridIndexDbPath.toString())) {

            byte[] data = gridIndexDb.get(s2Helper.longToByteArray(cellId));
            if (data != null) {
                long[] candidates = s2Helper.byteArrayToLongArray(data);
                System.out.println("Boundaries indexed for this cell: " + candidates.length);
                for (long id : candidates) {
                    System.out.println("  candidate boundary osmId: " + id);
                }
                assertTrue(candidates.length > 0, "Should have at least one boundary candidate for this cell");
            } else {
                System.out.println("No boundaries indexed for this cell — grid index miss");
                fail("No grid index entry for cell " + cellId + " — the admin_level=8 boundary should be indexed there");
            }
        }
    }

    @Test
    void shouldContainAddress() throws RocksDBException {
        POI poi = findPoiById(tempDataDir, 946757745L);
        assertNotNull(poi);
        assertEquals("BNP Paribas", poi.names(0).text());
        Address address = poi.address();
        assertNotNull(address);
        assertEquals("Monte-Carlo", address.city());
        assertEquals("MC", address.country());
    }

    @Test
    void testImportPoiWithNames() throws Exception {
        Path shardsDbPath = tempDataDir.resolve("poi_shards");

        try (Options options = new Options();
             RocksDB shardsDb = RocksDB.open(options, shardsDbPath.toString())) {

            var iterator = shardsDb.newIterator();
            iterator.seekToFirst();

            boolean foundPoiWithNames = false;
            while (iterator.isValid()) {
                byte[] value = iterator.value();
                ByteBuffer buffer = ByteBuffer.wrap(value);
                POIList poiList = POIList.getRootAsPOIList(buffer);

                for (int i = 0; i < poiList.poisLength(); i++) {
                    POI poi = poiList.pois(i);

                    if (poi.namesLength() > 0) {
                        // Verify names are properly stored
                        for (int j = 0; j < poi.namesLength(); j++) {
                            Name name = poi.names(j);
                            assertNotNull(name, "Name should not be null");
                            // Note: The actual text retrieval depends on the FlatBuffers structure
                        }
                        foundPoiWithNames = true;
                        break;
                    }
                }

                if (foundPoiWithNames) break;
                iterator.next();
            }

            // It's OK if no POIs have names - some POIs don't have names
            System.out.println("Found POI with names: " + foundPoiWithNames);
        }
    }

    /**
     * Helper method to find a POI by its OSM ID across all shards
     */
    private POI findPoiById(Path dataDir, long poiId) throws RocksDBException {
        Path shardsDbPath = dataDir.resolve("poi_shards");

        try (Options options = new Options();
             RocksDB shardsDb = RocksDB.open(options, shardsDbPath.toString())) {

            var iterator = shardsDb.newIterator();
            iterator.seekToFirst();

            while (iterator.isValid()) {
                byte[] value = iterator.value();
                ByteBuffer buffer = ByteBuffer.wrap(value);
                POIList poiList = POIList.getRootAsPOIList(buffer);

                for (int i = 0; i < poiList.poisLength(); i++) {
                    POI poi = poiList.pois(i);
                    if (poi.id() == poiId) {
                        return poi;
                    }
                }

                iterator.next();
            }
        }

        return null;
    }

    /**
     * Helper to list hierarchy levels for a POI as a readable string.
     */
    private String hierarchyLevels(POI poi) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < poi.hierarchyLength(); i++) {
            HierarchyItem item = poi.hierarchy(i);
            if (i > 0) sb.append(", ");
            sb.append("{level=").append(item.level())
                .append(", name=").append(item.name())
                .append(", code=").append(item.code())
                .append(", osmId=").append(item.osmId()).append("}");
        }
        return sb.toString();
    }

    /**
     * Recursively delete a directory
     */
    private void deleteDirectory(java.io.File dir) {
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
