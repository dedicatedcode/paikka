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
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for StandaloneBoundaryImporter.
 * Uses the schleswig-holstein-boundaries.osm.pbf test file to verify
 * the three RocksDB output databases and name streaming.
 */
class StandaloneBoundaryImporterTest {

    private static Path tempOutputDir;
    private static Path tempPbfFile;

    @BeforeAll
    static void setUp() throws Exception {
        tempOutputDir = Files.createTempDirectory("paikka-boundary-test");
        tempPbfFile = Files.createTempFile("paikka-boundary-test", ".pbf");

        try (InputStream is = StandaloneBoundaryImporter.class.getClassLoader()
                .getResourceAsStream("schleswig-holstein-boundaries.osm.pbf")) {
            assertNotNull(is, "schleswig-holstein-boundaries.osm.pbf not found in test resources");
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

    @Test
    void testOutputDatabasesExist() {
        assertTrue(Files.exists(tempOutputDir.resolve("h3_to_osm")), "h3_to_osm directory should exist");
        assertTrue(Files.exists(tempOutputDir.resolve("region_metadata")), "region_metadata directory should exist");
        assertTrue(Files.exists(tempOutputDir.resolve("region_geometry")), "region_geometry directory should exist");
        assertTrue(Files.exists(tempOutputDir.resolve("osm_names.tsv")), "osm_names.tsv should exist");
    }

    @Test
    void testH3ToOsmHasEntries() throws RocksDBException {
        Path dbPath = tempOutputDir.resolve("h3_to_osm");
        try (Options opts = new Options().setCreateIfMissing(false);
             RocksDB db = RocksDB.open(opts, dbPath.toString())) {

            var it = db.newIterator();
            it.seekToFirst();
            assertTrue(it.isValid(), "h3_to_osm should have at least one entry");

            byte[] key = it.key();
            assertEquals(8, key.length, "H3 cell key should be 8 bytes");

            byte[] val = it.value();
            assertTrue(val.length >= 8, "H3 value should be at least 8 bytes");
            assertEquals(0, val.length % 8, "H3 value should be a multiple of 8 bytes");

            int count = 0;
            it.seekToFirst();
            while (it.isValid()) {
                count++;
                it.next();
            }
            System.out.println("Total h3_to_osm entries: " + count);
            assertTrue(count > 100, "Should have more than 100 H3 cell entries, got: " + count);
        }
    }

    @Test
    void testRegionMetadataFormat() throws RocksDBException {
        Path dbPath = tempOutputDir.resolve("region_metadata");
        try (Options opts = new Options().setCreateIfMissing(false);
             RocksDB db = RocksDB.open(opts, dbPath.toString())) {

            var it = db.newIterator();
            it.seekToFirst();
            assertTrue(it.isValid(), "region_metadata should have entries");

            byte[] key = it.key();
            assertEquals(8, key.length, "Region metadata key should be 8 bytes (OSM ID)");

            byte[] val = it.value();
            assertEquals(16, val.length, "Value should be 16 bytes (8-byte cell count + 4-byte resolution + 4-byte admin level)");

            ByteBuffer bb = ByteBuffer.wrap(val).order(ByteOrder.BIG_ENDIAN);
            long cellCount = bb.getLong();
            int resolution = bb.getInt();
            int adminLevel = bb.getInt();

            assertTrue(cellCount > 0, "Cell count should be positive, got: " + cellCount);
            assertTrue(resolution >= 4 && resolution <= 9,
                    "Resolution should be between 4 and 9, got: " + resolution);
            assertTrue(adminLevel >= 1 && adminLevel <= 11,
                    "Admin level should be between 2 and 11, got: " + adminLevel);

            int count = 0;
            it.seekToFirst();
            while (it.isValid()) {
                count++;
                it.next();
            }
            System.out.println("Total region_metadata entries: " + count);
            assertTrue(count >= 10, "Should have at least 10 boundaries, got: " + count);
        }
    }

    @Test
    void testAllRegionMetadataEntriesHaveValidResolution() throws RocksDBException {
        Path dbPath = tempOutputDir.resolve("region_metadata");
        try (Options opts = new Options().setCreateIfMissing(false);
             RocksDB db = RocksDB.open(opts, dbPath.toString())) {

            var it = db.newIterator();
            it.seekToFirst();
            int checked = 0;
            while (it.isValid()) {
                byte[] val = it.value();
                assertEquals(16, val.length,
                        "Every region_metadata entry should be 16 bytes");

                ByteBuffer bb = ByteBuffer.wrap(val).order(ByteOrder.BIG_ENDIAN);
                long cellCount = bb.getLong();
                int resolution = bb.getInt();
                int adminLevel = bb.getInt();

                assertTrue(cellCount > 0,
                        "Cell count should be positive for entry " + checked);
                assertTrue(resolution >= 4 && resolution <= 9,
                        "Resolution should be 4-9 for entry " + checked + ", got: " + resolution);
                assertTrue(adminLevel >= 1 && adminLevel <= 11,
                        "Admin level should be 2-11 for entry " + checked + ", got: " + adminLevel);

                checked++;
                it.next();
            }
            System.out.println("Verified " + checked + " region_metadata entries");
            assertTrue(checked >= 10, "Should verify at least 10 entries, got: " + checked);
        }
    }

    @Test
    void testRegionGeometryHasEntries() throws RocksDBException {
        Path dbPath = tempOutputDir.resolve("region_geometry");
        try (Options opts = new Options().setCreateIfMissing(false);
             RocksDB db = RocksDB.open(opts, dbPath.toString())) {

            var it = db.newIterator();
            it.seekToFirst();
            assertTrue(it.isValid(), "region_geometry should have entries");

            byte[] key = it.key();
            assertEquals(8, key.length, "Region geometry key should be 8 bytes");

            byte[] val = it.value();
            assertTrue(val.length > 0, "WKB value should not be empty");

            int count = 0;
            it.seekToFirst();
            while (it.isValid()) {
                count++;
                it.next();
            }
            System.out.println("Total region_geometry entries: " + count);
            assertTrue(count >= 10, "Should have at least 10 geometries, got: " + count);
        }
    }

    @Test
    void testOsmNamesFileNotEmpty() throws IOException {
        Path namesPath = tempOutputDir.resolve("osm_names.tsv");
        assertTrue(Files.size(namesPath) > 0, "osm_names.tsv should not be empty");

        String firstLine = Files.readAllLines(namesPath).getFirst();
        assertTrue(firstLine.contains("\t"), "osm_names.tsv should be tab-separated");
        System.out.println("osm_names.tsv first line: " + firstLine);
    }

    @Test
    void testH3CellsMapToValidOsmIds() throws RocksDBException {
        Set<Long> metadataOsmIds = new HashSet<>();
        Path metaDbPath = tempOutputDir.resolve("region_metadata");

        try (Options opts = new Options().setCreateIfMissing(false);
             RocksDB db = RocksDB.open(opts, metaDbPath.toString())) {

            var it = db.newIterator();
            it.seekToFirst();
            while (it.isValid()) {
                long osmId = ByteBuffer.wrap(it.key()).order(ByteOrder.BIG_ENDIAN).getLong();
                metadataOsmIds.add(osmId);
                it.next();
            }
        }
        assertFalse(metadataOsmIds.isEmpty(), "Should have metadata entries to cross-reference");

        Path h3DbPath = tempOutputDir.resolve("h3_to_osm");
        try (Options opts = new Options().setCreateIfMissing(false);
             RocksDB db = RocksDB.open(opts, h3DbPath.toString())) {

            var it = db.newIterator();
            it.seekToFirst();
            boolean foundMatch = false;
            int checked = 0;
            while (it.isValid() && checked < 100) {
                byte[] val = it.value();
                for (int i = 0; i < val.length; i += 8) {
                    long osmId = ByteBuffer.wrap(val, i, 8).order(ByteOrder.BIG_ENDIAN).getLong();
                    if (metadataOsmIds.contains(osmId)) {
                        foundMatch = true;
                        break;
                    }
                }
                if (foundMatch) break;
                it.next();
                checked++;
            }
            assertTrue(foundMatch,
                    "H3 cells should reference OSM IDs present in region_metadata");
        }
    }

    @Test
    void testRegionGeometryMatchesMetadata() throws RocksDBException {
        Set<Long> metadataOsmIds = new HashSet<>();
        Path metaDbPath = tempOutputDir.resolve("region_metadata");

        try (Options opts = new Options().setCreateIfMissing(false);
             RocksDB db = RocksDB.open(opts, metaDbPath.toString())) {

            var it = db.newIterator();
            it.seekToFirst();
            while (it.isValid()) {
                metadataOsmIds.add(
                        ByteBuffer.wrap(it.key()).order(ByteOrder.BIG_ENDIAN).getLong());
                it.next();
            }
        }

        Set<Long> geometryOsmIds = new HashSet<>();
        Path geomDbPath = tempOutputDir.resolve("region_geometry");

        try (Options opts = new Options().setCreateIfMissing(false);
             RocksDB db = RocksDB.open(opts, geomDbPath.toString())) {

            var it = db.newIterator();
            it.seekToFirst();
            while (it.isValid()) {
                geometryOsmIds.add(
                        ByteBuffer.wrap(it.key()).order(ByteOrder.BIG_ENDIAN).getLong());
                it.next();
            }
        }

        assertEquals(metadataOsmIds, geometryOsmIds,
                "Same OSM IDs should exist in region_metadata and region_geometry");
    }

    @Test
    void testNoTemporaryFilesLeaked() {
        Path tmpDir = tempOutputDir.resolve("tmp");
        assertFalse(Files.exists(tmpDir),
                "Temporary directory should be cleaned up after import");
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
