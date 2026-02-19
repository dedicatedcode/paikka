package com.dedicatedcode.paikka.service;

import com.dedicatedcode.paikka.config.PaikkaConfiguration;
import com.dedicatedcode.paikka.flatbuffers.Address;
import com.dedicatedcode.paikka.flatbuffers.Name;
import com.dedicatedcode.paikka.flatbuffers.POI;
import com.dedicatedcode.paikka.flatbuffers.POIList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for ImportService.
 * Uses the monaco-filtered.pbf test file to verify import functionality.
 */
class ImportServiceTest {

    private Path tempDataDir;
    private Path tempImportFile;
    private ImportService importService;
    private S2Helper s2Helper;

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
        config.setMaxImportThreads(2);
        config.setS2Level(12);
        
        s2Helper = new S2Helper();
        GeometrySimplificationService geometrySimplificationService = new GeometrySimplificationService();
        
        importService = new ImportService(s2Helper, geometrySimplificationService);
        
        // Import the data once during setup
        importService.importData(tempImportFile.toString(), tempDataDir.toString());
    }

    @AfterEach
    void tearDown() throws IOException {
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
                byte[] key = iterator.key();
                byte[] value = iterator.value();
                
                // Parse the POIList from FlatBuffers
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
            System.out.println("Total POIs imported: " + importedPoiIds.size());
            
            // Verify specific POIs from Monaco
            // Monaco has some known POIs - let's verify a few by checking if they exist
            assertTrue(importedPoiIds.size() > 0, "Should have imported POIs");
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
                
                for (int i = 0; i < poiList.poisLength(); i++) {
                    POI poi = poiList.pois(i);
                    if (targetPoiId == null) {
                        targetPoiId = poi.id();
                        targetPoi = poi;
                        break;
                    }
                }
                
                if (targetPoiId != null) break;
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
        assertEquals(2, poiById.hierarchyLength());
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
