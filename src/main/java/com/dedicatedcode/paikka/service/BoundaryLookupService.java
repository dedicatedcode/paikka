package com.dedicatedcode.paikka.service;

import com.dedicatedcode.paikka.config.PaikkaConfiguration;
import com.uber.h3core.H3Core;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import jakarta.annotation.PreDestroy;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Service
public class BoundaryLookupService {
    private static final Logger logger = LoggerFactory.getLogger(BoundaryLookupService.class);

    private final H3Core h3;
    private final RocksDB h3ToOsmDb;
    private final RocksDB regionMetadataDb;

    public BoundaryLookupService(PaikkaConfiguration paikkaConfiguration) throws Exception {
        this.h3 = H3Core.newInstance();
        RocksDB.loadLibrary();

        // Assuming the databases are stored in a 'boundaries' folder within the data directory.
        // Adjust this path if your importer outputs to a different location.
        Path dataDir = Paths.get(paikkaConfiguration.getDataDir());
        Path h3ToOsmPath = dataDir.resolve("h3_to_osm");
        Path regionMetaPath = dataDir.resolve("region_metadata");

        Options options = new Options().setReadOnly(true);
        
        logger.info("Opening RocksDB databases for boundary lookup...");
        this.h3ToOsmDb = RocksDB.open(options, h3ToOsmPath.toString());
        this.regionMetadataDb = RocksDB.open(options, regionMetaPath.toString());
        logger.info("RocksDB databases opened successfully.");
    }

    /**
     * Looks up the administrative boundaries for a given coordinate.
     * Returns a list of boundaries (OSM ID and total cell count) that contain this point.
     */
    public List<BoundaryInfo> lookup(double lat, double lng) {
        Set<Long> osmIds = new HashSet<>();

        // The importer uses different resolutions based on admin level.
        // We must query all three to get the full hierarchy (City, State, Country).
        
        // Resolution 9 (Districts/Cities - Admin Level >= 7)
        long cellRes9 = h3.latLngToCell(lat, lng, 9);
        osmIds.addAll(getOsmIdsForCell(cellRes9));

        // Resolution 6 (States/Regions - Admin Level 3-6)
        long cellRes6 = h3.latLngToCell(lat, lng, 6);
        osmIds.addAll(getOsmIdsForCell(cellRes6));

        // Resolution 4 (Countries/Continents - Admin Level <= 2)
        long cellRes4 = h3.latLngToCell(lat, lng, 4);
        osmIds.addAll(getOsmIdsForCell(cellRes4));

        List<BoundaryInfo> results = new ArrayList<>();
        for (Long osmId : osmIds) {
            int totalCells = getTotalCells(osmId);
            results.add(new BoundaryInfo(osmId, totalCells));
        }

        return results;
    }

    private Set<Long> getOsmIdsForCell(long cellId) {
        Set<Long> ids = new HashSet<>();
        try {
            byte[] val = h3ToOsmDb.get(longToBytes(cellId));
            if (val != null) {
                ByteBuffer bb = ByteBuffer.wrap(val).order(ByteOrder.BIG_ENDIAN);
                while (bb.hasRemaining()) {
                    ids.add(bb.getLong());
                }
            }
        } catch (RocksDBException e) {
            logger.error("Failed to lookup H3 cell {} in h3_to_osm", cellId, e);
        }
        return ids;
    }

    private int getTotalCells(long osmId) {
        try {
            byte[] val = regionMetadataDb.get(longToBytes(osmId));
            if (val != null && val.length == 4) {
                return ByteBuffer.wrap(val).order(ByteOrder.BIG_ENDIAN).getInt();
            }
        } catch (RocksDBException e) {
            logger.error("Failed to lookup metadata for OSM ID {} in region_metadata", osmId, e);
        }
        return -1; // Indicates unknown total
    }

    private byte[] longToBytes(long v) {
        return ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(v).array();
    }

    @PreDestroy
    public void cleanup() {
        if (this.h3ToOsmDb != null) {
            this.h3ToOsmDb.close();
        }
        if (this.regionMetadataDb != null) {
            this.regionMetadataDb.close();
        }
    }

    /**
     * Represents a boundary containing the queried point.
     * Reitti can use the totalCells to calculate the percentage visited.
     */
    public record BoundaryInfo(long osmId, int totalCells) {}
}
