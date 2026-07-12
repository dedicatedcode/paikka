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
    private final RocksDB osmToH3Db;

    public BoundaryLookupService(PaikkaConfiguration paikkaConfiguration) throws Exception {
        this.h3 = H3Core.newInstance();
        RocksDB.loadLibrary();

        Path dataDir = Paths.get(paikkaConfiguration.getDataDir());
        Path h3ToOsmPath = dataDir.resolve("h3_to_osm");
        Path regionMetaPath = dataDir.resolve("region_metadata");
        Path osmToH3Path = dataDir.resolve("osm_to_h3");

        Options options = new Options();

        logger.info("Opening RocksDB databases for boundary lookup...");
        this.h3ToOsmDb = RocksDB.open(options, h3ToOsmPath.toString());
        this.regionMetadataDb = RocksDB.open(options, regionMetaPath.toString());
        this.osmToH3Db = RocksDB.open(options, osmToH3Path.toString());
        logger.info("RocksDB databases opened successfully.");
    }

    /**
     * Looks up the administrative boundaries for a given coordinate.
     * Returns a list of boundaries (OSM ID and total cell count) that contain this point.
     */
    public List<BoundaryInfo> lookup(double lat, double lng) {
        long cellRes9 = h3.latLngToCell(lat, lng, 9);
        Set<Long> osmIds = new HashSet<>(getOsmIdsForCell(cellRes9));

        long cellRes6 = h3.latLngToCell(lat, lng, 6);
        osmIds.addAll(getOsmIdsForCell(cellRes6));

        long cellRes4 = h3.latLngToCell(lat, lng, 4);
        osmIds.addAll(getOsmIdsForCell(cellRes4));

        List<BoundaryInfo> results = new ArrayList<>();
        for (Long osmId : osmIds) {
            int totalCells = getTotalCells(osmId);
            results.add(new BoundaryInfo(osmId, totalCells));
        }

        return results;
    }

    /**
     * Fetches the H3 cells for a specific lat,lon in all needed resolutions.
     */
    public Set<Long> getCellsForPoint(double lat, double lng) {
        Set<Long> cells = new HashSet<>();
        cells.add(h3.latLngToCell(lat, lng, 9));
        cells.add(h3.latLngToCell(lat, lng, 6));
        cells.add(h3.latLngToCell(lat, lng, 4));
        return cells;
    }

    /**
     * Fetches all H3 cells belonging to a specific boundary (OSM ID).
     * This can be used to compare visited cells against the total cells of a boundary.
     */
    public Set<Long> getCellsForBoundary(long osmId) {
        Set<Long> cells = new HashSet<>();
        try {
            byte[] val = osmToH3Db.get(longToBytes(osmId));
            if (val != null) {
                ByteBuffer bb = ByteBuffer.wrap(val).order(ByteOrder.BIG_ENDIAN);
                while (bb.hasRemaining()) {
                    cells.add(bb.getLong());
                }
            }
        } catch (RocksDBException e) {
            logger.error("Failed to lookup cells for OSM ID {} in osm_to_h3", osmId, e);
        }
        return cells;
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
        if (this.osmToH3Db != null) {
            this.osmToH3Db.close();
        }
    }

    /**
     * Represents a boundary containing the queried point.
     * Reitti can use the totalCells to calculate the percentage visited.
     */
    public record BoundaryInfo(long osmId, int totalCells) {}
}