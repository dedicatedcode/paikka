package com.dedicatedcode.paikka.service;

import com.dedicatedcode.paikka.flatbuffers.Boundary;
import org.locationtech.jts.algorithm.locate.IndexedPointInAreaLocator;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Location;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.WKBReader;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;

import java.nio.ByteBuffer;
import java.util.*;

public class HierarchyCache {

    private final RocksDB boundariesDb;
    private final RocksDB gridIndexDb;
    private final S2Helper s2Helper;
    private final WKBReader wkbReader = new WKBReader();

    private long lastS2CellId = -1;
    private final Map<Integer, List<CachedBoundary>> levelCache = new HashMap<>();
    private List<SimpleHierarchyItem> lastHierarchy;

    public HierarchyCache(RocksDB boundariesDb, RocksDB gridIndexDb, S2Helper s2Helper) {
        this.boundariesDb = boundariesDb;
        this.gridIndexDb = gridIndexDb;
        this.s2Helper = s2Helper;
    }

    public List<SimpleHierarchyItem> resolve(Point point) {
        double lon = point.getX();
        double lat = point.getY();
        long currentS2Cell = s2Helper.getS2CellId(lon, lat, S2Helper.GRID_LEVEL);

        // If we changed S2 cells, we MUST refresh
        if (currentS2Cell == lastS2CellId && lastHierarchy != null) {
            // Optimization: If the point is still inside ALL layers of the previous
            // result, we can return the cached list immediately.
            if (layersStillValid(lon, lat)) {
                return lastHierarchy;
            }
        }
        lastHierarchy = refresh(lon, lat, currentS2Cell);
        return lastHierarchy;
    }
    private List<SimpleHierarchyItem> refresh(double lon, double lat, long cellId) {
        long[] candidates = fetchGridCandidates(cellId);

        // Start with a fresh temporary state for this specific coordinate
        Map<Integer, List<CachedBoundary>> nextCache = new HashMap<>();

        if (candidates != null) {
            for (long id : candidates) {
                // Reuse prepared geometry if we already have it in the old cache
                CachedBoundary cb = findInExistingCache(id);
                if (cb == null) {
                    cb = fetchFromDb(id);
                }

                // Point-in-Polygon check
                if (cb != null && cb.contains(lon, lat)) {
                    nextCache.computeIfAbsent(cb.level, k -> new ArrayList<>()).add(cb);
                }
            }
        }

        // Atomic update of the cache state
        levelCache.clear();
        levelCache.putAll(nextCache);

        return extractInfo();
    }

    private boolean layersStillValid(double lon, double lat) {
        if (levelCache.isEmpty()) return false;
        // All currently cached layers must still contain the point
        for (List<CachedBoundary> boundaries : levelCache.values()) {
            for (CachedBoundary cb : boundaries) {
                if (!cb.contains(lon, lat)) return false;
            }
        }
        return true;
    }

    private CachedBoundary findInExistingCache(long osmId) {
        return levelCache.values().stream()
                .flatMap(List::stream)
                .filter(cb -> cb.osmId == osmId)
                .findFirst()
                .orElse(null);
    }

    private CachedBoundary fetchFromDb(long id) {
        try {
            byte[] data = boundariesDb.get(s2Helper.longToByteArray(id));
            if (data == null) return null;
            Boundary b = Boundary.getRootAsBoundary(ByteBuffer.wrap(data));

            Envelope mir = b.mirMinX() != 0 ? new Envelope(b.mirMinX(), b.mirMaxX(), b.mirMinY(), b.mirMaxY()) : null;
            Envelope mbr = new Envelope(b.minX(), b.maxX(), b.minY(), b.maxY());

            ByteBuffer wkbBuf = b.geometry().dataAsByteBuffer();
            byte[] wkb = new byte[wkbBuf.remaining()];
            wkbBuf.get(wkb);

            IndexedPointInAreaLocator locator = new IndexedPointInAreaLocator(wkbReader.read(wkb));
            return new CachedBoundary(b.level(), b.name(), b.osmId(), mir, mbr, locator);
        } catch (Exception e) { return null; }
    }

    private long[] fetchGridCandidates(long cellId) {
        try {
            byte[] data = gridIndexDb.get(s2Helper.longToByteArray(cellId));
            return (data == null) ? null : s2Helper.byteArrayToLongArray(data);
        } catch (Exception e) { return null; }
    }

    private List<SimpleHierarchyItem> extractInfo() {
        return levelCache.entrySet().stream()
                .sorted(Map.Entry.comparingByKey()) // Sort by admin_level
                .flatMap(entry -> entry.getValue().stream()
                        .map(cb -> new SimpleHierarchyItem(cb.level, "administrative", cb.name, cb.osmId)))
                .toList();
    }

    private record CachedBoundary(int level, String name, long osmId, Envelope mir, Envelope mbr, IndexedPointInAreaLocator locator) {
        public boolean contains(double lon, double lat) {
            if (mir != null && mir.contains(lon, lat)) return true;
            if (!mbr.contains(lon, lat)) return false;
            return locator.locate(new Coordinate(lon, lat)) != Location.EXTERIOR;
        }
    }

    public record SimpleHierarchyItem(int level, String type, String name, long osmId) {}
}