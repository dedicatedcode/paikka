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

import com.dedicatedcode.paikka.flatbuffers.Boundary;
import com.github.benmanes.caffeine.cache.Cache;
import org.locationtech.jts.algorithm.locate.IndexedPointInAreaLocator;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Location;
import org.locationtech.jts.io.WKBReader;
import org.rocksdb.RocksDB;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class HierarchyCache {
    private final RocksDB boundariesDb;
    private final RocksDB gridIndexDb;
    private final S2Helper s2Helper;
    private final Cache<Long, CachedBoundary> globalCache;
    private final WKBReader wkbReader = new WKBReader();

    private long lastS2CellId = -1;
    private List<SimpleHierarchyItem> lastHierarchy;
    private boolean lastCellFullyContained = false;

    public HierarchyCache(RocksDB boundariesDb, RocksDB gridIndexDb, S2Helper s2Helper, Cache<Long, CachedBoundary> globalCache) {
        this.boundariesDb = boundariesDb;
        this.gridIndexDb = gridIndexDb;
        this.s2Helper = s2Helper;
        this.globalCache = globalCache;
    }

    public List<SimpleHierarchyItem> resolve(Double lon, Double lat) {
        long currentS2Cell = s2Helper.getS2CellId(lon, lat, S2Helper.GRID_LEVEL);

        if (currentS2Cell == lastS2CellId && lastHierarchy != null && lastCellFullyContained) {
            return lastHierarchy;
        }

        lastHierarchy = refresh(lon, lat, currentS2Cell);
        lastS2CellId = currentS2Cell;
        return lastHierarchy;
    }

    private List<SimpleHierarchyItem> refresh(double lon, double lat, long cellId) {
        long[] candidates = fetchGridCandidates(cellId);
        List<CachedBoundary> lastActiveBoundaries = new ArrayList<>(); // Reset this list

        Envelope cellEnvelope = s2Helper.getCellEnvelope(cellId);
        boolean allLayersContainCell = true;

        if (candidates != null) {
            for (long id : candidates) {
                CachedBoundary cb = globalCache.get(id, this::fetchFromDb);

                if (cb != null && cb.contains(lon, lat)) {
                    lastActiveBoundaries.add(cb); // Store for Semi-Fast Path

                    // Keep the MIR check for the Ultra-Fast path
                    if (cb.mir == null || !cb.mir.contains(cellEnvelope)) {
                        allLayersContainCell = false;
                    }
                } else {
                    allLayersContainCell = false;
                }
            }
        }

        this.lastCellFullyContained = !lastActiveBoundaries.isEmpty() && allLayersContainCell;

        return lastActiveBoundaries.stream()
                .sorted(Comparator.comparingInt(b -> b.level))
                .map(cb -> new SimpleHierarchyItem(cb.level, "administrative", cb.name, cb.osmId))
                .toList();
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

    public record CachedBoundary(int level, String name, long osmId, Envelope mir, Envelope mbr,
                                 IndexedPointInAreaLocator locator) {
        public boolean contains(double lon, double lat) {
            if (mir != null && mir.contains(lon, lat)) return true;
            if (!mbr.contains(lon, lat)) return false;
            return locator.locate(new Coordinate(lon, lat)) != Location.EXTERIOR;
        }
    }

    public record SimpleHierarchyItem(int level, String type, String name, long osmId) {}
}