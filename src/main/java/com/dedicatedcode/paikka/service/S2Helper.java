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

import com.google.common.geometry.S2Cell;
import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2LatLng;
import org.locationtech.jts.geom.Envelope;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.*;

/**
 * Optimized S2 Geometry helper for Planet-scale spatial indexing.
 */
@Service
public class S2Helper {

    private static final int SHARD_LEVEL = 14;   // For POIs
    public static final int GRID_LEVEL = 12;    // For Boundary Indexing

    /**
     * Get S2 cell ID for POI sharding.
     */
    public long getShardId(double lat, double lon) {
        return S2CellId.fromLatLng(S2LatLng.fromDegrees(lat, lon)).parent(SHARD_LEVEL).id();
    }

    /**
     * Returns the long ID for a specific coordinate and level.
     * Used by HierarchyCache to identify the 3km grid cell.
     */
    public long getS2CellId(double lon, double lat, int level) {
        // Note: OSM/JTS uses (Lon, Lat/X, Y). S2 uses (Lat, Lon).
        // Be careful with the order here to ensure consistency.
        return S2CellId.fromLatLng(S2LatLng.fromDegrees(lat, lon)).parent(level).id();
    }

    // --- ROCKSDB CONVERSIONS ---

    public byte[] longToByteArray(long value) {
        return ByteBuffer.allocate(Long.BYTES).putLong(value).array();
    }

    public long byteArrayToLong(byte[] bytes) {
        if (bytes == null || bytes.length < Long.BYTES) return -1;
        return ByteBuffer.wrap(bytes).getLong();
    }

    /**
     * Converts a long array (list of OsmIds) to a byte array for RocksDB storage.
     * This is essential for the GridIndex [CellID -> List<OsmID>].
     */
    public byte[] longArrayToByteArray(long[] values) {
        ByteBuffer buffer = ByteBuffer.allocate(values.length * Long.BYTES);
        buffer.asLongBuffer().put(values);
        return buffer.array();
    }

    /**
     * Converts bytes from RocksDB back into a long array.
     * This array will be used by the SIMD vector API in the Cache.
     */
    public long[] byteArrayToLongArray(byte[] bytes) {
        if (bytes == null) return new long[0];
        LongBuffer lb = ByteBuffer.wrap(bytes).asLongBuffer();
        long[] res = new long[lb.capacity()];
        lb.get(res);
        return res;
    }

    /**
     * Get neighboring S2 cell IDs at the same level as the given shard.
     * This returns the 8 adjacent cells (north, south, east, west, and 4 diagonals).
     */
    public Set<Long> getNeighborShards(long shardId) {
        S2CellId cellId = new S2CellId(shardId);
        List<S2CellId> allNeighbours = new ArrayList<>();
        cellId.getAllNeighbors(SHARD_LEVEL, allNeighbours);
        return new HashSet<>(allNeighbours.stream().map(S2CellId::id).toList());
    }

    /**
     * Get expanding rings of neighboring S2 cell IDs around the given shard.
     * Returns shards in rings of increasing distance from the center.
     * 
     * @param centerShardId The center shard ID
     * @param maxRings Maximum number of rings to expand (1 = immediate neighbors only)
     * @return Map where key is ring number (1, 2, 3...) and value is set of shard IDs in that ring
     */
    public Map<Integer, Set<Long>> getExpandingNeighborRings(long centerShardId, int maxRings) {
        Map<Integer, Set<Long>> rings = new HashMap<>();
        Set<Long> visited = new HashSet<>();
        visited.add(centerShardId);
        
        Set<Long> currentRing = Set.of(centerShardId);
        
        for (int ring = 1; ring <= maxRings; ring++) {
            Set<Long> nextRing = new HashSet<>();
            
            // For each shard in current ring, get its neighbors
            for (long shardId : currentRing) {
                Set<Long> neighbors = getNeighborShards(shardId);
                for (long neighbor : neighbors) {
                    if (!visited.contains(neighbor)) {
                        nextRing.add(neighbor);
                        visited.add(neighbor);
                    }
                }
            }
            
            if (nextRing.isEmpty()) {
                break; // No more neighbors to expand to
            }
            
            rings.put(ring, nextRing);
            currentRing = nextRing;
        }
        
        return rings;
    }

    public Envelope getCellEnvelope(long cellId) {
        S2Cell cell = new S2Cell(new S2CellId(cellId));
        Envelope envelope = new Envelope();

        // Check all 4 corners of the S2 Cell to build the Bounding Box
        for (int i = 0; i < 4; i++) {
            S2LatLng latLng = new S2LatLng(cell.getVertex(i));
            envelope.expandToInclude(latLng.lngDegrees(), latLng.latDegrees());
        }
        return envelope;
    }
}
