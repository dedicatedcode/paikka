package com.dedicatedcode.paikka.service;

import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2LatLng;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;

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

    /**
     * Helper to get S2CellId object if needed for library calls.
     */
    public S2CellId getCellId(double lat, double lon, int level) {
        return S2CellId.fromLatLng(S2LatLng.fromDegrees(lat, lon)).parent(level);
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
}