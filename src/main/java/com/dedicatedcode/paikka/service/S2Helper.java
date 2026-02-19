package com.dedicatedcode.paikka.service;

import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2LatLng;
import org.springframework.stereotype.Service;

/**
 * Helper service for S2 geometry operations.
 * Handles spatial indexing using S2 cells at level 14 for POI sharding.
 */
@Service
public class S2Helper {
    
    private static final int SHARD_LEVEL = 14;
    
    /**
     * Get the S2 cell ID at level 14 for the given coordinates.
     * This is used as the shard key for storing POIs.
     * 
     * @param lat Latitude in degrees
     * @param lon Longitude in degrees
     * @return S2 cell ID as long
     */
    public long getShardId(double lat, double lon) {
        S2LatLng latLng = S2LatLng.fromDegrees(lat, lon);
        S2CellId cellId = S2CellId.fromLatLng(latLng);
        return cellId.parent(SHARD_LEVEL).id();
    }
    
    /**
     * Get the S2 cell ID for a point.
     * 
     * @param lat Latitude in degrees
     * @param lon Longitude in degrees
     * @return S2CellId object
     */
    public S2CellId getCellId(double lat, double lon) {
        S2LatLng latLng = S2LatLng.fromDegrees(lat, lon);
        return S2CellId.fromLatLng(latLng).parent(SHARD_LEVEL);
    }

    public S2CellId getCellId(double lat, double lon, int level) {
        S2LatLng latLng = S2LatLng.fromDegrees(lat, lon);
        return S2CellId.fromLatLng(latLng).parent(level);
    }

    /**
     * Convert long to byte array for RocksDB key.
     * 
     * @param value Long value to convert
     * @return Byte array representation
     */
    public byte[] longToByteArray(long value) {
        return new byte[] {
            (byte) (value >>> 56),
            (byte) (value >>> 48),
            (byte) (value >>> 40),
            (byte) (value >>> 32),
            (byte) (value >>> 24),
            (byte) (value >>> 16),
            (byte) (value >>> 8),
            (byte) value
        };
    }
    
    /**
     * Convert byte array back to long for RocksDB key.
     * 
     * @param bytes Byte array to convert
     * @return Long value
     */
    public long byteArrayToLong(byte[] bytes) {
        return ((long) bytes[0] << 56) |
               ((long) (bytes[1] & 0xFF) << 48) |
               ((long) (bytes[2] & 0xFF) << 40) |
               ((long) (bytes[3] & 0xFF) << 32) |
               ((long) (bytes[4] & 0xFF) << 24) |
               ((long) (bytes[5] & 0xFF) << 16) |
               ((long) (bytes[6] & 0xFF) << 8) |
               ((long) (bytes[7] & 0xFF));
    }
}
