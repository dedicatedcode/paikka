/*
 *  This file is part of paikka.
 *
 *  Paikka is free software: you can redistribute it and/or
 *  modify it under the terms of the GNU Affero General Public License
 *  as published by the Free Software Foundation, either version 3 of
 *  the License, or (at your option) any later version.
 *
 *  Paikka is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with Paikka.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.dedicatedcode.paikka.service.geometry;

import com.dedicatedcode.paikka.service.WkbEnvelope;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Encodes and decodes the geometry blobs stored in Paikka's FlatBuffers
 * {@code Geometry} tables.
 *
 * <p>Format 0 is standard WKB and remains the fallback for all unsupported
 * shapes. Format 1 is a compact single-ring polygon representation:</p>
 *
 * <pre>
 * int32 minX, minY, width, height   // little-endian, 1e-7 degrees
 * varint pointCount                // unique points, closing point omitted
 * repeat pointCount:
 *   varint dx, dy                  // relative to minX/minY
 * </pre>
 *
 * <p>OSM coordinates are on a 1e-7 degree grid, so this representation is
 * lossless for current OSM data.</p>
 */
public final class GeometryCodec {

    public static final byte FORMAT_WKB = 0;
    public static final byte FORMAT_COMPACT_POLYGON_V1 = 1;
    public static final String FORMAT_NAME = "compact-polygon-v1";

    private static final Logger logger = LoggerFactory.getLogger(GeometryCodec.class);
    private static final int SCALE = 10_000_000;
    private static final int MAX_COMPACT_POINTS = 10_000_000;
    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

    private GeometryCodec() {
    }

    /**
     * Encodes a geometry, preferring the compact polygon format when possible.
     */
    public static EncodedGeometry encode(Geometry geometry) {
        if (geometry == null || geometry.isEmpty()) {
            return null;
        }

        byte[] compact = encodeCompactPolygon(geometry);
        if (compact != null) {
            return new EncodedGeometry(FORMAT_COMPACT_POLYGON_V1, compact);
        }

        byte[] wkb = new WKBWriter().write(geometry);
        return new EncodedGeometry(FORMAT_WKB, wkb);
    }

    /**
     * Decodes geometry bytes according to their stored format.
     */
    public static Geometry decode(byte format, byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }
        try {
            return switch (format) {
                case FORMAT_WKB -> new WKBReader().read(data);
                case FORMAT_COMPACT_POLYGON_V1 -> decodeCompactPolygon(data);
                default -> {
                    logger.warn("Unsupported geometry format {}; ignoring geometry", format & 0xFF);
                    yield null;
                }
            };
        } catch (Exception e) {
            logger.warn("Failed to decode geometry (format {}): {}", format & 0xFF, e.getMessage());
            return null;
        }
    }

    /**
     * Reads only the envelope of a geometry. This is the fast path used by the
     * building shard index.
     */
    public static boolean readEnvelope(byte format, ByteBuffer source, double[] target, int offset) {
        if (source == null || target == null || offset < 0 || offset + 4 > target.length) {
            return false;
        }

        return switch (format) {
            case FORMAT_WKB -> WkbEnvelope.read(source, target, offset);
            case FORMAT_COMPACT_POLYGON_V1 -> readCompactEnvelope(source.duplicate(), target, offset);
            default -> false;
        };
    }

    private static byte[] encodeCompactPolygon(Geometry geometry) {
        if (!(geometry instanceof Polygon polygon)) {
            return null;
        }
        if (polygon.getNumInteriorRing() != 0 || polygon.getExteriorRing() == null) {
            return null;
        }

        Coordinate[] coordinates = polygon.getExteriorRing().getCoordinates();
        if (coordinates.length < 4) {
            return null;
        }

        int[] qx = new int[coordinates.length];
        int[] qy = new int[coordinates.length];
        for (int i = 0; i < coordinates.length; i++) {
            Coordinate coordinate = coordinates[i];
            if (coordinate == null
                    || !Double.isFinite(coordinate.x)
                    || !Double.isFinite(coordinate.y)) {
                return null;
            }
            long x = Math.round(coordinate.x * SCALE);
            long y = Math.round(coordinate.y * SCALE);
            if (x < Integer.MIN_VALUE || x > Integer.MAX_VALUE
                    || y < Integer.MIN_VALUE || y > Integer.MAX_VALUE) {
                return null;
            }
            qx[i] = (int) x;
            qy[i] = (int) y;
        }

        int pointCount = coordinates.length;
        if (qx[0] == qx[pointCount - 1] && qy[0] == qy[pointCount - 1]) {
            pointCount--;
        }

        int minX = Integer.MAX_VALUE;
        int minY = Integer.MAX_VALUE;
        int maxX = Integer.MIN_VALUE;
        int maxY = Integer.MIN_VALUE;
        for (int i = 0; i < pointCount; i++) {
            if (qx[i] < minX) minX = qx[i];
            if (qx[i] > maxX) maxX = qx[i];
            if (qy[i] < minY) minY = qy[i];
            if (qy[i] > maxY) maxY = qy[i];
        }

        long width = (long) maxX - minX;
        long height = (long) maxY - minY;
        if (width < 0 || width > Integer.MAX_VALUE || height < 0 || height > Integer.MAX_VALUE) {
            // Includes polygons spanning the antimeridian.
            return null;
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream(24 + pointCount * 4);
        putIntLE(out, minX);
        putIntLE(out, minY);
        putIntLE(out, (int) width);
        putIntLE(out, (int) height);
        writeVarint(out, pointCount);
        for (int i = 0; i < pointCount; i++) {
            writeVarint(out, qx[i] - minX);
            writeVarint(out, qy[i] - minY);
        }
        return out.toByteArray();
    }

    private static Geometry decodeCompactPolygon(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        if (buffer.remaining() < 16) {
            return null;
        }

        int minX = buffer.getInt();
        int minY = buffer.getInt();
        int width = buffer.getInt();
        int height = buffer.getInt();
        if (width < 0 || height < 0) {
            return null;
        }

        long pointCount = readVarint(buffer);
        if (pointCount < 3 || pointCount > MAX_COMPACT_POINTS || pointCount > buffer.remaining()) {
            return null;
        }

        Coordinate[] coordinates = new Coordinate[(int) pointCount + 1];
        for (int i = 0; i < pointCount; i++) {
            long dx = readVarint(buffer);
            long dy = readVarint(buffer);
            long x = (long) minX + dx;
            long y = (long) minY + dy;
            if (x < Integer.MIN_VALUE || x > Integer.MAX_VALUE
                    || y < Integer.MIN_VALUE || y > Integer.MAX_VALUE) {
                return null;
            }
            coordinates[i] = new Coordinate(x / (double) SCALE, y / (double) SCALE);
        }
        coordinates[coordinates.length - 1] = new Coordinate(coordinates[0]);

        LinearRing ring = GEOMETRY_FACTORY.createLinearRing(coordinates);
        return GEOMETRY_FACTORY.createPolygon(ring);
    }

    private static boolean readCompactEnvelope(ByteBuffer buffer, double[] target, int offset) {
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        if (buffer.remaining() < 16) {
            return false;
        }
        int minX = buffer.getInt();
        int minY = buffer.getInt();
        int width = buffer.getInt();
        int height = buffer.getInt();
        if (width < 0 || height < 0) {
            return false;
        }
        target[offset] = minX / (double) SCALE;
        target[offset + 1] = minY / (double) SCALE;
        target[offset + 2] = (minX + (long) width) / (double) SCALE;
        target[offset + 3] = (minY + (long) height) / (double) SCALE;
        return true;
    }

    private static void putIntLE(ByteArrayOutputStream out, int value) {
        out.write(value & 0xFF);
        out.write((value >>> 8) & 0xFF);
        out.write((value >>> 16) & 0xFF);
        out.write((value >>> 24) & 0xFF);
    }

    private static void writeVarint(ByteArrayOutputStream out, long value) {
        while ((value & ~0x7FL) != 0) {
            out.write((int) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        out.write((int) value);
    }

    private static long readVarint(ByteBuffer buffer) {
        long value = 0;
        int shift = 0;
        while (true) {
            if (!buffer.hasRemaining()) {
                throw new IllegalArgumentException("Truncated varint");
            }
            int b = buffer.get() & 0xFF;
            value |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return value;
            }
            shift += 7;
            if (shift >= 64) {
                throw new IllegalArgumentException("Varint too long");
            }
        }
    }
}
