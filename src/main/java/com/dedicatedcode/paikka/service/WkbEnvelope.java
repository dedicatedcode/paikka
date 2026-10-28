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
 *  You should have received a copy of the GNU Affero General Public License
 *  along with Paikka. If not, see <https://www.gnu.org/licenses/>.
 */

package com.dedicatedcode.paikka.service;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Reads the bounding box of a 2D WKB geometry without constructing JTS objects.
 * Only polygon-based geometries are supported; callers must fall back to JTS
 * when this returns false.
 */
public final class WkbEnvelope {

    private static final int MAX_NESTING_DEPTH = 8;
    private static final int POINT_TYPE = 1;
    private static final int LINE_STRING_TYPE = 2;
    private static final int POLYGON_TYPE = 3;
    private static final int MULTI_POINT_TYPE = 4;
    private static final int MULTI_LINE_STRING_TYPE = 5;
    private static final int MULTI_POLYGON_TYPE = 6;
    private static final int GEOMETRY_COLLECTION_TYPE = 7;

    private WkbEnvelope() {
    }

    /**
     * Reads the envelope into {@code target} at {@code offset} as
     * [minX, minY, maxX, maxY].
     *
     * @return true when the geometry was understood and contained at least one coordinate
     */
    public static boolean read(ByteBuffer source, double[] target, int offset) {
        if (source == null || target == null || offset < 0 || offset + 4 > target.length) {
            return false;
        }

        Accumulator accumulator = new Accumulator();
        try {
            ByteBuffer buffer = source.duplicate();
            if (!readGeometry(buffer, accumulator, 0) || !accumulator.hasValue) {
                return false;
            }
        } catch (BufferUnderflowException | IllegalArgumentException e) {
            return false;
        }

        target[offset] = accumulator.minX;
        target[offset + 1] = accumulator.minY;
        target[offset + 2] = accumulator.maxX;
        target[offset + 3] = accumulator.maxY;
        return true;
    }

    private static boolean readGeometry(ByteBuffer buffer, Accumulator accumulator, int depth) {
        if (depth > MAX_NESTING_DEPTH || buffer.remaining() < 5) {
            return false;
        }

        byte byteOrder = buffer.get();
        if (byteOrder != 0 && byteOrder != 1) {
            return false;
        }
        buffer.order(byteOrder == 1 ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN);

        int rawType = buffer.getInt();
        if (rawType < 0 || rawType >= 1000 || (rawType & 0xE0000000) != 0) {
            // ISO WKB with Z/M dimensions or EWKB flags. Not produced by the importer;
            // let the caller fall back to JTS for these.
            return false;
        }

        return switch (rawType) {
            case POINT_TYPE -> readPoint(buffer, accumulator);
            case LINE_STRING_TYPE -> readLineString(buffer, accumulator);
            case POLYGON_TYPE -> readPolygon(buffer, accumulator);
            case MULTI_POINT_TYPE,
                 MULTI_LINE_STRING_TYPE,
                 MULTI_POLYGON_TYPE,
                 GEOMETRY_COLLECTION_TYPE -> readCollection(buffer, accumulator, depth);
            default -> false;
        };
    }

    private static boolean readPoint(ByteBuffer buffer, Accumulator accumulator) {
        if (buffer.remaining() < 16) {
            return false;
        }
        accumulator.add(buffer.getDouble(), buffer.getDouble());
        return true;
    }

    private static boolean readLineString(ByteBuffer buffer, Accumulator accumulator) {
        if (buffer.remaining() < 4) {
            return false;
        }
        int pointCount = buffer.getInt();
        if (pointCount < 0 || buffer.remaining() < (long) pointCount * 16) {
            return false;
        }
        for (int i = 0; i < pointCount; i++) {
            accumulator.add(buffer.getDouble(), buffer.getDouble());
        }
        return true;
    }

    private static boolean readPolygon(ByteBuffer buffer, Accumulator accumulator) {
        if (buffer.remaining() < 4) {
            return false;
        }
        int ringCount = buffer.getInt();
        if (ringCount < 0) {
            return false;
        }
        for (int ring = 0; ring < ringCount; ring++) {
            if (buffer.remaining() < 4) {
                return false;
            }
            int pointCount = buffer.getInt();
            if (pointCount < 0 || buffer.remaining() < (long) pointCount * 16) {
                return false;
            }
            for (int point = 0; point < pointCount; point++) {
                accumulator.add(buffer.getDouble(), buffer.getDouble());
            }
        }
        return true;
    }

    private static boolean readCollection(ByteBuffer buffer, Accumulator accumulator, int depth) {
        if (buffer.remaining() < 4) {
            return false;
        }
        int geometryCount = buffer.getInt();
        if (geometryCount < 0) {
            return false;
        }
        for (int i = 0; i < geometryCount; i++) {
            if (!readGeometry(buffer, accumulator, depth + 1)) {
                return false;
            }
        }
        return true;
    }

    private static final class Accumulator {
        private boolean hasValue;
        private double minX;
        private double minY;
        private double maxX;
        private double maxY;

        private void add(double x, double y) {
            if (!hasValue) {
                hasValue = true;
                minX = maxX = x;
                minY = maxY = y;
                return;
            }
            if (x < minX) {
                minX = x;
            } else if (x > maxX) {
                maxX = x;
            }
            if (y < minY) {
                minY = y;
            } else if (y > maxY) {
                maxY = y;
            }
        }
    }
}
