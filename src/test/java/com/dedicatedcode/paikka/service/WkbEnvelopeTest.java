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

import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ByteOrderValues;
import org.locationtech.jts.io.WKBWriter;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

class WkbEnvelopeTest {

    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

    @Test
    void readsPolygonWithHole() {
        assertEnvelopeMatches(polygonWithHole(), 2, ByteOrderValues.LITTLE_ENDIAN);
        assertEnvelopeMatches(polygonWithHole(), 2, ByteOrderValues.BIG_ENDIAN);
    }

    @Test
    void readsMultiPolygon() {
        Polygon first = polygon(new Coordinate(10, 20), new Coordinate(14, 20), new Coordinate(14, 24), new Coordinate(10, 24));
        Polygon second = polygon(new Coordinate(-5, -7), new Coordinate(-1, -7), new Coordinate(-1, -3), new Coordinate(-5, -3));
        Geometry multiPolygon = GEOMETRY_FACTORY.createMultiPolygon(new Polygon[]{first, second});

        assertEnvelopeMatches(multiPolygon, 2, ByteOrderValues.LITTLE_ENDIAN);
        assertEnvelopeMatches(multiPolygon, 2, ByteOrderValues.BIG_ENDIAN);
    }

    @Test
    void readsPointAndLineString() {
        assertEnvelopeMatches(GEOMETRY_FACTORY.createPoint(new Coordinate(1.5, -2.5)), 2, ByteOrderValues.LITTLE_ENDIAN);
        assertEnvelopeMatches(GEOMETRY_FACTORY.createLineString(new Coordinate[]{
                new Coordinate(-3, -4),
                new Coordinate(5, 6)
        }), 2, ByteOrderValues.LITTLE_ENDIAN);
    }

    @Test
    void rejectsThreeDimensionalWkb() {
        byte[] wkb = new WKBWriter(3).write(GEOMETRY_FACTORY.createPoint(new Coordinate(1, 2, 3)));
        double[] target = new double[4];

        assertThat(WkbEnvelope.read(ByteBuffer.wrap(wkb), target, 0)).isFalse();
    }

    @Test
    void rejectsTruncatedWkb() {
        byte[] wkb = new WKBWriter().write(polygonWithHole());
        byte[] truncated = new byte[wkb.length / 2];
        System.arraycopy(wkb, 0, truncated, 0, truncated.length);

        assertThat(WkbEnvelope.read(ByteBuffer.wrap(truncated), new double[4], 0)).isFalse();
    }

    @Test
    void writesEnvelopeAtRequestedOffset() {
        byte[] wkb = new WKBWriter().write(polygonWithHole());
        double[] target = new double[6];

        assertThat(WkbEnvelope.read(ByteBuffer.wrap(wkb), target, 1)).isTrue();
        assertThat(target[0]).isZero();
        assertThat(target[1]).isEqualTo(20);
        assertThat(target[2]).isEqualTo(20);
        assertThat(target[3]).isEqualTo(30);
        assertThat(target[4]).isEqualTo(30);
        assertThat(target[5]).isZero();
    }

    private void assertEnvelopeMatches(Geometry geometry, int outputDimension, int byteOrder) {
        byte[] wkb = new WKBWriter(outputDimension, byteOrder).write(geometry);
        double[] target = new double[4];

        assertThat(WkbEnvelope.read(ByteBuffer.wrap(wkb), target, 0))
                .as("envelope for %s", geometry.getGeometryType())
                .isTrue();

        var expected = geometry.getEnvelopeInternal();
        assertThat(target[0]).isEqualTo(expected.getMinX());
        assertThat(target[1]).isEqualTo(expected.getMinY());
        assertThat(target[2]).isEqualTo(expected.getMaxX());
        assertThat(target[3]).isEqualTo(expected.getMaxY());
    }

    private Polygon polygonWithHole() {
        LinearRing shell = GEOMETRY_FACTORY.createLinearRing(new Coordinate[]{
                new Coordinate(20, 30),
                new Coordinate(30, 30),
                new Coordinate(30, 20),
                new Coordinate(20, 20),
                new Coordinate(20, 30)
        });
        LinearRing hole = GEOMETRY_FACTORY.createLinearRing(new Coordinate[]{
                new Coordinate(23, 27),
                new Coordinate(27, 27),
                new Coordinate(27, 23),
                new Coordinate(23, 23),
                new Coordinate(23, 27)
        });
        return GEOMETRY_FACTORY.createPolygon(shell, new LinearRing[]{hole});
    }

    private Polygon polygon(Coordinate... coordinates) {
        Coordinate[] closed = new Coordinate[coordinates.length + 1];
        System.arraycopy(coordinates, 0, closed, 0, coordinates.length);
        closed[coordinates.length] = coordinates[0];
        return GEOMETRY_FACTORY.createPolygon(closed);
    }
}
