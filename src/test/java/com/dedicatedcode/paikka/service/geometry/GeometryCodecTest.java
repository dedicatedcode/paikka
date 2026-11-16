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

import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

class GeometryCodecTest {

    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

    @Test
    void roundTripsCompactPolygonLosslesslyAtOsmPrecision() {
        Polygon polygon = rectangle(4.0001234, 52.0005678, 4.0019876, 52.0018765);

        EncodedGeometry encoded = GeometryCodec.encode(polygon);

        assertThat(encoded).isNotNull();
        assertThat(encoded.format()).isEqualTo(GeometryCodec.FORMAT_COMPACT_POLYGON_V1);
        Geometry decoded = GeometryCodec.decode(encoded.format(), encoded.data());
        assertThat(decoded).isInstanceOf(Polygon.class);
        assertCoordinatesEqual(polygon, decoded);
        assertThat(decoded.getArea()).isCloseTo(polygon.getArea(), within(1e-14));
    }

    @Test
    void readsCompactEnvelopeWithoutDecodingPoints() {
        Polygon polygon = rectangle(4.0001234, 52.0005678, 4.0019876, 52.0018765);
        EncodedGeometry encoded = GeometryCodec.encode(polygon);
        double[] envelope = new double[4];

        boolean read = GeometryCodec.readEnvelope(encoded.format(), ByteBuffer.wrap(encoded.data()), envelope, 0);

        assertThat(read).isTrue();
        assertThat(envelope[0]).isCloseTo(4.0001234, within(1e-12));
        assertThat(envelope[1]).isCloseTo(52.0005678, within(1e-12));
        assertThat(envelope[2]).isCloseTo(4.0019876, within(1e-12));
        assertThat(envelope[3]).isCloseTo(52.0018765, within(1e-12));
    }

    @Test
    void fallsBackToWkbForPolygonWithHole() {
        LinearRing shell = GEOMETRY_FACTORY.createLinearRing(new Coordinate[]{
                new Coordinate(4.0, 52.0),
                new Coordinate(4.1, 52.0),
                new Coordinate(4.1, 52.1),
                new Coordinate(4.0, 52.1),
                new Coordinate(4.0, 52.0)
        });
        LinearRing hole = GEOMETRY_FACTORY.createLinearRing(new Coordinate[]{
                new Coordinate(4.02, 52.02),
                new Coordinate(4.03, 52.02),
                new Coordinate(4.03, 52.03),
                new Coordinate(4.02, 52.03),
                new Coordinate(4.02, 52.02)
        });

        EncodedGeometry encoded = GeometryCodec.encode(GEOMETRY_FACTORY.createPolygon(shell, new LinearRing[]{hole}));

        assertThat(encoded.format()).isEqualTo(GeometryCodec.FORMAT_WKB);
        assertCoordinatesEqual(
                GEOMETRY_FACTORY.createPolygon(shell, new LinearRing[]{hole}),
                GeometryCodec.decode(encoded.format(), encoded.data()));
    }

    @Test
    void fallsBackToWkbForMultiPolygonAndAntimeridianPolygon() {
        Polygon first = rectangle(-179.0, -10.0, -178.0, -9.0);
        Polygon second = rectangle(179.0, 9.0, 180.0, 10.0);
        EncodedGeometry multi = GeometryCodec.encode(GEOMETRY_FACTORY.createMultiPolygon(new Polygon[]{first, second}));
        assertThat(multi.format()).isEqualTo(GeometryCodec.FORMAT_WKB);

        EncodedGeometry antimeridian = GeometryCodec.encode(rectangle(-179.0, -10.0, 179.0, 10.0));
        assertThat(antimeridian.format()).isEqualTo(GeometryCodec.FORMAT_WKB);
    }

    @Test
    void returnsNullForEmptyGeometry() {
        assertThat(GeometryCodec.encode(null)).isNull();
        assertThat(GeometryCodec.encode(GEOMETRY_FACTORY.createPolygon())).isNull();
    }

    private Polygon rectangle(double minX, double minY, double maxX, double maxY) {
        return GEOMETRY_FACTORY.createPolygon(new Coordinate[]{
                new Coordinate(minX, minY),
                new Coordinate(maxX, minY),
                new Coordinate(maxX, maxY),
                new Coordinate(minX, maxY),
                new Coordinate(minX, minY)
        });
    }

    private void assertCoordinatesEqual(Geometry expected, Geometry actual) {
        assertThat(actual).isNotNull();
        Coordinate[] expectedCoordinates = expected.getCoordinates();
        Coordinate[] actualCoordinates = actual.getCoordinates();
        assertThat(actualCoordinates).hasSameSizeAs(expectedCoordinates);
        for (int i = 0; i < expectedCoordinates.length; i++) {
            assertThat(actualCoordinates[i].x).isCloseTo(expectedCoordinates[i].x, within(1e-12));
            assertThat(actualCoordinates[i].y).isCloseTo(expectedCoordinates[i].y, within(1e-12));
        }
    }

    private static org.assertj.core.data.Offset<Double> within(double value) {
        return org.assertj.core.data.Offset.offset(value);
    }
}
