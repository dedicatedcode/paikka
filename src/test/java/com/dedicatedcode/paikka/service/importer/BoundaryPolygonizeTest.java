/*
 *  This file is part of paikka.
 *
 *  Paikka is free software: you can redistribute it and/or
 *  modify it under the terms of the GNU Affero General Public License
 *  as published by the Free Software Foundation, either version 3 of the
 *  License, or any later version.
 *
 *  Paikka is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License
 * along with Paikka. If not, see <https://www.gnu.org/licenses/>.
 */

package com.dedicatedcode.paikka.service.importer;

import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.*;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class BoundaryPolygonizeTest {

    private static final GeometryFactory GF = new GeometryFactory();

    private static LineString line(double... xy) {
        Coordinate[] cs = new Coordinate[xy.length / 2];
        for (int i = 0; i < cs.length; i++) {
            cs[i] = new Coordinate(xy[2 * i], xy[2 * i + 1]);
        }
        return GF.createLineString(cs);
    }

    /**
     * Two squares that touch at exactly one corner must stay two polygons. Greedy way stitching
     * merged them into a single self-intersecting ring, which the polygonizer then discarded
     * (this is what made e.g. Austria lose its geometry).
     */
    private static List<Geometry> outerLines(List<Geometry> all) {
        return all.subList(0, 4);
    }

    private static List<Geometry> innerLines(List<Geometry> all) {
        return all.subList(4, all.size());
    }

    @Test
    void keepsRingsThatTouchAtASinglePointSeparate() {
        List<Geometry> lines = new ArrayList<>();
        // square A: (0,0)-(1,0)-(1,1)-(0,1), split into four ways
        lines.add(line(0, 0, 1, 0));
        lines.add(line(1, 0, 1, 1));
        lines.add(line(1, 1, 0, 1));
        lines.add(line(0, 1, 0, 0));
        // square B touching A at (1,1), also split into four ways
        lines.add(line(1, 1, 2, 1));
        lines.add(line(2, 1, 2, 2));
        lines.add(line(2, 2, 1, 2));
        lines.add(line(1, 2, 1, 1));

        Geometry result = StandaloneBoundaryImporter.assemble(lines, List.of());

        assertNotNull(result, "expected two polygons");
        assertEquals(2, result.getNumGeometries(), "point-touching rings must not be merged");
        assertEquals(2.0, result.getArea(), 1e-9);
        assertTrue(result.isValid(), "result must be a valid geometry");
    }

    @Test
    void assignsInnerRingsAsHoles() {
        List<Geometry> lines = new ArrayList<>();
        lines.add(line(0, 0, 4, 0));
        lines.add(line(4, 0, 4, 4));
        lines.add(line(4, 4, 0, 4));
        lines.add(line(0, 4, 0, 0));
        lines.add(line(1, 1, 3, 1));
        lines.add(line(3, 1, 3, 3));
        lines.add(line(3, 3, 1, 3));
        lines.add(line(1, 3, 1, 1));

        Geometry result = StandaloneBoundaryImporter.assemble(outerLines(lines), innerLines(lines));

        assertNotNull(result);
        assertEquals(1, result.getNumGeometries());
        Polygon polygon = (Polygon) result;
        assertEquals(1, polygon.getNumInteriorRing(), "inner ring must become a hole");
        assertEquals(16.0 - 4.0, polygon.getArea(), 1e-9);
    }

    @Test
    void returnsNullForOpenLines() {
        List<Geometry> lines = new ArrayList<>();
        lines.add(line(0, 0, 1, 1));
        lines.add(line(1, 1, 2, 0));

        assertNull(StandaloneBoundaryImporter.assemble(lines, List.of()));
    }
}
