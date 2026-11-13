/*
 *  This file is part of paikka.
 *
 *  Paikka is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  Paikka is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with Paikka. If not, see <https://www.gnu.org/licenses/>.
 */

package com.dedicatedcode.paikka.service.importer;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTReader;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Relation 943080 (Gutsbezirk Kaufunger Wald) has 40 outer and 52 inner ways.
 * Its polygonized outer shell is invalid at one node, which previously made the
 * outer.difference(inner) overlay fail with TopologyException and dropped the
 * whole relation. The assembler must repair the polygon before the overlay.
 */
class StandaloneBoundaryImporterTopologyTest {

    @Test
    void repairsInvalidPolygonizedOuterBeforeDifference() throws Exception {
        List<Geometry> outer = readLines("relation-943080-outer.wkt");
        List<Geometry> inner = readLines("relation-943080-inner.wkt");

        Geometry result = StandaloneBoundaryImporter.assemble(outer, inner);

        assertNotNull(result, "The relation must produce a usable geometry");
        assertTrue(result.isValid(), "The assembled geometry must be valid");
        assertTrue(result.getArea() > 0, "The assembled geometry must have area");
    }

    private static List<Geometry> readLines(String resource) throws Exception {
        List<Geometry> lines = new ArrayList<>();
        try (InputStream is = StandaloneBoundaryImporterTopologyTest.class
                .getClassLoader().getResourceAsStream(resource)) {
            assertNotNull(is, resource + " not found");
            WKTReader reader = new WKTReader();
            try (BufferedReader buffered = new BufferedReader(new InputStreamReader(is))) {
                String line;
                while ((line = buffered.readLine()) != null) {
                    if (!line.isBlank()) {
                        lines.add(reader.read(line));
                    }
                }
            }
        }
        return lines;
    }
}
