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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.WKBWriter;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * The geometry fallback keeps regions that the tree could not assign but whose interior point is
 * inside a known country polygon.
 */
class RegionHierarchyCountryFallbackTest {

    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

    @TempDir
    Path tempDir;

    @Test
    void assignsRegionInsideCountryPolygon() throws Exception {
        long countryId = 1L;
        long regionId = 2L;

        try (Options options = new Options().setCreateIfMissing(true);
             RocksDB db = RocksDB.open(options, tempDir.toString())) {
            put(db, countryId, square(0, 10, 0, 10));
            put(db, regionId, square(1, 2, 1, 2));

            Map<Long, String> countries = new HashMap<>();
            int assigned = new RegionHierarchyWriter().assignCountriesByGeometry(
                    List.of(new RegionHierarchyWriter.RegionInfo(regionId, 6, 9, 1)),
                    countries,
                    db,
                    Map.of("XX", countryId));

            assertEquals(1, assigned);
            assertEquals("XX", countries.get(regionId));
        }
    }

    @Test
    void leavesRegionOutsideCountryPolygonUnassigned() throws Exception {
        long countryId = 1L;
        long regionId = 2L;

        try (Options options = new Options().setCreateIfMissing(true);
             RocksDB db = RocksDB.open(options, tempDir.toString())) {
            put(db, countryId, square(0, 10, 0, 10));
            put(db, regionId, square(20, 21, 20, 21));

            Map<Long, String> countries = new HashMap<>();
            int assigned = new RegionHierarchyWriter().assignCountriesByGeometry(
                    List.of(new RegionHierarchyWriter.RegionInfo(regionId, 6, 9, 1)),
                    countries,
                    db,
                    Map.of("XX", countryId));

            assertEquals(0, assigned);
            assertFalse(countries.containsKey(regionId));
        }
    }

    private static Geometry square(double minX, double maxX, double minY, double maxY) {
        return GEOMETRY_FACTORY.toGeometry(new Envelope(minX, maxX, minY, maxY));
    }

    private static void put(RocksDB db, long id, Geometry geometry) throws Exception {
        byte[] key = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(id).array();
        db.put(key, new WKBWriter().write(geometry));
    }
}
