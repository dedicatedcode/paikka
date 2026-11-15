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
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *  See the GNU Affero General Public License for more details.
 *  You should have received a copy of the GNU Affero General Public License
 *  along with paikka. If not, see <https://www.gnu.org/licenses/>.
 */

package com.dedicatedcode.paikka.service.importer;

import com.dedicatedcode.paikka.config.PaikkaConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class StandaloneBoundaryImporterH3MergeTest {

    private static final ByteOrder ORDER = ByteOrder.BIG_ENDIAN;

    @TempDir
    Path tempDir;

    private final StandaloneBoundaryImporter importer = newImporter();

    @Test
    void mergesOverlappingKeysAndDeduplicatesOsmIds() throws Exception {
        Path first = tempDir.resolve("h3_0");
        Path second = tempDir.resolve("h3_1");
        Path third = tempDir.resolve("h3_2");
        Path missing = tempDir.resolve("h3_3");
        Path targetPath = tempDir.resolve("merged");

        createDb(first,
                 entry(1, 10, 20),
                 entry(2, 30),
                 entry(4, 40));
        createDb(second,
                 entry(1, 20, 50),
                 entry(3, 60),
                 entry(4, 40, 70));
        createDb(third,
                 entry(2, 30, 80),
                 entry(5, 90));

        try (Options options = new Options().setCreateIfMissing(true);
             RocksDB target = RocksDB.open(options, targetPath.toString())) {
            importer.mergeH3Dbs(List.of(first, second, third, missing), target,
                                new Options().setCreateIfMissing(false));

            assertEquals(List.of(10L, 20L, 50L), readOsmIds(target, 1));
            assertEquals(List.of(30L, 80L), readOsmIds(target, 2));
            assertEquals(List.of(60L), readOsmIds(target, 3));
            assertEquals(List.of(40L, 70L), readOsmIds(target, 4));
            assertEquals(List.of(90L), readOsmIds(target, 5));
            assertNull(target.get(key(99)));
        }
    }

    @Test
    void mergesWithDataAlreadyPresentInTheTarget() throws Exception {
        Path source = tempDir.resolve("h3_0");
        Path targetPath = tempDir.resolve("merged");

        createDb(source,
                 entry(1, 10, 20),
                 entry(2, 30));

        try (Options options = new Options().setCreateIfMissing(true);
             RocksDB target = RocksDB.open(options, targetPath.toString())) {
            target.put(key(1), value(20, 99));
            target.put(key(6), value(100));

            importer.mergeH3Dbs(List.of(source), target, new Options().setCreateIfMissing(false));

            assertEquals(List.of(10L, 20L, 99L), readOsmIds(target, 1));
            assertEquals(List.of(30L), readOsmIds(target, 2));
            assertEquals(List.of(100L), readOsmIds(target, 6));
        }
    }

    private void createDb(Path path, Entry... entries) throws Exception {
        try (Options options = new Options().setCreateIfMissing(true);
             RocksDB db = RocksDB.open(options, path.toString())) {
            for (Entry entry : entries) {
                db.put(key(entry.cell()), value(entry.osmIds()));
            }
        }
    }

    private static List<Long> readOsmIds(RocksDB db, long cell) throws Exception {
        byte[] raw = db.get(key(cell));
        if (raw == null) {
            return List.of();
        }
        ByteBuffer buffer = ByteBuffer.wrap(raw).order(ORDER);
        List<Long> result = new ArrayList<>();
        while (buffer.remaining() >= Long.BYTES) {
            result.add(buffer.getLong());
        }
        return result;
    }

    private static Entry entry(long cell, long... osmIds) {
        return new Entry(cell, osmIds);
    }

    private static byte[] key(long cell) {
        return ByteBuffer.allocate(Long.BYTES).order(ORDER).putLong(cell).array();
    }

    private static byte[] value(long... osmIds) {
        ByteBuffer buffer = ByteBuffer.allocate(osmIds.length * Long.BYTES).order(ORDER);
        for (long osmId : osmIds) {
            buffer.putLong(osmId);
        }
        return buffer.array();
    }

    private static StandaloneBoundaryImporter newImporter() {
        try {
            PaikkaConfiguration config = new PaikkaConfiguration();

            PaikkaConfiguration.ImportConfiguration importCfg = new PaikkaConfiguration.ImportConfiguration();
            importCfg.setThreads(1);
            config.setImportConfiguration(importCfg);

            PaikkaConfiguration.SimplificationConfiguration simplCfg = new PaikkaConfiguration.SimplificationConfiguration();
            simplCfg.setContinentTolerance(0.005);
            simplCfg.setCountryTolerance(0.00045);
            simplCfg.setStateTolerance(0.00009);
            simplCfg.setPoiTolerance(0.000018);
            simplCfg.setDefaultTolerance(0.000045);
            config.setSimplificationConfiguration(simplCfg);

            return new StandaloneBoundaryImporter(new GeometrySimplificationService(config), config);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private record Entry(long cell, long[] osmIds) {
    }
}
