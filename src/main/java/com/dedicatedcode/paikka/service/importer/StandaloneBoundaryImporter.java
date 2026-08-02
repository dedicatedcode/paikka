/*
 *  This file is part of paikka.
 *
 *  Paikka is free software: you can redistribute it and/or
 *  modify it under the terms of the GNU Affero General Public License
 *  as published by the Free Software Foundation, either version 3 or
 *  any later version.
 *
 *  Paikka is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied
 *  warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *  See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License
 * along with Paikka. If not, see <https://www.gnu.org/licenses/>.
 */

package com.dedicatedcode.paikka.service.importer;

import com.dedicatedcode.paikka.config.PaikkaConfiguration;
import com.uber.h3core.H3Core;
import com.uber.h3core.util.LatLng;
import de.topobyte.osm4j.core.model.iface.*;
import de.topobyte.osm4j.pbf.seq.PbfIterator;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.WKBWriter;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Standalone H3-based administrative boundary importer for Paikka.
 * <p>
 * Reads a pre-filtered boundaries_only.pbf (Nodes -> Ways -> Relations ordered)
 * and produces three RocksDB databases for offline mobile lookup:
 * - h3_to_osm: H3_CELL_ID (uint64) -> List[OSM_ID] (raw byte array)
 * - region_metadata: OSM_ID -> [total cell count (long), h3 resolution (int)] (12 bytes)
 * - region_geometry: OSM_ID -> simplified WKB (bytes)
 */
@Service
public class StandaloneBoundaryImporter {
    private static final Logger logger = LoggerFactory.getLogger(StandaloneBoundaryImporter.class);

    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
    private static final double BUFFER_DISTANCE = 0.0001; // ~11m at equator, ensures border cells
    private static final long H3_THREAD_WRITE_BUFFER = 64L * 1024 * 1024; // per-thread RocksDB write buffer

    private final GeometrySimplificationService geometrySimplificationService;
    private final PaikkaConfiguration paikkaConfiguration;
    private final H3Core h3;
    private final BoundaryImportStatistics stats;

    public StandaloneBoundaryImporter(GeometrySimplificationService geometrySimplificationService, PaikkaConfiguration paikkaConfiguration) throws Exception {
        this.geometrySimplificationService = geometrySimplificationService;
        this.paikkaConfiguration = paikkaConfiguration;
        this.h3 = H3Core.newInstance(); // Uber H3-Java 4.x
        this.stats = new BoundaryImportStatistics();
    }

    // ============================ PUBLIC API ============================

    public void importBoundaries(List<String> pbfPaths, String outputDir) throws Exception {
        RocksDB.loadLibrary();
        Path out = Paths.get(outputDir);
        Path tmp = out.resolve("tmp");
        Files.createDirectories(out);
        Files.createDirectories(tmp);

        Path nodeCachePath = tmp.resolve("node_cache");
        Path wayCachePath = tmp.resolve("way_cache");
        Path h3ToOsmPath = out.resolve("h3_to_osm");
        Path regionMetaPath = out.resolve("region_metadata");
        Path regionGeomPath = out.resolve("region_geometry");
        Path nameSql = out.resolve("osm_names.tsv");

        Path tmpRegionMetaPath = tmp.resolve("tmp_region_metadata");
        Path tmpRegionGeomPath = tmp.resolve("tmp_region_geometry");

        cleanup(nodeCachePath);
        cleanup(wayCachePath);
        cleanup(h3ToOsmPath);
        cleanup(regionMetaPath);
        cleanup(regionGeomPath);
        cleanup(tmpRegionMetaPath);
        cleanup(tmpRegionGeomPath);

        BlockBasedTableConfig tableCfg = new BlockBasedTableConfig()
                .setBlockSize(64 * 1024)
                .setFilterPolicy(new BloomFilter(10, false));

        Options cacheOpts = new Options()
                .setCreateIfMissing(true)
                .setTableFormatConfig(tableCfg)
                .setCompressionType(CompressionType.LZ4_COMPRESSION)
                .setWriteBufferSize(512 * 1024 * 1024)
                .setMaxWriteBufferNumber(3)
                .setLevel0FileNumCompactionTrigger(4);

        // Per-thread H3 DBs use smaller write buffers (64MB each) to control memory
        Options threadH3Opts = new Options(cacheOpts)
                .setWriteBufferSize(H3_THREAD_WRITE_BUFFER)
                .setMaxWriteBufferNumber(2);

        Options finalOpts = new Options()
                .setCreateIfMissing(true)
                .setTableFormatConfig(tableCfg)
                .setCompressionType(CompressionType.ZSTD_COMPRESSION)
                .setWriteBufferSize(256 * 1024 * 1024)
                .setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION)
                .setCompressionPerLevel(List.of(
                    CompressionType.NO_COMPRESSION,
                    CompressionType.NO_COMPRESSION,
                    CompressionType.LZ4_COMPRESSION,
                    CompressionType.LZ4_COMPRESSION,
                    CompressionType.ZSTD_COMPRESSION,
                    CompressionType.ZSTD_COMPRESSION,
                    CompressionType.ZSTD_COMPRESSION
                ));

        stats.startProgressReporter();

        // Shared block cache reduces redundant I/O across all RocksDB instances
        try (Cache sharedCache = new LRUCache(2L * 1024 * 1024 * 1024)) {
            tableCfg.setBlockCache(sharedCache);

            try (
                    RocksDB nodeCache = RocksDB.open(cacheOpts, nodeCachePath.toString());
                    RocksDB wayCache = RocksDB.open(cacheOpts, wayCachePath.toString());
                    RocksDB h3ToOsm = RocksDB.open(finalOpts, h3ToOsmPath.toString());
                    RocksDB regionMeta = RocksDB.open(finalOpts, regionMetaPath.toString());
                    RocksDB regionGeom = RocksDB.open(finalOpts, regionGeomPath.toString());
                    RocksDB tmpRegionMeta = RocksDB.open(cacheOpts, tmpRegionMetaPath.toString());
                    RocksDB tmpRegionGeom = RocksDB.open(cacheOpts, tmpRegionGeomPath.toString());
                    OsmNameStreamer nameStreamer = new OsmNameStreamer(nameSql.toString())
            ) {
                int threads = paikkaConfiguration.getImportConfiguration().getThreads();

                // Pre-create per-thread H3 db paths and clean any left-overs
                Path[] threadH3Paths = new Path[threads];
                for (int t = 0; t < threads; t++) {
                    threadH3Paths[t] = tmp.resolve("h3_osm_" + t);
                    cleanup(threadH3Paths[t]);
                }

                for (String pbfPath : pbfPaths) {
                    stats.setCurrentPhase(1, "Phase 1: Caching Nodes & Ways");

                    // ------ Single pass: cache nodes/ways and collect relation stubs ------
                    List<RelationStub> stubs = new ArrayList<>(500_000);

                    try (InputStream is = Files.newInputStream(Paths.get(pbfPath));
                         WriteOptions wo = new WriteOptions().setDisableWAL(true)) {

                        PbfIterator iterator = new PbfIterator(is, false);
                        WriteBatch nodeBatch = new WriteBatch();
                        WriteBatch wayBatch = new WriteBatch();
                        AtomicLong phaseCounter = new AtomicLong();

                        while (iterator.hasNext()) {
                            EntityContainer c = iterator.next();
                            if (c.getType() == EntityType.Node) {
                                OsmNode n = (OsmNode) c.getEntity();
                                ByteBuffer bb = ByteBuffer.allocate(16)
                                        .putDouble(n.getLatitude())
                                        .putDouble(n.getLongitude());
                                nodeBatch.put(longToBytes(n.getId()), bb.array());
                                stats.incrementNodesCached();
                                if (phaseCounter.incrementAndGet() % 100_000 == 0) {
                                    nodeCache.write(wo, nodeBatch);
                                    nodeBatch.clear();
                                }
                            } else if (c.getType() == EntityType.Way) {
                                OsmWay w = (OsmWay) c.getEntity();
                                long[] ids = new long[w.getNumberOfNodes()];
                                for (int i = 0; i < w.getNumberOfNodes(); i++) ids[i] = w.getNodeId(i);
                                wayBatch.put(longToBytes(w.getId()), longArrayToBytes(ids));
                                stats.incrementWaysCached();
                                if (phaseCounter.incrementAndGet() % 50_000 == 0) {
                                    wayCache.write(wo, wayBatch);
                                    wayBatch.clear();
                                }
                            } else if (c.getType() == EntityType.Relation) {
                                OsmRelation r = (OsmRelation) c.getEntity();
                                try {
                                    nameStreamer.processEntity(r, "R");
                                } catch (IOException e) {
                                    logger.warn("Failed to stream name for relation ID: {}", r.getId(), e);
                                }
                                if (isAdministrativeBoundary(r)) {
                                    stats.incrementRelationsFound();
                                    stubs.add(buildRelationStub(r));
                                }
                            }
                        }
                        nodeCache.write(wo, nodeBatch);
                        wayCache.write(wo, wayBatch);
                        nodeBatch.close();
                        wayBatch.close();
                    }

                    // ------ Process Relations via partitioned thread pool ------
                    stats.setCurrentPhase(2, "Phase 2: Processing Relations & H3");

                    if (!stubs.isEmpty()) {
                        ExecutorService executor = Executors.newFixedThreadPool(threads);
                        List<Future<?>> futures = new ArrayList<>();
                        int partitionSize = (stubs.size() + threads - 1) / threads;

                        for (int t = 0; t < threads; t++) {
                            int from = t * partitionSize;
                            int to = Math.min(from + partitionSize, stubs.size());
                            if (from >= to) break;

                            List<RelationStub> partition = stubs.subList(from, to);
                            final int threadIndex = t;

                            futures.add(executor.submit(() -> {
                                try {
                                    try (RocksDB threadH3 = RocksDB.open(threadH3Opts, threadH3Paths[threadIndex].toString());
                                         WriteOptions wo = new WriteOptions().setDisableWAL(true)) {
                                        for (RelationStub stub : partition) {
                                            if (stub.adminLevel() <= 3) {
                                                logger.debug("Processing relation OSM ID: {} [Admin Level: {}]", stub.osmId(), stub.adminLevel());
                                            }
                                            try {
                                                Geometry geom = buildMultiPolygon(stub, nodeCache, wayCache);
                                                if (geom == null || geom.isEmpty()) {
                                                    continue;
                                                }

                                                // Repair invalid geometries using buffer(0)
                                                if (!geom.isValid()) {
                                                    logger.debug("Relation OSM ID: {} [Admin Level: {}] Geometry is invalid, attempting repair", stub.osmId(), stub.adminLevel());
                                                    geom = geom.buffer(0);
                                                    if (geom == null || geom.isEmpty() || !geom.isValid()) {
                                                        logger.error("Relation OSM ID: {} [Admin Level: {}] Geometry repair failed", stub.osmId(), stub.adminLevel());
                                                        continue;
                                                    }
                                                }
                                                // Simplify first to reduce H3 cell count, then buffer
                                                Geometry simplified = geometrySimplificationService.simplifyByAdminLevel(geom, stub.adminLevel());
                                                if (simplified == null || simplified.isEmpty()) {
                                                    simplified = geom;
                                                }
                                                // Buffer to include border-touching cells
                                                Geometry buffered = simplified.buffer(BUFFER_DISTANCE);
                                                if (stub.adminLevel() <= 3) {
                                                    logger.debug("Simplified Geometry: {} points for OSM ID: {}", simplified.getNumPoints(), stub.osmId());
                                                }

                                                int resolution = getResolutionForAdminLevel(stub.adminLevel());

                                                // ---- H3 Polyfill ----
                                                AtomicLong cellCount = new AtomicLong(0);
                                                long startTime = System.currentTimeMillis();
                                                processCellsH3Stream(buffered, stub.osmId(), wo, threadH3, cellCount, resolution);
                                                if (stub.adminLevel() <= 3) {
                                                    logger.debug("H3 Polyfill (Res {}) took {}ms for OSM ID: {}", resolution, System.currentTimeMillis() - startTime, stub.osmId());
                                                }
                                                long totalCells = cellCount.get();
                                                if (totalCells == 0) continue;

                                                stats.incrementRelationsProcessed();
                                                stats.addH3CellsGenerated(totalCells);

                                                startTime = System.currentTimeMillis();
                                                tmpRegionMeta.put(wo, longToBytes(stub.osmId()), cellMetaToBytes(totalCells, resolution));
                                                if (stub.adminLevel() <= 3) {
                                                    logger.debug("H3 Cells written to tmpRegionMeta in {}ms for OSM ID: {}", System.currentTimeMillis() - startTime, stub.osmId());
                                                }
                                                startTime = System.currentTimeMillis();
                                                byte[] wkb = new WKBWriter().write(simplified);
                                                tmpRegionGeom.put(wo, longToBytes(stub.osmId()), wkb);
                                                if (stub.adminLevel() <= 3) {
                                                    logger.debug("WKB written to tmpRegionGeom in {}ms for OSM ID: {}", System.currentTimeMillis() - startTime, stub.osmId());
                                                }
                                            } catch (Exception e) {
                                                stats.recordError(BoundaryImportStatistics.Stage.PROCESSING_RELATIONS, BoundaryImportStatistics.Kind.GEOMETRY, stub.osmId(), "process-relation", e);
                                            }
                                        }
                                    }
                                } catch (RocksDBException e) {
                                    throw new RuntimeException("Failed to open per-thread H3 DB", e);
                                }
                            }));
                        }

                        for (Future<?> f : futures) {
                            f.get();
                        }
                        executor.shutdown();
                        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

                        // Merge per-thread H3 DBs into the final h3_to_osm
                        stats.setCurrentPhase(3, "3.1: Merging H3 thread DBs");
                        for (int t = 0; t < threads; t++) {
                            if (Files.exists(threadH3Paths[t])) {
                                try (RocksDB threadDb = RocksDB.open(cacheOpts, threadH3Paths[t].toString())) {
                                    copyH3Db(threadDb, h3ToOsm);
                                }
                                cleanup(threadH3Paths[t]);
                            }
                        }
                    }
                }

                stats.setCurrentPhase(3, "3.2: Compacting Final Databases");
                // Copy from temporary DBs to final DBs in sorted order
                copyDb(tmpRegionMeta, regionMeta);
                copyDb(tmpRegionGeom, regionGeom);

                // Compact finals
                h3ToOsm.compactRange();
                regionMeta.compactRange();
                regionGeom.compactRange();
            }
        }

        stats.setOutputSizes(
                dirSize(h3ToOsmPath),
                dirSize(regionMetaPath),
                dirSize(regionGeomPath),
                fileSize(nameSql)
        );

        stats.stop();
        stats.setTotalTime(System.currentTimeMillis() - stats.getStartTime());
        stats.printFinalStatistics();
        stats.printOutcomeAndErrors();

        // Cleanup tmp
        cleanup(tmp);
    }

    private void copyDb(RocksDB source, RocksDB target) throws RocksDBException {
        try (RocksIterator it = source.newIterator(); WriteOptions wo = new WriteOptions().setDisableWAL(true)) {
            it.seekToFirst();
            while (it.isValid()) {
                target.put(wo, it.key(), it.value());
                it.next();
            }
        }
    }

    private void copyH3Db(RocksDB source, RocksDB target) throws RocksDBException {
        try (RocksIterator it = source.newIterator(); WriteOptions wo = new WriteOptions().setDisableWAL(true)) {
            it.seekToFirst();
            while (it.isValid()) {
                byte[] key = it.key();
                byte[] newVal = it.value();
                byte[] existing = target.get(key);
                if (existing == null) {
                    target.put(wo, key, newVal);
                } else {
                    byte[] merged = mergeOsmIdArrays(existing, newVal);
                    target.put(wo, key, merged);
                }
                it.next();
            }
        }
    }

    private byte[] mergeOsmIdArrays(byte[] existing, byte[] newVal) {
        ByteBuffer bb = ByteBuffer.wrap(newVal).order(ByteOrder.BIG_ENDIAN);
        byte[] current = existing;
        while (bb.hasRemaining()) {
            long osmId = bb.getLong();
            current = appendOsmIdToArray(current, osmId);
        }
        return current;
    }

    // ============================ GEOMETRY STITCHING ============================

    /**
     * Builds a JTS MultiPolygon from relation outer/inner way members.
     * Rings are stitched by coordinate continuation (same logic as ImportService.buildConnectedRings).
     */
    private Geometry buildMultiPolygon(RelationStub stub, RocksDB nodeCache, RocksDB wayCache) {
        List<List<Coordinate>> outerRings = stitchRings(stub.outerWays(), nodeCache, wayCache);
        List<List<Coordinate>> innerRings = stitchRings(stub.innerWays(), nodeCache, wayCache);
        if (outerRings.isEmpty()) return null;

        List<Polygon> polygons = new ArrayList<>();
        for (List<Coordinate> outer : outerRings) {
            try {
                LinearRing shell = GEOMETRY_FACTORY.createLinearRing(outer.toArray(new Coordinate[0]));
                List<LinearRing> holes = new ArrayList<>();
                for (List<Coordinate> inner : innerRings) {
                    try {
                        holes.add(GEOMETRY_FACTORY.createLinearRing(inner.toArray(new Coordinate[0])));
                    } catch (Exception e) {
                        stats.recordError(BoundaryImportStatistics.Stage.PROCESSING_RELATIONS, BoundaryImportStatistics.Kind.GEOMETRY, stub.osmId(), "createLinearRing-inner", e);
                    }
                }
                Polygon p = GEOMETRY_FACTORY.createPolygon(shell, holes.toArray(new LinearRing[0]));
                if (p.isValid()) polygons.add(p);
            } catch (Exception e) {
                stats.recordError(BoundaryImportStatistics.Stage.PROCESSING_RELATIONS, BoundaryImportStatistics.Kind.GEOMETRY, stub.osmId(), "buildMultiPolygon", e);
            }
        }
        if (polygons.isEmpty()) return null;
        return polygons.size() == 1 ? polygons.getFirst() : GEOMETRY_FACTORY.createMultiPolygon(polygons.toArray(new Polygon[0]));
    }

    private List<List<Coordinate>> stitchRings(List<Long> wayIds, RocksDB nodeCache, RocksDB wayCache) {
        Map<Long, List<Coordinate>> wayCoords = new HashMap<>();
        for (long wid : wayIds) {
            try {
                byte[] seq = wayCache.get(longToBytes(wid));
                if (seq == null) continue;
                long[] nodeIds = bytesToLongArray(seq);
                List<Coordinate> coords = resolveCoordinates(nodeIds, nodeCache);
                if (coords != null && coords.size() >= 2) wayCoords.put(wid, coords);
            } catch (RocksDBException e) {
                stats.recordError(BoundaryImportStatistics.Stage.CACHING_NODES_WAYS, BoundaryImportStatistics.Kind.STORE, wid, "stitchRings", e);
            }
        }
        List<List<Coordinate>> rings = new ArrayList<>();
        Set<Long> used = new HashSet<>();
        while (used.size() < wayCoords.size()) {
            Long start = wayCoords.keySet().stream().filter(id -> !used.contains(id)).findFirst().orElse(null);
            if (start == null) break;
            List<Coordinate> ring = new ArrayList<>(wayCoords.get(start));
            used.add(start);
            boolean extended;
            do {
                extended = false;
                Coordinate end = ring.getLast();
                for (Map.Entry<Long, List<Coordinate>> e : wayCoords.entrySet()) {
                    if (used.contains(e.getKey())) continue;
                    List<Coordinate> w = e.getValue();
                    if (end.equals2D(w.getFirst())) {
                        ring.addAll(w.subList(1, w.size()));
                        used.add(e.getKey());
                        extended = true;
                        break;
                    } else if (end.equals2D(w.getLast())) {
                        List<Coordinate> rev = new ArrayList<>(w);
                        Collections.reverse(rev);
                        ring.addAll(rev.subList(1, rev.size()));
                        used.add(e.getKey());
                        extended = true;
                        break;
                    }
                }
            } while (extended);
            if (ring.size() >= 3 && !ring.getFirst().equals2D(ring.getLast()))
                ring.add(new Coordinate(ring.getFirst()));
            if (ring.size() >= 4) rings.add(ring);
        }
        return rings;
    }

    private List<Coordinate> resolveCoordinates(long[] nodeIds, RocksDB nodeCache) {
        try {
            List<byte[]> keys = new ArrayList<>(nodeIds.length);
            for (long id : nodeIds) keys.add(longToBytes(id));
            List<byte[]> vals = nodeCache.multiGetAsList(keys);
            List<Coordinate> coords = new ArrayList<>(nodeIds.length);
            for (byte[] v : vals) {
                if (v != null && v.length == 16) {
                    ByteBuffer bb = ByteBuffer.wrap(v);
                    double lat = bb.getDouble(0);
                    double lon = bb.getDouble(8);
                    coords.add(new Coordinate(lon, lat)); // JTS uses (x=lon, y=lat)
                } else return null;
            }
            return coords;
        } catch (RocksDBException e) {
            stats.recordError(BoundaryImportStatistics.Stage.CACHING_NODES_WAYS, BoundaryImportStatistics.Kind.STORE, null, "resolveCoordinates", e);
            return null;
        }
    }

    // ============================ H3 POLYFILL ============================

    /**
     * Determines the H3 resolution based on the administrative level.
     * Lower admin levels (countries) use lower resolutions to save space.
     * Higher admin levels (cities) use higher resolutions for accuracy.
     */
    private int getResolutionForAdminLevel(int adminLevel) {
        if (adminLevel <= 2) return 4; // Continents/Countries
        if (adminLevel <= 5) return 6; // States/Regions
        return 9; // Districts/Cities
    }

    /**
     * Converts a JTS Geometry to H3 cells at the specified resolution.
     * Uses h3.polygonToCells with LatLng vertices. Multipolygons are expanded.
     * Each thread has its own RocksDB instance (passed via threadH3ToOsm) so no
     * synchronization is needed on writes.
     */
    private void processCellsH3Stream(Geometry geom, long osmId, WriteOptions wo, RocksDB threadH3ToOsm, AtomicLong cellCount, int resolution) {
        int num = geom.getNumGeometries();
        for (int i = 0; i < num; i++) {
            Geometry part = geom.getGeometryN(i);
            if (!(part instanceof Polygon poly)) continue;
            List<LatLng> outer = toLatLng(poly.getExteriorRing().getCoordinates());
            List<List<LatLng>> holes = new ArrayList<>();
            for (int h = 0; h < poly.getNumInteriorRing(); h++) {
                holes.add(toLatLng(poly.getInteriorRingN(h).getCoordinates()));
            }
            List<Long> batch = new ArrayList<>(5_000);
            h3.polygonToCells(outer, holes, resolution).forEach(cell -> {
                batch.add(cell);
                if (batch.size() >= 5_000) {
                    processH3Batch(batch, osmId, wo, threadH3ToOsm, cellCount);
                    batch.clear();
                }
            });
            if (!batch.isEmpty()) {
                processH3Batch(batch, osmId, wo, threadH3ToOsm, cellCount);
            }
        }
    }

    /**
     * Writes H3 cell -> OSM ID associations to a thread-local RocksDB.
     * No synchronization needed since each thread owns its RocksDB instance.
     */
    private void processH3Batch(List<Long> cells, long osmId, WriteOptions wo, RocksDB h3Db, AtomicLong cellCount) {
        List<byte[]> keys = new ArrayList<>(cells.size());
        for (long cell : cells) {
            keys.add(longToBytes(cell));
        }
        try {
            List<byte[]> existingValues = h3Db.multiGetAsList(keys);
            try (WriteBatch writeBatch = new WriteBatch()) {
                for (int i = 0; i < cells.size(); i++) {
                    cellCount.incrementAndGet();
                    byte[] existing = existingValues.get(i);
                    byte[] updated = appendOsmIdToArray(existing, osmId);
                    writeBatch.put(keys.get(i), updated);
                }
                h3Db.write(wo, writeBatch);
            }
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    private List<LatLng> toLatLng(Coordinate[] coords) {
        List<LatLng> list = new ArrayList<>(coords.length);
        for (Coordinate c : coords) {
            list.add(new LatLng(c.y, c.x)); // lat, lon
        }
        return list;
    }

    // ============================ BYTE UTILS ============================

    private byte[] longToBytes(long v) {
        return ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(v).array();
    }

    private byte[] longArrayToBytes(long[] arr) {
        ByteBuffer bb = ByteBuffer.allocate(8 * arr.length).order(ByteOrder.BIG_ENDIAN);
        for (long v : arr) bb.putLong(v);
        return bb.array();
    }

    private long[] bytesToLongArray(byte[] b) {
        ByteBuffer bb = ByteBuffer.wrap(b).order(ByteOrder.BIG_ENDIAN);
        long[] arr = new long[b.length / 8];
        for (int i = 0; i < arr.length; i++) arr[i] = bb.getLong();
        return arr;
    }

    private byte[] cellMetaToBytes(long cellCount, int resolution) {
        return ByteBuffer.allocate(12).order(ByteOrder.BIG_ENDIAN)
                .putLong(cellCount)
                .putInt(resolution)
                .array();
    }

    /**
     * Appends an OSM_ID to a raw byte array of longs, preventing duplicates.
     * Format: sequence of 8-byte big-endian longs.
     */
    private byte[] appendOsmIdToArray(byte[] existing, long osmId) {
        if (existing == null || existing.length == 0) {
            return longToBytes(osmId);
        }
        int count = existing.length / 8;
        for (int i = 0; i < count; i++) {
            long val = ByteBuffer.wrap(existing, i * 8, 8).order(ByteOrder.BIG_ENDIAN).getLong();
            if (val == osmId) return existing; // duplicate
        }
        ByteBuffer bb = ByteBuffer.allocate(existing.length + 8).order(ByteOrder.BIG_ENDIAN);
        bb.put(existing);
        bb.putLong(osmId);
        return bb.array();
    }

    // ============================ OSM HELPERS ============================

    private boolean isAdministrativeBoundary(OsmRelation r) {
        boolean boundary = false, adminLevel = false;
        for (int i = 0; i < r.getNumberOfTags(); i++) {
            OsmTag t = r.getTag(i);
            if ("boundary".equals(t.getKey()) && "administrative".equals(t.getValue())) boundary = true;
            if ("admin_level".equals(t.getKey())) adminLevel = true;
            if ("type".equals(t.getKey()) && "boundary".equals(t.getValue())) boundary = true;
        }
        return boundary && adminLevel;
    }

    private RelationStub buildRelationStub(OsmRelation r) {
        List<Long> outer = new ArrayList<>();
        List<Long> inner = new ArrayList<>();
        int level = 10;
        for (int i = 0; i < r.getNumberOfMembers(); i++) {
            OsmRelationMember m = r.getMember(i);
            if (m.getType() == EntityType.Way) {
                String role = m.getRole();
                if ("outer".equals(role) || role == null || role.isEmpty()) outer.add(m.getId());
                else if ("inner".equals(role)) inner.add(m.getId());
            }
        }
        for (int i = 0; i < r.getNumberOfTags(); i++) {
            OsmTag t = r.getTag(i);
            if ("admin_level".equals(t.getKey())) {
                try {
                    level = Integer.parseInt(t.getValue());
                } catch (NumberFormatException ignored) {
                }
            }
        }
        return new RelationStub(r.getId(), level, outer, inner);
    }

    private record RelationStub(long osmId, int adminLevel, List<Long> outerWays, List<Long> innerWays) {
    }

    private void cleanup(Path p) {
        if (Files.exists(p)) {
            try {
                Files.walk(p).sorted(Comparator.reverseOrder()).forEach(path -> {
                    try {
                        Files.delete(path);
                    } catch (IOException e) {
                        System.err.println("warn: " + e.getMessage());
                    }
                });
            } catch (IOException e) {
                System.err.println("Failed cleanup: " + p + " -> " + e.getMessage());
            }
        }
    }

    private long dirSize(Path dir) {
        try {
            return Files.walk(dir)
                    .filter(Files::isRegularFile)
                    .mapToLong(p -> {
                        try {
                            return Files.size(p);
                        } catch (IOException e) {
                            return 0L;
                        }
                    })
                    .sum();
        } catch (IOException e) {
            return 0L;
        }
    }

    private long fileSize(Path file) {
        try {
            return Files.exists(file) ? Files.size(file) : 0L;
        } catch (IOException e) {
            return 0L;
        }
    }
}
