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
import org.locationtech.jts.operation.polygonize.Polygonizer;
import org.locationtech.jts.operation.union.UnaryUnionOp;
import org.locationtech.jts.geom.util.GeometryFixer;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Standalone H3-based administrative boundary importer for Paikka.
 * <p>
 * Reads a pre-filtered boundaries_only.pbf (Nodes -> Ways -> Relations ordered)
 * and produces three RocksDB databases for offline mobile lookup:
 * - h3_to_osm: H3_CELL_ID (uint64) -> List[OSM_ID] (raw byte array)
 * - region_metadata: OSM_ID -> [total cell count (long), h3 resolution (int), admin_level (int)] (16 bytes)
 * - region_geometry: OSM_ID -> simplified WKB (bytes)
 * <p>
 * Additionally writes two PostgreSQL-COPY compatible side-car files:
 * - osm_names.tsv: OSM_ID, type, all name:* tags as JSON
 * - region_hierarchy.tsv: OSM_ID, parent OSM_ID, kind, admin_level, resolution, total_cells,
 *   depth, child_count, continent code
 */
@Service
public class StandaloneBoundaryImporter {
    private static final Logger logger = LoggerFactory.getLogger(StandaloneBoundaryImporter.class);

    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
    private static final double BUFFER_DISTANCE = 0.0001; // ~11m at equator, ensures border cells
    private static final long H3_THREAD_WRITE_BUFFER = 64L * 1024 * 1024; // per-thread RocksDB write buffer
    // Regions smaller than one cell at their admin-level resolution (Monaco, San Marino,
    // Liechtenstein, ...) yield no cells whose centre falls inside the polygon. Retry finer.
    private static final int[] FALLBACK_RESOLUTIONS = {6, 9};

    private final GeometrySimplificationService geometrySimplificationService;
    private final PaikkaConfiguration paikkaConfiguration;
    private final H3Core h3;
    private final BoundaryImportStatistics stats;
    private final Map<Long, String> skippedRelationReasons = new ConcurrentHashMap<>();
    private final Set<Long> importedRelationIds = ConcurrentHashMap.newKeySet();

    public StandaloneBoundaryImporter(GeometrySimplificationService geometrySimplificationService, PaikkaConfiguration paikkaConfiguration) throws Exception {
        this.geometrySimplificationService = geometrySimplificationService;
        this.paikkaConfiguration = paikkaConfiguration;
        this.h3 = H3Core.newInstance(); // Uber H3-Java 4.x
        this.stats = new BoundaryImportStatistics();
    }

    // ============================ PUBLIC API ============================

    public void importBoundaries(List<String> pbfPaths, String outputDir) throws Exception {
        importBoundaries(pbfPaths, outputDir, null);
    }

    public void importBoundaries(List<String> pbfPaths, String outputDir, String scratchDir) throws Exception {
        RocksDB.loadLibrary();
        Path out = Paths.get(outputDir);
        Path tmp = (scratchDir == null || scratchDir.isBlank())
                ? out.resolve("tmp")
                : Paths.get(scratchDir).toAbsolutePath();
        if (tmp.getParent() == null) {
            throw new IllegalArgumentException("Refusing to use '" + tmp + "' as scratch directory");
        }
        Files.createDirectories(out);
        Files.createDirectories(tmp);
        skippedRelationReasons.clear();
        importedRelationIds.clear();

        Path nodeCachePath = tmp.resolve("node_cache");
        Path wayCachePath = tmp.resolve("way_cache");
        Path h3ToOsmPath = out.resolve("h3_to_osm");
        Path regionMetaPath = out.resolve("region_metadata");
        Path regionGeomPath = out.resolve("region_geometry");
        Path nameSql = out.resolve("osm_names.tsv");
        Path hierarchyTsv = out.resolve("region_hierarchy.tsv");

        Path tmpRegionMetaPath = tmp.resolve("tmp_region_metadata");
        Path tmpRegionGeomPath = tmp.resolve("tmp_region_geometry");

        cleanup(nodeCachePath);
        cleanup(wayCachePath);
        cleanup(h3ToOsmPath);
        cleanup(regionMetaPath);
        cleanup(regionGeomPath);
        cleanup(tmpRegionMetaPath);
        cleanup(tmpRegionGeomPath);
        Files.deleteIfExists(hierarchyTsv);

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

                // Hierarchy inputs accumulate across all PBF files
                Map<Long, String> isoCodes = new HashMap<>();
                Map<Long, List<Long>> subareaEdges = new HashMap<>();
                Map<Long, CountryStub> countryStubs = new LinkedHashMap<>();
                Map<Long, CountryHints> countryHints = new HashMap<>();
                Map<String, Long> regionByIso3166_2 = new HashMap<>();
                Map<String, String> countryIsoByName = new HashMap<>();

                for (String pbfPath : pbfPaths) {
                    stats.setCurrentPhase(1, BoundaryImportStatistics.PHASE_CACHING);

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
                                    RelationStub stub = buildRelationStub(r);
                                    if (stub.isoCode() != null) {
                                        isoCodes.put(stub.osmId(), stub.isoCode());
                                    }
                                    CountryHints hints = new CountryHints(stub.iso3166_2(), stub.countryCode(),
                                                                          stub.countryName(), stub.nameEn());
                                    if (hints.hasAny()) {
                                        countryHints.put(stub.osmId(), hints);
                                    }
                                    if (stub.iso3166_2() != null) {
                                        regionByIso3166_2.put(stub.iso3166_2(), stub.osmId());
                                    }
                                    if (stub.adminLevel() == 2 && stub.isoCode() != null) {
                                        countryStubs.put(stub.osmId(), new CountryStub(stub.osmId(), stub.isoCode(), stub.name()));
                                        if (stub.name() != null) {
                                            countryIsoByName.put(normalizeCountryName(stub.name()), stub.isoCode());
                                        }
                                        if (stub.nameEn() != null) {
                                            countryIsoByName.put(normalizeCountryName(stub.nameEn()), stub.isoCode());
                                        }
                                    }
                                    if (!stub.subareaRelations().isEmpty()) {
                                        subareaEdges.put(stub.osmId(), stub.subareaRelations());
                                    }
                                    stubs.add(stub);
                                }
                            }
                        }
                        nodeCache.write(wo, nodeBatch);
                        wayCache.write(wo, wayBatch);
                        nodeBatch.close();
                        wayBatch.close();
                    }

                    // ------ Process Relations via partitioned thread pool ------
                    stats.setCurrentPhase(2, BoundaryImportStatistics.PHASE_RELATIONS);
                    stats.setWorkerThreads(threads);

                    if (!stubs.isEmpty()) {
                        // Longest-processing-time first: large regions start early so they
                        // do not become a one-thread tail at the end of the phase.
                        stubs.sort(Comparator
                                .comparingInt(RelationStub::adminLevel)
                                .thenComparing(Comparator.comparingInt((RelationStub s) -> s.outerWays().size()).reversed())
                                .thenComparing(Comparator.comparingInt((RelationStub s) -> s.innerWays().size()).reversed()));

                        ExecutorService executor = Executors.newFixedThreadPool(threads);
                        List<Future<?>> futures = new ArrayList<>();
                        BlockingQueue<List<RelationStub>> queue = new LinkedBlockingQueue<>(Math.max(2, threads * 2));
                        AtomicReference<Throwable> fatalFailure = new AtomicReference<>();
                        int batchSize = 100;

                        Thread producer = Thread.ofPlatform().daemon().start(() -> {
                            try {
                                for (int from = 0; from < stubs.size(); from += batchSize) {
                                    if (fatalFailure.get() != null) break;
                                    int to = Math.min(from + batchSize, stubs.size());
                                    queue.put(new ArrayList<>(stubs.subList(from, to)));
                                }
                                for (int i = 0; i < threads; i++) {
                                    queue.put(Collections.emptyList());
                                }
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        });

                        for (int t = 0; t < threads; t++) {
                            final int threadIndex = t;

                            futures.add(executor.submit(() -> {
                                stats.incrementActiveThreads();
                                try {
                                    try (RocksDB threadH3 = RocksDB.open(threadH3Opts, threadH3Paths[threadIndex].toString());
                                         WriteOptions wo = new WriteOptions().setDisableWAL(true)) {
                                        while (true) {
                                            List<RelationStub> batch;
                                            try {
                                                batch = queue.take();
                                            } catch (InterruptedException e) {
                                                Thread.currentThread().interrupt();
                                                break;
                                            }
                                            if (batch.isEmpty()) {
                                                break;
                                            }
                                            for (RelationStub stub : batch) {
                                                if (fatalFailure.get() != null) {
                                                    break;
                                                }
                                                try {
                                                    processRelationStub(stub, nodeCache, wayCache, threadH3, wo, tmpRegionMeta, tmpRegionGeom);
                                                } catch (Exception e) {
                                                    if (isStorageFailure(e)) {
                                                        fatalFailure.compareAndSet(null, e);
                                                        queue.clear();
                                                        for (int i = 0; i < threads; i++) {
                                                            queue.offer(Collections.emptyList());
                                                        }
                                                        break;
                                                    }
                                                    stats.recordError(BoundaryImportStatistics.Stage.PROCESSING_RELATIONS,
                                                            BoundaryImportStatistics.Kind.GEOMETRY, stub.osmId(), "process-relation", e);
                                                }
                                            }
                                        }
                                    }
                                } catch (RocksDBException e) {
                                    fatalFailure.compareAndSet(null, e);
                                } finally {
                                    stats.decrementActiveThreads();
                                }
                            }));
                        }

                        producer.join();
                        for (Future<?> f : futures) {
                            f.get();
                        }
                        executor.shutdown();
                        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

                        if (fatalFailure.get() != null) {
                            throw new RuntimeException("Import failed while processing relations", fatalFailure.get());
                        }

                        // Merge per-thread H3 DBs into the final h3_to_osm
                        stats.setCurrentPhase(3, BoundaryImportStatistics.PHASE_MERGING);
                        stats.setMergedThreadCount(threads);
                        for (int t = 0; t < threads; t++) {
                            if (Files.exists(threadH3Paths[t])) {
                                try (RocksDB threadDb = RocksDB.open(cacheOpts, threadH3Paths[t].toString())) {
                                    copyH3Db(threadDb, h3ToOsm);
                                }
                                stats.incrementMergedThreadDatabasesProcessed();
                                cleanup(threadH3Paths[t]);
                            }
                        }
                    }

                }

                stats.setCurrentPhase(3, BoundaryImportStatistics.PHASE_HIERARCHY);
                RegionHierarchyWriter.HierarchyResult hierarchyResult =
                        new RegionHierarchyWriter().write(hierarchyTsv, tmpRegionMeta, tmpRegionGeom, h3ToOsm, h3,
                                                         isoCodes, subareaEdges, countryHints,
                                                         regionByIso3166_2, countryIsoByName,
                                                         countryStubs, stats);
                stats.setHierarchyPublished(hierarchyResult.regionsWritten());
                writeCountrySidecar(out.resolve("region_countries.tsv"), countryStubs,
                                    importedRelationIds, skippedRelationReasons,
                                    hierarchyResult.placeholderCountryIds());

                if (!hierarchyResult.unassignedIds().isEmpty()) {
                    stats.setCurrentPhase(3, BoundaryImportStatistics.PHASE_PRUNING);
                    pruneUnassigned(hierarchyResult.unassignedIds(), tmpRegionMeta, tmpRegionGeom, h3ToOsm,
                                    nameSql, stats);
                }

                stats.setCurrentPhase(3, BoundaryImportStatistics.PHASE_COMPACTING);
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
                fileSize(nameSql),
                fileSize(hierarchyTsv)
        );

        stats.stop();
        stats.setTotalTime(stats.getElapsedMillis());
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
                this.stats.incrementMergedEntriesProcessed();
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

    private void processRelationStub(RelationStub stub,
                                     RocksDB nodeCache,
                                     RocksDB wayCache,
                                     RocksDB threadH3,
                                     WriteOptions wo,
                                     RocksDB tmpRegionMeta,
                                     RocksDB tmpRegionGeom) {
        if (stub.adminLevel() <= 3) {
            logger.debug("Processing relation OSM ID: {} [Admin Level: {}]", stub.osmId(), stub.adminLevel());
        }
        try {
            Geometry geom = buildMultiPolygon(stub, nodeCache, wayCache);
            if (geom == null || geom.isEmpty()) {
                logSkip(stub, "has no usable geometry (missing ways/nodes or open rings)");
                return;
            }

            if (!geom.isValid()) {
                logger.debug("Relation OSM ID: {} [Admin Level: {}] Geometry is invalid, attempting repair", stub.osmId(), stub.adminLevel());
                geom = geom.buffer(0);
                if (geom == null || geom.isEmpty() || !geom.isValid()) {
                    logger.error("Relation OSM ID: {} [Admin Level: {}] Geometry repair failed", stub.osmId(), stub.adminLevel());
                    skippedRelationReasons.put(stub.osmId(), "geometry repair failed");
                    stats.incrementSkippedRelations(stub.adminLevel());
                    return;
                }
            }

            Geometry simplified = geometrySimplificationService.simplifyByAdminLevel(geom, stub.adminLevel());
            if (simplified == null || simplified.isEmpty()) {
                simplified = geom;
            }
            Geometry buffered = simplified.buffer(BUFFER_DISTANCE);
            if (stub.adminLevel() <= 3) {
                logger.debug("Simplified Geometry: {} points for OSM ID: {}", simplified.getNumPoints(), stub.osmId());
            }

            int resolution = getResolutionForAdminLevel(stub.adminLevel());
            AtomicLong cellCount = new AtomicLong(0);
            long startTime = System.currentTimeMillis();
            processCellsH3Stream(buffered, stub.osmId(), wo, threadH3, cellCount, resolution);
            if (stub.adminLevel() <= 3) {
                logger.debug("H3 Polyfill (Res {}) took {}ms for OSM ID: {}", resolution, System.currentTimeMillis() - startTime, stub.osmId());
            }
            long totalCells = cellCount.get();

            if (totalCells == 0) {
                for (int fallbackResolution : FALLBACK_RESOLUTIONS) {
                    if (fallbackResolution <= resolution) {
                        continue;
                    }
                    cellCount.set(0);
                    processCellsH3Stream(buffered, stub.osmId(), wo, threadH3, cellCount, fallbackResolution);
                    totalCells = cellCount.get();
                    if (totalCells > 0) {
                        stats.incrementResolutionFallback();
                        resolution = fallbackResolution;
                        break;
                    }
                }
            }
            if (totalCells == 0) {
                int fallbackResolution = FALLBACK_RESOLUTIONS[FALLBACK_RESOLUTIONS.length - 1];
                totalCells = writeInteriorPointCell(buffered, stub.osmId(), wo, threadH3, fallbackResolution);
                if (totalCells > 0) {
                    stats.incrementInteriorPointFallback();
                    resolution = fallbackResolution;
                }
            }
            if (totalCells == 0) {
                logSkip(stub, "produced no H3 cells");
                return;
            }

            stats.incrementRelationsProcessed();
            importedRelationIds.add(stub.osmId());
            stats.addH3CellsGenerated(totalCells);

            startTime = System.currentTimeMillis();
            tmpRegionMeta.put(wo, longToBytes(stub.osmId()), cellMetaToBytes(totalCells, resolution, stub.adminLevel()));
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
            if (isStorageFailure(e)) {
                throw new RuntimeException("Storage failure while processing relation " + stub.osmId()
                                                   + "; aborting instead of writing an incomplete bundle", e);
            }
            stats.recordError(BoundaryImportStatistics.Stage.PROCESSING_RELATIONS,
                              BoundaryImportStatistics.Kind.GEOMETRY, stub.osmId(), "process-relation", e);
            if (!importedRelationIds.contains(stub.osmId())) {
                logSkip(stub, "geometry processing failed: " + e.getMessage());
            }
        }
    }

    /**
     * Skipped relations are counted in the statistics output instead of being logged
     * line by line; the final report includes a breakdown by admin level.
     */
    private void logSkip(RelationStub stub, String reason) {
        skippedRelationReasons.put(stub.osmId(), reason);
        stats.incrementSkippedRelations(stub.adminLevel());
    }

    /**
     * Storage failures (RocksDB/IO) are not data problems: continuing would silently
     * write an incomplete bundle and slow the run down. They abort the import instead.
     */
    private static boolean isStorageFailure(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c instanceof RocksDBException || c instanceof java.io.IOException) {
                return true;
            }
            if (c.getCause() == c) {
                break;
            }
        }
        return false;
    }

    /**
     * Builds a JTS MultiPolygon from relation outer/inner way members.
     * Rings are stitched by coordinate continuation (same logic as ImportService.buildConnectedRings).
     */
    private Geometry buildMultiPolygon(RelationStub stub, RocksDB nodeCache, RocksDB wayCache) {
        List<Geometry> outerLines = new ArrayList<>(stub.outerWays().size());
        collectWayLines(stub.outerWays(), nodeCache, wayCache, outerLines);
        if (outerLines.isEmpty()) {
            return null;
        }
        List<Geometry> innerLines = new ArrayList<>(stub.innerWays().size());
        collectWayLines(stub.innerWays(), nodeCache, wayCache, innerLines);
        return assemble(outerLines, innerLines);
    }

    /**
     * Builds the polygon geometry from boundary lines. Lines are noded before polygonizing so that
     * separate rings which touch at a single point (e.g. Austria's exclave Jungholz) stay separate;
     * hand-stitching them by endpoint matching merged them into one self-intersecting ring, which
     * the polygonizer then discarded.
     */
    static Geometry assemble(Collection<? extends Geometry> outerLines, Collection<? extends Geometry> innerLines) {
        Geometry outer = polygonize(outerLines);
        if (outer == null) {
            return null;
        }
        if (innerLines.isEmpty()) {
            return outer;
        }
        Geometry inner = polygonize(innerLines);
        if (inner == null) {
            return outer;
        }

        try {
            Geometry result = outer.difference(inner);
            if (result != null && !result.isValid()) {
                result = GeometryFixer.fix(result);
            }
            return polygonalOnly(result);
        } catch (TopologyException firstFailure) {
            // Polygonizer can produce an invalid polygon when rings touch or overlap at a
            // single node. Repairing is expensive, so only do it when the overlay actually
            // fails instead of paying that cost for every relation.
            Geometry fixedOuter = outer.isValid() ? outer : GeometryFixer.fix(outer);
            Geometry fixedInner = inner.isValid() ? inner : GeometryFixer.fix(inner);
            Geometry result = fixedOuter.difference(fixedInner);
            if (result != null && !result.isValid()) {
                result = GeometryFixer.fix(result);
            }
            return polygonalOnly(result);
        }
    }

    private static Geometry polygonalOnly(Geometry geometry) {
        if (geometry == null || geometry.isEmpty()) {
            return null;
        }
        if (geometry instanceof Polygon || geometry instanceof MultiPolygon) {
            return geometry;
        }
        List<Polygon> polygons = new ArrayList<>();
        for (int i = 0; i < geometry.getNumGeometries(); i++) {
            Geometry part = geometry.getGeometryN(i);
            if (part instanceof Polygon polygon) {
                polygons.add(polygon);
            } else if (part instanceof MultiPolygon multiPolygon) {
                for (int j = 0; j < multiPolygon.getNumGeometries(); j++) {
                    polygons.add((Polygon) multiPolygon.getGeometryN(j));
                }
            }
        }
        if (polygons.isEmpty()) {
            return null;
        }
        return polygons.size() == 1
                ? polygons.getFirst()
                : GEOMETRY_FACTORY.createMultiPolygon(polygons.toArray(new Polygon[0]));
    }

    /**
     * Polygonizes lines into the polygons they enclose (holes are handled by {@link #assemble}).
     */
    static Geometry polygonize(Collection<? extends Geometry> lines) {
        Geometry noded = UnaryUnionOp.union(lines);
        Polygonizer polygonizer = new Polygonizer();
        polygonizer.add(noded);
        @SuppressWarnings("unchecked")
        Collection<Polygon> polygons = polygonizer.getPolygons();
        if (polygons.isEmpty()) {
            return null;
        }
        return polygons.size() == 1
                ? polygons.iterator().next()
                : GEOMETRY_FACTORY.createMultiPolygon(polygons.toArray(new Polygon[0]));
    }

    private int collectWayLines(List<Long> wayIds, RocksDB nodeCache, RocksDB wayCache, List<Geometry> target) {
        int added = 0;
        for (long wid : wayIds) {
            byte[] seq;
            try {
                seq = wayCache.get(longToBytes(wid));
            } catch (RocksDBException e) {
                throw new RuntimeException("Failed to read way " + wid + " from the way cache", e);
            }
            if (seq == null) {
                continue;
            }
            long[] nodeIds = bytesToLongArray(seq);
            List<Coordinate> coords = resolveCoordinates(nodeIds, nodeCache);
            if (coords != null && coords.size() >= 2) {
                target.add(GEOMETRY_FACTORY.createLineString(coords.toArray(new Coordinate[0])));
                added++;
            }
        }
        return added;
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
            throw new RuntimeException("Failed to read node coordinates from the node cache", e);
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
     * Last-resort fallback for degenerate or sub-cell geometries: writes the single H3 cell
     * that contains the geometry's interior point so the region still appears in the hierarchy.
     */
    private long writeInteriorPointCell(Geometry geometry, long osmId, WriteOptions wo, RocksDB threadH3ToOsm, int resolution) {
        Point point;
        try {
            point = geometry.getInteriorPoint();
        } catch (Exception e) {
            point = geometry.getCentroid();
        }
        if (point == null || point.isEmpty()) {
            return 0;
        }
        long cell = h3.latLngToCell(point.getY(), point.getX(), resolution);
        AtomicLong written = new AtomicLong(0);
        processH3Batch(List.of(cell), osmId, wo, threadH3ToOsm, written);
        return written.get();
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

    private byte[] cellMetaToBytes(long cellCount, int resolution, int adminLevel) {
        return ByteBuffer.allocate(16).order(ByteOrder.BIG_ENDIAN)
                .putLong(cellCount)
                .putInt(resolution)
                .putInt(adminLevel)
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

    boolean isAdministrativeBoundary(OsmRelation r) {
        boolean area = false, adminLevel = false, isoCountry = false, linear = false;
        for (int i = 0; i < r.getNumberOfTags(); i++) {
            OsmTag t = r.getTag(i);
            if ("boundary".equals(t.getKey())
                    && ("administrative".equals(t.getValue()) || "disputed".equals(t.getValue()))) {
                area = true;
            }
            if ("admin_level".equals(t.getKey())) adminLevel = true;
            if ("type".equals(t.getKey())
                    && ("multilinestring".equals(t.getValue())
                        || "linestring".equals(t.getValue())
                        || "route".equals(t.getValue()))) {
                linear = true;
            }
            if ("ISO3166-1".equals(t.getKey()) || "ISO3166-1:alpha2".equals(t.getKey())) isoCountry = true;
        }
        if (linear) {
            return false;
        }
        // Country-level relations are sometimes tagged boundary=disputed (e.g. Palestine)
        // and may omit admin_level. Keep them so their subareas can be linked.
        return area && (adminLevel || isoCountry);
    }

    RelationStub buildRelationStub(OsmRelation r) {
        List<Long> outer = new ArrayList<>();
        List<Long> inner = new ArrayList<>();
        List<Long> subareas = new ArrayList<>();
        int level = 10;
        boolean hasAdminLevel = false;
        String isoCode = null;
        String isoAlpha2 = null;
        String name = null;
        String nameEn = null;
        String iso3166_2 = null;
        String countryCode = null;
        String countryName = null;
        for (int i = 0; i < r.getNumberOfMembers(); i++) {
            OsmRelationMember m = r.getMember(i);
            String role = m.getRole();
            if (m.getType() == EntityType.Way) {
                if ("outer".equals(role) || role == null || role.isEmpty()) outer.add(m.getId());
                else if ("inner".equals(role)) inner.add(m.getId());
            } else if (m.getType() == EntityType.Relation
                    && ("subarea".equals(role) || role == null || role.isEmpty())) {
                subareas.add(m.getId());
            }
        }
        for (int i = 0; i < r.getNumberOfTags(); i++) {
            OsmTag t = r.getTag(i);
            if ("admin_level".equals(t.getKey())) {
                try {
                    level = Integer.parseInt(t.getValue());
                    hasAdminLevel = true;
                } catch (NumberFormatException ignored) {
                }
            } else if ("ISO3166-1".equals(t.getKey())) {
                isoCode = normalizeIsoCode(t.getValue());
            } else if ("ISO3166-1:alpha2".equals(t.getKey())) {
                isoAlpha2 = normalizeIsoCode(t.getValue());
            } else if ("name".equals(t.getKey())) {
                name = t.getValue();
            } else if ("name:en".equals(t.getKey())) {
                nameEn = t.getValue();
            } else if ("ISO3166-2".equals(t.getKey())) {
                iso3166_2 = t.getValue();
            } else if ("addr:country".equals(t.getKey())) {
                countryCode = normalizeCountryCode(t.getValue());
            } else if ("is_in:country_code".equals(t.getKey())) {
                if (countryCode == null) countryCode = normalizeCountryCode(t.getValue());
            } else if ("is_in:country".equals(t.getKey())) {
                countryName = t.getValue();
            }
        }
        String iso = isoAlpha2 != null ? isoAlpha2 : isoCode;
        if (!hasAdminLevel && iso != null) {
            level = 2;
        }
        return new RelationStub(r.getId(), level, iso, name, nameEn, iso3166_2, countryCode, countryName, outer, inner, subareas);
    }

    private static String normalizeCountryName(String value) {
        return value == null ? null : value.trim().toLowerCase(Locale.ROOT);
    }

    private String normalizeCountryCode(String value) {
        if (value == null) return null;
        String candidate = value.trim().toUpperCase(Locale.ROOT);
        return candidate.length() == 2 && candidate.chars().allMatch(Character::isLetter) ? candidate : null;
    }

    private String normalizeIsoCode(String value) {
        if (value == null) {
            return null;
        }
        for (String part : value.split("[;,]", -1)) {
            String candidate = part.trim().toUpperCase(Locale.ROOT);
            if (candidate.length() == 2 && candidate.chars().allMatch(Character::isLetter)) {
                return candidate;
            }
        }
        return null;
    }

    record RelationStub(long osmId, int adminLevel, String isoCode, String name, String nameEn,
                        String iso3166_2, String countryCode, String countryName,
                        List<Long> outerWays, List<Long> innerWays, List<Long> subareaRelations) {
    }

    record CountryStub(long osmId, String isoCode, String name) {
    }

    record CountryHints(String iso3166_2, String countryCode, String countryName, String countryNameEn) {
        boolean hasAny() {
            return iso3166_2 != null || countryCode != null || countryName != null || countryNameEn != null;
        }
    }

    private void pruneUnassigned(Set<Long> unassigned, RocksDB tmpRegionMeta, RocksDB tmpRegionGeom,
                                   RocksDB h3ToOsm, Path osmNames, BoundaryImportStatistics stats) throws Exception {
        try (WriteBatch batch = new WriteBatch(); WriteOptions wo = new WriteOptions().setDisableWAL(true)) {
            for (Long osmId : unassigned) {
                batch.delete(longToBytes(osmId));
            }
            tmpRegionMeta.write(wo, batch);
            tmpRegionGeom.write(wo, batch);
        }
        removeOsmIdsFromH3(h3ToOsm, unassigned, stats);
        filterOsmNames(osmNames, unassigned);
    }

    private void removeOsmIdsFromH3(RocksDB h3ToOsm, Set<Long> unassigned,
                                    BoundaryImportStatistics stats) throws RocksDBException {
        try (RocksIterator iterator = h3ToOsm.newIterator();
             WriteBatch batch = new WriteBatch();
             WriteOptions wo = new WriteOptions().setDisableWAL(true)) {
            iterator.seekToFirst();
            int pending = 0;
            while (iterator.isValid()) {
                ByteBuffer buffer = ByteBuffer.wrap(iterator.value()).order(ByteOrder.BIG_ENDIAN);
                List<Long> keep = new ArrayList<>();
                boolean changed = false;
                while (buffer.hasRemaining()) {
                    long osmId = buffer.getLong();
                    if (unassigned.contains(osmId)) {
                        changed = true;
                    } else {
                        keep.add(osmId);
                    }
                }
                if (changed) {
                    byte[] key = iterator.key().clone();
                    if (keep.isEmpty()) {
                        batch.delete(key);
                    } else {
                        ByteBuffer value = ByteBuffer.allocate(keep.size() * Long.BYTES).order(ByteOrder.BIG_ENDIAN);
                        for (Long osmId : keep) {
                            value.putLong(osmId);
                        }
                        batch.put(key, value.array());
                    }
                    pending++;
                    if (pending >= 10_000) {
                        h3ToOsm.write(wo, batch);
                        batch.clear();
                        pending = 0;
                    }
                }
                iterator.next();
            }
            h3ToOsm.write(wo, batch);
        }
    }

    private void filterOsmNames(Path osmNames, Set<Long> unassigned) throws IOException {
        if (unassigned.isEmpty() || !Files.exists(osmNames)) {
            return;
        }
        Path filtered = osmNames.resolveSibling(osmNames.getFileName() + ".filtered");
        try (var reader = Files.newBufferedReader(osmNames, StandardCharsets.UTF_8);
             BufferedWriter writer = Files.newBufferedWriter(filtered, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                int tab = line.indexOf('\t');
                if (tab < 0) {
                    writer.write(line);
                    writer.newLine();
                    continue;
                }
                try {
                    long osmId = Long.parseLong(line.substring(0, tab));
                    if (unassigned.contains(osmId)) {
                        continue;
                    }
                } catch (NumberFormatException ignored) {
                    // keep malformed lines; they are not our concern here
                }
                writer.write(line);
                writer.newLine();
            }
        }
        Files.move(filtered, osmNames, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
    }

    private void writeCountrySidecar(Path output,
                                     Map<Long, CountryStub> countryStubs,
                                     Set<Long> importedRelationIds,
                                     Map<Long, String> skipReasons,
                                     Set<Long> placeholderCountryIds) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
            for (CountryStub country : countryStubs.values()) {
                String status;
                if (importedRelationIds.contains(country.osmId())) {
                    status = "imported";
                } else if (placeholderCountryIds.contains(country.osmId())) {
                    status = "placeholder";
                } else {
                    status = "skipped";
                }
                String reason = status.equals("skipped")
                        ? skipReasons.getOrDefault(country.osmId(), "unknown")
                        : "";
                writer.write(Long.toString(country.osmId()));
                writer.write('\t');
                writer.write(country.isoCode() == null ? "" : country.isoCode());
                writer.write('\t');
                writer.write(country.name() == null ? "" : country.name());
                writer.write('\t');
                writer.write(status);
                writer.write('\t');
                writer.write(reason);
                writer.write('\n');
            }
        }
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
