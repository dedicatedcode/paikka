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
import com.dedicatedcode.paikka.flatbuffers.*;
import com.dedicatedcode.paikka.flatbuffers.Geometry;
import com.dedicatedcode.paikka.service.PaikkaMetadata;
import com.dedicatedcode.paikka.service.S2Helper;
import com.dedicatedcode.paikka.service.importer.ImportStatistics.Kind;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2LatLngRect;
import com.google.common.geometry.S2RegionCoverer;
import com.google.flatbuffers.FlatBufferBuilder;
import de.topobyte.osm4j.core.access.OsmIterator;
import de.topobyte.osm4j.core.model.iface.*;
import de.topobyte.osm4j.pbf.seq.PbfIterator;
import org.locationtech.jts.algorithm.construct.MaximumInscribedCircle;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.WKBWriter;
import org.rocksdb.*;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

@Service
public class ImportService {

    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
    private final S2Helper s2Helper;
    private final GeometrySimplificationService geometrySimplificationService;
    private final PaikkaConfiguration config;

    private final Map<String, String> tagCache = new ConcurrentHashMap<>(1000);

    private final int fileReadWindowSize;

    private final AtomicLong sequence = new AtomicLong(0);
    private final AtomicLong buildingSequence = new AtomicLong(0);

    public ImportService(S2Helper s2Helper, GeometrySimplificationService geometrySimplificationService, PaikkaConfiguration config) {
        this.s2Helper = s2Helper;
        this.geometrySimplificationService = geometrySimplificationService;
        this.config = config;
        this.fileReadWindowSize = calculateFileReadWindowSize();
    }

    private int calculateFileReadWindowSize() {
        long maxHeap = Runtime.getRuntime().maxMemory();
        if (maxHeap > 24L * 1024 * 1024 * 1024) {
            return 256 * 1024 * 1024;
        } else if (maxHeap > 8L * 1024 * 1024 * 1024) {
            return 96 * 1024 * 1024;
        } else {
            return 48 * 1024 * 1024;
        }
    }

    public void importData(String pbfFilePath, String dataDir) throws Exception {
        long totalStartTime = System.currentTimeMillis();
        printHeader(pbfFilePath, dataDir);

        Path pbfFile = Paths.get(pbfFilePath);
        Path dataDirectory = Paths.get(dataDir);
        Path tmpDirectory = dataDirectory.resolve("tmp");
        dataDirectory.toFile().mkdirs();
        tmpDirectory.toFile().mkdirs();

        Path shardsDbPath = dataDirectory.resolve("poi_shards");
        Path boundariesDbPath = dataDirectory.resolve("boundaries");
        Path buildingsDbPath = dataDirectory.resolve("buildings_shards");
        Path appendBuildingDbPath = dataDirectory.resolve("tmp/append_building");
        Path gridIndexDbPath = dataDirectory.resolve("tmp/grid_index");
        Path buildingGridIndexDbPath = dataDirectory.resolve("tmp/building_grid_index");
        Path appendDbPath = dataDirectory.resolve("tmp/append_poi");
        Path nodeCacheDbPath = dataDirectory.resolve("tmp/node_cache");
        Path wayIndexDbPath = dataDirectory.resolve("tmp/way_index");
        Path boundaryWayIndexDbPath = dataDirectory.resolve("tmp/boundary_way_index");
        Path neededNodesDbPath = dataDirectory.resolve("tmp/needed_nodes");
        Path relIndexDbPath = dataDirectory.resolve("tmp/rel_index");
        Path poiIndexDbPath = dataDirectory.resolve("tmp/poi_index");

        cleanupDatabase(shardsDbPath);
        cleanupDatabase(boundariesDbPath);
        cleanupDatabase(buildingsDbPath);
        cleanupDatabase(appendBuildingDbPath);
        cleanupDatabase(gridIndexDbPath);
        cleanupDatabase(buildingGridIndexDbPath);
        cleanupDatabase(nodeCacheDbPath);
        cleanupDatabase(wayIndexDbPath);
        cleanupDatabase(boundaryWayIndexDbPath);
        cleanupDatabase(neededNodesDbPath);
        cleanupDatabase(relIndexDbPath);
        cleanupDatabase(poiIndexDbPath);
        cleanupDatabase(appendDbPath);

        RocksDB.loadLibrary();
        ImportStatistics stats = new ImportStatistics();
        stats.startProgressReporter();

        try (Cache sharedCache = new LRUCache(2 * 1024 * 1024 * 1024L)) {
            BlockBasedTableConfig tableConfig = new BlockBasedTableConfig()
                    .setBlockCache(sharedCache)
                    .setBlockSize(64 * 1024)
                    .setEnableIndexCompression(true)
                    .setFilterPolicy(new BloomFilter(10, false));


            Options poiShardsOpts = new Options()
                    .setCreateIfMissing(true)
                    .setTableFormatConfig(tableConfig)
                    .setCompressionType(CompressionType.ZSTD_COMPRESSION)
                    .setMaxOpenFiles(-1);

            Options gridOpts = new Options()
                    .setCreateIfMissing(true)
                    .setTableFormatConfig(tableConfig)
                    .setCompressionType(CompressionType.LZ4_COMPRESSION)
                    .setWriteBufferSize(256 * 1024 * 1024);

            Options nodeOpts = new Options()
                    .setCreateIfMissing(true)
                    .setTableFormatConfig(tableConfig)
                    .setCompressionType(CompressionType.LZ4_COMPRESSION)
                    .setWriteBufferSize(512 * 1024 * 1024)
                    .setMaxWriteBufferNumber(3)
                    .setLevel0FileNumCompactionTrigger(4);

            Options appendOpts = new Options()
                    .setCreateIfMissing(true)
                    .setTableFormatConfig(tableConfig)
                    .setCompressionType(CompressionType.LZ4_COMPRESSION);

            Options poiIndexOpts = new Options()
                    .setCreateIfMissing(true)
                    .setTableFormatConfig(tableConfig)
                    .setCompressionType(CompressionType.LZ4_COMPRESSION)
                    .setWriteBufferSize(512 * 1024 * 1024)
                    .setMaxWriteBufferNumber(3)
                    .setLevel0FileNumCompactionTrigger(4);

            Options wayIndexOpts = new Options()
                    .setCreateIfMissing(true)
                    .setTableFormatConfig(tableConfig)
                    .setCompressionType(CompressionType.LZ4_COMPRESSION)
                    .setWriteBufferSize(512 * 1024 * 1024)
                    .setMaxWriteBufferNumber(3)
                    .setLevel0FileNumCompactionTrigger(4);

            Options neededNodesOpts = new Options()
                    .setCreateIfMissing(true)
                    .setTableFormatConfig(tableConfig)
                    .setCompressionType(CompressionType.LZ4_COMPRESSION)
                    .setWriteBufferSize(512 * 1024 * 1024)
                    .setMaxWriteBufferNumber(3)
                    .setLevel0FileNumCompactionTrigger(4);

            try (RocksDB shardsDb = RocksDB.open(poiShardsOpts, shardsDbPath.toString());
                 RocksDB boundariesDb = RocksDB.open(poiShardsOpts, boundariesDbPath.toString());
                 RocksDB buildingsDb = RocksDB.open(poiShardsOpts, buildingsDbPath.toString());
                 RocksDB appendBuildingDb = RocksDB.open(appendOpts, appendBuildingDbPath.toString());
                 RocksDB gridIndexDb = RocksDB.open(gridOpts, gridIndexDbPath.toString());
                 RocksDB buildingGridIndexDb = RocksDB.open(gridOpts, buildingGridIndexDbPath.toString());
                 RocksDB nodeCache = RocksDB.open(nodeOpts, nodeCacheDbPath.toString());
                 RocksDB wayIndexDb = RocksDB.open(wayIndexOpts, wayIndexDbPath.toString());
                 RocksDB neededBoundaryWaysDb = RocksDB.open(wayIndexOpts, boundaryWayIndexDbPath.toString());
                 RocksDB neededNodesDb = RocksDB.open(neededNodesOpts, neededNodesDbPath.toString());
                 RocksDB relIndexDb = RocksDB.open(wayIndexOpts, relIndexDbPath.toString());
                 RocksDB poiIndexDb = RocksDB.open(poiIndexOpts, poiIndexDbPath.toString());
                 RocksDB appendDb = RocksDB.open(appendOpts, appendDbPath.toString())) {

                // PASS 1: Discovery & Indexing
                stats.printPhaseHeader("PASS 1: Discovery & Indexing");
                long pass1Start = System.currentTimeMillis();
                stats.setCurrentPhase(1, "1.1.1: Discovery & Indexing");
                pass1DiscoveryAndIndexing(pbfFile, wayIndexDb, neededBoundaryWaysDb, neededNodesDb, relIndexDb, poiIndexDb, stats);
                stats.setCurrentPhase(2, "1.1.2: Indexing boundary member ways");
                indexBoundaryMemberWays(pbfFile, neededBoundaryWaysDb, wayIndexDb, neededNodesDb, stats);
                stats.printPhaseSummary("PASS 1", pass1Start);

                // PASS 2: Nodes Cache, Boundaries, POIs
                stats.printPhaseHeader("PASS 2: Nodes Cache, Boundaries, POIs");
                long pass2Start = System.currentTimeMillis();
                stats.setCurrentPhase(3, "1.2: Caching node coordinates");
                cacheNeededNodeCoordinates(pbfFile, neededNodesDb, nodeCache, stats);

                stats.setCurrentPhase(4, "1.3: Processing administrative boundaries");
                processAdministrativeBoundariesFromIndex(relIndexDb, nodeCache, wayIndexDb, gridIndexDb, boundariesDb, stats);

                stats.setCurrentPhase(5, "1.4: Processing building boundaries");
                processBuildingBoundariesFromIndex(poiIndexDb, nodeCache, wayIndexDb, buildingGridIndexDb, appendBuildingDb, stats);

                stats.setCurrentPhase(6, "2.1: Processing POIs & Sharding");
                pass2PoiShardingFromIndex(nodeCache, wayIndexDb, appendDb, boundariesDb, poiIndexDb, gridIndexDb, stats);

                stats.setCurrentPhase(7, "2.2: Compacting POIs");
                compactShards(appendDb, shardsDb, stats);
                stats.setCurrentPhase(8, "2.3: Compacting Buildings");
                compactBuildingShards(appendBuildingDb, buildingsDb, stats);
                stats.stop();
                stats.printPhaseSummary("PASS 2", pass2Start);

                shardsDb.compactRange();
                boundariesDb.compactRange();
                buildingsDb.compactRange();

                stats.setTotalTime(System.currentTimeMillis() - totalStartTime);


                recordSizeMetrics(stats,
                                  shardsDbPath,
                                  boundariesDbPath,
                                  buildingsDbPath,
                                  appendBuildingDbPath,
                                  gridIndexDbPath,
                                  buildingGridIndexDbPath,
                                  nodeCacheDbPath,
                                  wayIndexDbPath,
                                  boundaryWayIndexDbPath,
                                  neededNodesDbPath,
                                  relIndexDbPath,
                                  poiIndexDbPath,
                                  appendDbPath);

                shardsDb.flush(new FlushOptions().setWaitForFlush(true));
                boundariesDb.flush(new FlushOptions().setWaitForFlush(true));
                stats.printFinalStatistics();
                stats.printOutcomeAndErrors();
                writeMetadataFile(pbfFile, dataDirectory);
            } catch (Exception e) {
                stats.stop();
                stats.printError("IMPORT FAILED: " + e.getMessage());
                stats.recordError(ImportStatistics.Stage.OVERALL, Kind.OVERALL, null, "fatal", e);
                stats.printOutcomeAndErrors();
            }
            try {
                cleanupDatabase(tmpDirectory);
                System.out.println("\n\033[1;90mTemporary databases deleted.\033[0m");
            } catch (Exception e) {
                System.err.println("Warning: Failed to delete temporary databases: " + e.getMessage());
                stats.recordError(ImportStatistics.Stage.OVERALL, Kind.STORE, null, "cleanup-tmp-db", e);
            }
        }
    }

    private void writeMetadataFile(Path pbfFile, Path dataDirectory) throws IOException {
        Path metadataPath = dataDirectory.resolve("paikka_metadata.json");
        Instant now = Instant.now();
        String importTimestamp = DateTimeFormatter.ISO_INSTANT.format(now);
        String dataVersion = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss").withZone(ZoneOffset.UTC).format(now);
        ObjectMapper objectMapper = new ObjectMapper();
        PaikkaMetadata metadata = new PaikkaMetadata(importTimestamp, dataVersion, pbfFile.getFileName().toString(), S2Helper.GRID_LEVEL, "1.0.0");

        objectMapper.writeValue(metadataPath.toFile(), metadata);
        System.out.println("\n\033[1;32mMetadata file written to: " + metadataPath + "\033[0m");
    }

    private void indexBoundaryMemberWays(Path pbfFile,
                                         RocksDB neededBoundaryWaysDb,
                                         RocksDB wayIndexDb,
                                         RocksDB neededNodesDb,
                                         ImportStatistics stats) throws Exception {
        stats.resetBoundaryPhaseEntitiesRead();
        final byte[] ONE = new byte[]{1};
        try (RocksBatchWriter wayWriter = new RocksBatchWriter(wayIndexDb, 10_000, stats);
             RocksBatchWriter neededWriter = new RocksBatchWriter(neededNodesDb, 500_000, stats)) {

            withPbfIterator(pbfFile, iterator -> {

                while (iterator.hasNext()) {
                    EntityContainer container = iterator.next();
                    stats.incrementBoundaryPhaseEntitiesRead();
                    if (container.getType() != EntityType.Way) continue;

                    OsmWay way = (OsmWay) container.getEntity();
                    byte[] wayKey = s2Helper.longToByteArray(way.getId());

                    // Only process ways that are needed AND not already indexed
                    if (neededBoundaryWaysDb.get(wayKey) == null) continue;
                    if (wayIndexDb.get(wayKey) != null) { continue; }
                    int n = way.getNumberOfNodes();
                    long[] nodeIds = new long[n];
                    for (int j = 0; j < n; j++) {
                        long nid = way.getNodeId(j);
                        nodeIds[j] = nid;
                        neededWriter.put(s2Helper.longToByteArray(nid), ONE);
                    }
                    wayWriter.put(wayKey, s2Helper.longArrayToByteArray(nodeIds));
                    stats.incrementBoundaryWaysProcessed();
                }
            });

            wayWriter.flush();
            neededWriter.flush();
        }
    }

    private void pass1DiscoveryAndIndexing(Path pbfFile, RocksDB wayIndexDb, RocksDB boundaryWayIndexDb, RocksDB neededNodesDb, RocksDB relIndexDb, RocksDB poiIndexDb, ImportStatistics stats) throws Exception {

        final byte[] ONE = new byte[]{1};
        try (RocksBatchWriter wayWriter = new RocksBatchWriter(wayIndexDb, 10_000, stats);
             RocksBatchWriter boundaryWayWriter = new RocksBatchWriter(boundaryWayIndexDb, 10_000, stats);
             RocksBatchWriter neededWriter = new RocksBatchWriter(neededNodesDb, 500_000, stats);
             RocksBatchWriter relWriter = new RocksBatchWriter(relIndexDb, 2_000, stats);
             RocksBatchWriter poiWriter = new RocksBatchWriter(poiIndexDb, 20_000, stats)) {

            withPbfIterator(pbfFile, iterator -> {
                while (iterator.hasNext()) {
                    EntityContainer container = iterator.next();
                    stats.incrementEntitiesRead();
                    EntityType type = container.getType();
                    try {
                        if (type == EntityType.Node) {
                            OsmNode node = (OsmNode) container.getEntity();
                            boolean isAddressNode = hasAddressTags(node);

                            if (isPoi(node) || isAddressNode) {
                                PoiIndexRec rec = buildPoiIndexRecFromEntity(node);
                                rec.lat = node.getLatitude();
                                rec.lon = node.getLongitude();
                                byte[] key = buildPoiKey((byte) 'N', node.getId());
                                poiWriter.put(key, encodePoiIndexRec(rec));
                                neededWriter.put(s2Helper.longToByteArray(node.getId()), ONE);
                                stats.incrementNodesFound();
                                if (isAddressNode) {
                                    stats.incrementAddressNodesFound();
                                }
                            }

                        } else if (type == EntityType.Way) {
                            OsmWay way = (OsmWay) container.getEntity();
                            boolean isPoi = isPoi(way);
                            boolean isAdmin = isAdministrativeBoundaryWay(way);

                            if (isPoi || isAdmin) {
                                stats.incrementWaysProcessed();
                                int n = way.getNumberOfNodes();
                                long[] nodeIds = new long[n];
                                for (int j = 0; j < n; j++) {
                                    long nid = way.getNodeId(j);
                                    nodeIds[j] = nid;
                                    neededWriter.put(s2Helper.longToByteArray(nid), ONE);
                                }
                                wayWriter.put(s2Helper.longToByteArray(way.getId()),
                                              s2Helper.longArrayToByteArray(nodeIds));

                                if (isPoi) {
                                    PoiIndexRec rec = buildPoiIndexRecFromEntity(way);
                                    byte[] key = buildPoiKey((byte) 'W', way.getId());
                                    poiWriter.put(key, encodePoiIndexRec(rec));
                                    if ("building".equals(rec.type)) {
                                        stats.incrementBuildingsFound();
                                    }
                                }
                            }
                        } else if (type == EntityType.Relation) {
                            OsmRelation relation = (OsmRelation) container.getEntity();
                            if (isAdministrativeBoundary(relation)) {
                                stats.incrementRelationsFound();
                                RelRec rec = buildRelRec(relation);
                                relWriter.put(s2Helper.longToByteArray(relation.getId()), encodeRelRec(rec));
                                for (long wid : rec.outer) {
                                    boundaryWayWriter.put(s2Helper.longToByteArray(wid), ONE);
                                }
                                for (long wid : rec.inner) {
                                    boundaryWayWriter.put(s2Helper.longToByteArray(wid), ONE);
                                }

                            }
                        }
                    } catch (Exception e) {
                        stats.recordError(ImportStatistics.Stage.SCAN_PBF_STRUCTURE, Kind.STORE, safeEntityId(container), "pass1-indexing", e);
                    }
                }
            });

            wayWriter.flush();
            neededWriter.flush();
            relWriter.flush();
            poiWriter.flush();
        }
    }

    private void pass2PoiShardingFromIndex(RocksDB nodeCache,
                                           RocksDB wayIndexDb,
                                           RocksDB appendDb,
                                           RocksDB boundariesDb,
                                           RocksDB poiIndexDb,
                                           RocksDB gridIndexDb,
                                           ImportStatistics stats) throws Exception {
        AtomicReference<Map<Long, List<PoiData>>> shardBufferRef = new AtomicReference<>(new ConcurrentHashMap<>());
        Runnable flushTask = () -> {
            try {
                Map<Long, List<PoiData>> bufferToFlush = shardBufferRef.getAndSet(new ConcurrentHashMap<>());
                if (!bufferToFlush.isEmpty()) {
                    writeShardBatchAppendOnly(bufferToFlush, appendDb, stats);
                }
            } catch (Exception e) {
                stats.recordError(ImportStatistics.Stage.PROCESSING_POIS_SHARDING, Kind.STORE, null, "flush-append-batch", e);
            }
        };

        try (PeriodicFlusher _ = PeriodicFlusher.start("shard-buffer-flush", 5, 5, flushTask)) {
            BlockingQueue<List<PoiQueueItem>> queue = new LinkedBlockingQueue<>(10000);
            int numReaders = Math.max(1, config.getImportConfiguration().getThreads());
            int chunkSize = config.getImportConfiguration().getChunkSize();
            int localPoiBufferSize = 10_000;

            CountDownLatch latch = new CountDownLatch(numReaders);

            try (ExecutorService executor = createExecutorService(numReaders); ReadOptions ro = new ReadOptions().setReadaheadSize(8 * 1024 * 1024)) {
                com.github.benmanes.caffeine.cache.Cache<Long, HierarchyCache.CachedBoundary> globalBoundaryCache = Caffeine.newBuilder().maximumSize(1000).recordStats().build();
                ThreadLocal<HierarchyCache> hierarchyCacheThreadLocal = ThreadLocal.withInitial(() -> new HierarchyCache(boundariesDb, gridIndexDb, s2Helper, globalBoundaryCache));
                long total = 0;
                try (RocksIterator it = poiIndexDb.newIterator(ro)) {
                    for (it.seekToFirst(); it.isValid(); it.next()) total++;
                }

                long step = Math.max(1, total / numReaders);
                List<byte[]> splitKeys = new ArrayList<>();
                try (RocksIterator it = poiIndexDb.newIterator(ro)) {
                    long i = 0;
                    for (it.seekToFirst(); it.isValid() && splitKeys.size() < numReaders; it.next(), i++) {
                        if (i % step == 0) splitKeys.add(it.key().clone());
                    }
                }
                splitKeys.add(null);
                List<Thread> readerThreads = new ArrayList<>(numReaders);
                for (int t = 0; t < numReaders; t++) {
                    final byte[] startKey = splitKeys.get(t);
                    final byte[] endKey = (t + 1 < splitKeys.size()) ? splitKeys.get(t + 1) : null;
                    Thread readerThread = Thread.ofVirtual().name("PoiReader-" + t).start(() -> {
                        List<PoiQueueItem> chunk = new ArrayList<>(chunkSize);

                        try (RocksIterator it = poiIndexDb.newIterator(ro)) {
                            it.seek(startKey);
                            while (it.isValid()) {
                                byte[] key = it.key();
                                if (endKey != null && Arrays.compareUnsigned(key, endKey) >= 0) {
                                    break;
                                }
                                byte[] value = it.value();
                                byte kind = key[0];
                                long id = bytesToLong(key, 1);
                                PoiIndexRec rec = decodePoiIndexRec(value);
                                stats.incrementPoiIndexRecRead();
                                rec.kind = kind;
                                rec.id = id;

                                // Solution: The old workaround is removed. Enrichment is done at query time.
                                // We no longer try to enrich address nodes during import.

                                // Buildings are processed in a separate phase and not included as point POIs.
                                if ("building".equals(rec.type)) {
                                    it.next();
                                    continue;
                                }

                                // For way POIs, lat/lon is NaN — resolve from nodeCache/wayIndexDb
                                byte[] cacheWayNodes = null;
                                if (Double.isNaN(rec.lat) && kind == 'W') {
                                    cacheWayNodes = resolveWayCenter(rec, nodeCache, wayIndexDb, stats);
                                }

                                if (!Double.isNaN(rec.lat) && !Double.isNaN(rec.lon)) {
                                    PoiQueueItem item = new PoiQueueItem(rec, S2CellId.fromLatLng(S2LatLng.fromDegrees(rec.lat, rec.lon)).id(), cacheWayNodes);
                                    chunk.add(item);
                                }

                                if (chunk.size() >= chunkSize) {
                                    sortAndEmitChunk(chunk, queue, stats);
                                    chunk.clear();
                                }

                                it.next();
                            }

                            // Emit remaining items
                            if (!chunk.isEmpty()) {
                                sortAndEmitChunk(chunk, queue, stats);
                                chunk.clear();
                            }

                        } catch (Exception e) {
                            stats.recordError(ImportStatistics.Stage.PROCESSING_POIS_SHARDING, Kind.READ, null, "poi-reader", e);
                        }
                    });
                    readerThreads.add(readerThread);

                }

                for (int i = 0; i < numReaders; i++) {
                    executor.submit(() -> {
                        stats.incrementActiveThreads();
                        final GeometryFactory geometryFactory = new GeometryFactory();
                        final HierarchyCache hierarchyCache = hierarchyCacheThreadLocal.get();
                        final Map<Long, List<PoiData>> localShardBuffer = new HashMap<>();
                        int localPoiCount = 0;
                        try {
                            while (true) {
                                List<PoiQueueItem> batch = queue.take();
                                stats.setQueueSize(queue.size());
                                if (batch.isEmpty()) break;
                                int localCount = batch.size();
                                for (PoiQueueItem item : batch) {
                                    try {
                                        PoiIndexRec rec = item.rec;
                                        double lat = rec.lat;
                                        double lon = rec.lon;
                                        byte[] boundaryWkb = null;

                                        if (rec.kind == 'W') {
                                            List<Coordinate> coords = buildCoordinatesFromWay(nodeCache, item.cachedWayNodes);
                                            if (coords != null && coords.size() >= 3) {
                                                if (!coords.getFirst().equals2D(coords.getLast())) {
                                                    coords.add(new Coordinate(coords.getFirst()));
                                                }
                                                if (coords.size() >= 4) {
                                                    LinearRing shell = geometryFactory.createLinearRing(coords.toArray(new Coordinate[0]));
                                                    Polygon poly = geometryFactory.createPolygon(shell);
                                                    if (poly.isValid()) {
                                                        boundaryWkb = new WKBWriter().write(poly);
                                                    }
                                                }
                                            }
                                        }

                                        List<HierarchyCache.SimpleHierarchyItem> hierarchy = hierarchyCache.resolve(lon, lat);
                                        PoiData poiData = createPoiDataFromIndex(rec, lat, lon, hierarchy, boundaryWkb);
                                        localShardBuffer.computeIfAbsent(s2Helper.getShardId(lat, lon), k -> new ArrayList<>()).add(poiData);
                                    } catch (Exception e) {
                                        stats.recordError(ImportStatistics.Stage.PROCESSING_POIS_SHARDING, Kind.READ, null, "poi-reader", e);
                                    }
                                    localPoiCount++;
                                }
                                stats.incrementPoisProcessed(localCount);
                                if (localPoiCount > localPoiBufferSize) {
                                    Map<Long, List<PoiData>> currentActiveBuffer = shardBufferRef.get();
                                    localShardBuffer.forEach((shardId, poiList) -> currentActiveBuffer.computeIfAbsent(shardId, k -> new CopyOnWriteArrayList<>()).addAll(poiList));
                                    localShardBuffer.clear();
                                    localPoiCount = 0;
                                }
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } finally {
                            Map<Long, List<PoiData>> currentActiveBuffer = shardBufferRef.get();
                            localShardBuffer.forEach((shardId, poiList) -> currentActiveBuffer.computeIfAbsent(shardId, k -> new CopyOnWriteArrayList<>()).addAll(poiList));
                            localShardBuffer.clear();
                            hierarchyCacheThreadLocal.remove();
                            stats.decrementActiveThreads();
                            latch.countDown();
                        }
                    });
                }
                for (Thread reader : readerThreads) {
                    try {
                        reader.join();
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                    }
                }
                for (int i = 0; i < numReaders; i++) {
                    try {
                        queue.put(Collections.emptyList());
                        stats.setPoiIndexRecReadDone();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
                latch.await();
            }
            flushTask.run();
        }
    }

    private void sortAndEmitChunk(List<PoiQueueItem> chunk, BlockingQueue<List<PoiQueueItem>> queue, ImportStatistics stats) throws InterruptedException {
        chunk.sort((a, b) -> Long.compareUnsigned(a.s2SortKey, b.s2SortKey));

        List<PoiQueueItem> buf = new ArrayList<>(2000);
        for (PoiQueueItem item : chunk) {
            buf.add(item);
            if (buf.size() >= 2000) {
                queue.put(new ArrayList<>(buf));
                buf.clear();
                stats.setQueueSize(queue.size());
            }
        }
        if (!buf.isEmpty()) {
            queue.put(new ArrayList<>(buf));
            buf.clear();
            stats.setQueueSize(queue.size());
        }
    }

    private byte[] resolveWayCenter(PoiIndexRec rec, RocksDB nodeCache, RocksDB wayIndexDb, ImportStatistics stats) {
        try {
            byte[] wayNodes = wayIndexDb.get(s2Helper.longToByteArray(rec.id));
            if (wayNodes == null) return null;
            long[] nids = s2Helper.byteArrayToLongArray(wayNodes);
            if (nids.length == 0) return null;

            List<byte[]> keys = new ArrayList<>(nids.length);
            for (long nid : nids) keys.add(s2Helper.longToByteArray(nid));
            List<byte[]> values = nodeCache.multiGetAsList(keys);

            double minX = Double.MAX_VALUE, maxX = -Double.MAX_VALUE;
            double minY = Double.MAX_VALUE, maxY = -Double.MAX_VALUE;
            int found = 0;

            for (byte[] v : values) {
                if (v != null && v.length == 16) {
                    ByteBuffer bb = ByteBuffer.wrap(v);
                    double lat = bb.getDouble(0);
                    double lon = bb.getDouble(8);
                    if (lat < minY) minY = lat;
                    if (lat > maxY) maxY = lat;
                    if (lon < minX) minX = lon;
                    if (lon > maxX) maxX = lon;
                    found++;
                }
            }

            if (found > 0) {
                rec.lat = (minY + maxY) / 2.0;
                rec.lon = (minX + maxX) / 2.0;
            }
            return wayNodes;
        } catch (Exception e) {
            stats.recordError(ImportStatistics.Stage.PROCESSING_POIS_SHARDING, Kind.READ, rec.id, "resolve-way-center", e);
        }
        return null;
    }

    private void cacheNeededNodeCoordinates(Path pbfFile, RocksDB neededNodesDb, RocksDB nodeCache, ImportStatistics stats) throws Exception {
        final int BATCH_SIZE = 50_000;

        BlockingQueue<List<OsmNode>> nodeBatchQueue = new LinkedBlockingQueue<>(200);
        int numProcessors = Math.max(1, config.getImportConfiguration().getThreads());
        CountDownLatch latch = new CountDownLatch(numProcessors);

        try (ExecutorService executor = createExecutorService(numProcessors)) {
            Thread reader = Thread.ofVirtual().start(() -> {
                List<OsmNode> buf = new ArrayList<>(BATCH_SIZE);
                try {
                    withPbfIterator(pbfFile, iterator -> {
                        while (iterator.hasNext()) {
                            EntityContainer c = iterator.next();
                            if (c.getType() == EntityType.Node) {
                                buf.add((OsmNode) c.getEntity());
                                if (buf.size() >= BATCH_SIZE) {
                                    nodeBatchQueue.put(new ArrayList<>(buf));
                                    buf.clear();
                                    stats.setQueueSize(nodeBatchQueue.size());
                                }
                            }
                        }
                        if (!buf.isEmpty()) {
                            nodeBatchQueue.put(new ArrayList<>(buf));
                            stats.setQueueSize(nodeBatchQueue.size());
                        }
                    });
                } catch (Exception e) {
                    stats.recordError(ImportStatistics.Stage.CACHING_NODE_COORDINATES, Kind.READ, null, "node-cache-reader", e);
                } finally {
                    for (int i = 0; i < numProcessors; i++) {
                        try {
                            nodeBatchQueue.put(Collections.emptyList());
                        } catch (InterruptedException ex) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
            });

            for (int i = 0; i < numProcessors; i++) {
                executor.submit(() -> {
                    stats.incrementActiveThreads();
                    final ThreadLocal<RocksBatchWriter> nodeWriterLocal = ThreadLocal.withInitial(() -> new RocksBatchWriter(nodeCache, 50_000, stats));
                    final ThreadLocal<ByteBuffer> valueBufferLocal = ThreadLocal.withInitial(() -> ByteBuffer.allocate(16));
                    final ThreadLocal<List<byte[]>> keysListLocal = ThreadLocal.withInitial(ArrayList::new);

                    try {
                        while (true) {
                            List<OsmNode> nodes = nodeBatchQueue.take();
                            stats.setQueueSize(nodeBatchQueue.size());
                            if (nodes.isEmpty()) break;

                            List<byte[]> keys = keysListLocal.get();
                            keys.clear();
                            for (OsmNode n : nodes) {
                                keys.add(s2Helper.longToByteArray(n.getId()));
                            }

                            List<byte[]> presence = neededNodesDb.multiGetAsList(keys);

                            RocksBatchWriter nodeWriter = nodeWriterLocal.get();
                            ByteBuffer valueBuffer = valueBufferLocal.get();

                            for (int idx = 0; idx < nodes.size(); idx++) {
                                if (presence.get(idx) != null) {
                                    OsmNode n = nodes.get(idx);
                                    valueBuffer.clear();
                                    valueBuffer.putDouble(n.getLatitude());
                                    valueBuffer.putDouble(n.getLongitude());
                                    nodeWriter.put(keys.get(idx), valueBuffer.array());
                                    stats.incrementNodesCached();
                                }
                            }
                        }
                        nodeWriterLocal.get().flush();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (RocksDBException e) {
                        throw new RuntimeException(e);
                    } finally {
                        try {
                            nodeWriterLocal.get().close();
                        } catch (Exception e) {
                            stats.recordError(ImportStatistics.Stage.CACHING_NODE_COORDINATES, Kind.STORE, null, "node-cache-writer-close", e);
                        }
                        nodeWriterLocal.remove();
                        valueBufferLocal.remove();
                        keysListLocal.remove();
                        stats.decrementActiveThreads();
                        latch.countDown();
                    }
                });
            }

            latch.await();
            reader.join();
        }
    }

    private void processAdministrativeBoundariesFromIndex(RocksDB relIndexDb, RocksDB nodeCache, RocksDB wayIndexDb, RocksDB gridsIndexDb, RocksDB boundariesDb, ImportStatistics stats) throws Exception {
        int maxConcurrentGeometries = 100;
        Semaphore semaphore = new Semaphore(maxConcurrentGeometries);

        int numThreads = Math.max(1, config.getImportConfiguration().getThreads());
        ExecutorService executor = createExecutorService(numThreads);
        ExecutorCompletionService<BoundaryResultLite> ecs = new ExecutorCompletionService<>(executor);
        AtomicInteger submitted = new AtomicInteger(0);
        AtomicLong collected = new AtomicLong(0);

        try (RocksBatchWriter boundariesWriter = new RocksBatchWriter(boundariesDb, 500, stats)) {
            try (RocksIterator it = relIndexDb.newIterator()) {
                it.seekToFirst();
                while (it.isValid()) {
                    byte[] key = it.key();
                    byte[] val = it.value();
                    long relId = s2Helper.byteArrayToLong(key);
                    RelRec rec = decodeRelRec(val, relId);

                    semaphore.acquire();
                    stats.incrementActiveThreads();
                    ecs.submit(() -> {
                        try {
                            org.locationtech.jts.geom.Geometry geometry = buildGeometryFromRelRec(rec, nodeCache, wayIndexDb, stats);
                            if (geometry == null) return null;

                            if (!geometry.isValid()) {
                                try {
                                    geometry = geometry.buffer(0);
                                } catch (Exception e) {
                                    return null;
                                }
                            }
                            if (geometry == null || geometry.isEmpty() || !geometry.isValid()) return null;

                            org.locationtech.jts.geom.Geometry simplified = geometrySimplificationService.simplifyByAdminLevel(geometry, rec.level);

                            if (simplified == null || simplified.isEmpty() || !simplified.isValid()) {
                                try {
                                    simplified = simplified != null ? simplified.buffer(0) : null;
                                } catch (Exception e) {
                                    simplified = geometry;
                                }
                            }
                            if (simplified == null || simplified.isEmpty() || !simplified.isValid()) {
                                simplified = geometry;
                            }

                            return new BoundaryResultLite(rec.osmId, rec.level, rec.name, rec.code, simplified);
                        } finally {
                            semaphore.release();
                            stats.decrementActiveThreads();
                        }
                    });

                    submitted.incrementAndGet();

                    for (Future<BoundaryResultLite> f; (f = ecs.poll()) != null; ) {
                        collected.incrementAndGet();
                        try {
                            BoundaryResultLite r = f.get();
                            if (r != null) {
                                storeBoundary(r.osmId(), r.level(), r.name(), r.code(), r.geometry(), boundariesWriter, gridsIndexDb);
                                stats.incrementBoundariesProcessed();
                            }
                        } catch (Exception e) {
                            stats.recordError(ImportStatistics.Stage.PROCESSING_ADMIN_BOUNDARIES, Kind.CONCURRENCY, null, "boundary-future-poll", e);
                        }
                    }
                    it.next();
                }
            }
            long remaining = submitted.get() - collected.get();
            for (int i = 0; i < remaining; i++) {
                try {
                    Future<BoundaryResultLite> f = ecs.take();
                    BoundaryResultLite r = f.get();
                    if (r != null) {
                        storeBoundary(r.osmId(), r.level(), r.name(), r.code(), r.geometry(), boundariesWriter, gridsIndexDb);
                        stats.incrementBoundariesProcessed();
                    }
                } catch (Exception e) {
                    stats.recordError(ImportStatistics.Stage.PROCESSING_ADMIN_BOUNDARIES, Kind.CONCURRENCY, null, "boundary-future-take", e);
                }
            }
            boundariesWriter.flush();

        } finally {
            executor.shutdown();
            executor.awaitTermination(30, TimeUnit.MINUTES);
        }
    }

    private void processBuildingBoundariesFromIndex(RocksDB poiIndexDb, RocksDB nodeCache, RocksDB wayIndexDb, RocksDB buildingGridIndexDb, RocksDB appendBuildingDb, ImportStatistics stats) throws Exception {
        int maxInFlight = 100;
        Semaphore semaphore = new Semaphore(maxInFlight);
        int batchSize = 1000;
        int numThreads = Math.max(1, config.getImportConfiguration().getThreads());
        ExecutorService executor = createExecutorService(numThreads);
        ExecutorCompletionService<List<BuildingData>> ecs = new ExecutorCompletionService<>(executor);

        AtomicInteger submitted = new AtomicInteger(0);
        AtomicLong collected = new AtomicLong(0);

        AtomicReference<Map<Long, List<BuildingData>>> shardBufferRef = new AtomicReference<>(new ConcurrentHashMap<>());
        Runnable flushTask = () -> {
            try {
                Map<Long, List<BuildingData>> bufferToFlush = shardBufferRef.getAndSet(new ConcurrentHashMap<>());
                if (!bufferToFlush.isEmpty()) {
                    writeBuildingShardBatchAppendOnly(bufferToFlush, appendBuildingDb, stats);
                }
            } catch (Exception e) {
                stats.recordError(ImportStatistics.Stage.PROCESSING_BUILDINGS, Kind.STORE, null, "flush-building-append-batch", e);
            }
        };

        try (PeriodicFlusher _ = PeriodicFlusher.start("building-shard-flush", 5, 5, flushTask);
             RocksIterator it = poiIndexDb.newIterator()) {

            it.seekToFirst();
            List<Long> currentBatchIds = new ArrayList<>(batchSize);
            List<PoiIndexRec> currentBatchRecs = new ArrayList<>(batchSize);

            while (it.isValid()) {
                byte[] key = it.key();
                if (key[0] == 'W') { // Buildings must be ways
                    byte[] val = it.value();
                    final PoiIndexRec rec = decodePoiIndexRec(val);
                    if ("building".equals(rec.type)) {
                        final long wayId = bytesToLong(key, 1);
                        currentBatchIds.add(wayId);
                        currentBatchRecs.add(rec);

                        if (currentBatchIds.size() >= batchSize) {
                            semaphore.acquire();
                            stats.incrementActiveThreads();
                            List<Long> idsToProcess = new ArrayList<>(currentBatchIds);
                            List<PoiIndexRec> recsToProcess = new ArrayList<>(currentBatchRecs);
                            ecs.submit(() -> {
                                try {
                                    List<BuildingData> results = new ArrayList<>();
                                    for (int i = 0; i < idsToProcess.size(); i++) {
                                        long wayIdInner = idsToProcess.get(i);
                                        PoiIndexRec recInner = recsToProcess.get(i);
                                        try {
                                            org.locationtech.jts.geom.Geometry geometry = buildGeometryFromWay(wayIdInner, nodeCache, wayIndexDb, stats);
                                            if (geometry == null) continue;
                                            if (!geometry.isValid()) {
                                                try {
                                                    geometry = geometry.buffer(0);
                                                } catch (Exception e) {
                                                    continue;
                                                }
                                            }
                                            if (geometry == null || geometry.isEmpty() || !geometry.isValid()) continue;
                                            results.add(new BuildingData(wayIdInner, recInner.subtype, null, geometry));
                                        } catch (Exception e) {
                                            stats.recordError(ImportStatistics.Stage.PROCESSING_BUILDINGS, Kind.GEOMETRY, wayIdInner, "build-building-geometry-batch", e);
                                        }
                                    }
                                    return results;
                                } finally {
                                    semaphore.release();
                                    stats.decrementActiveThreads();
                                }
                            });
                            submitted.incrementAndGet();
                            currentBatchIds.clear();
                            currentBatchRecs.clear();

                            Future<List<BuildingData>> f;
                            while ((f = ecs.poll()) != null) {
                                collected.incrementAndGet();
                                try {
                                    List<BuildingData> batchResults = f.get();
                                    Map<Long, List<BuildingData>> currentActiveBuffer = shardBufferRef.get();
                                    for (BuildingData r : batchResults) {
                                        Coordinate center = r.geometry().getEnvelopeInternal().centre();
                                        long shardId = s2Helper.getShardId(center.getY(), center.getX());
                                        currentActiveBuffer.computeIfAbsent(shardId, k -> new CopyOnWriteArrayList<>()).add(r);
                                        stats.incrementBuildingsProcessed();
                                    }
                                } catch (Exception e) {
                                    stats.recordError(ImportStatistics.Stage.PROCESSING_BUILDINGS, Kind.CONCURRENCY, null, "building-future-poll", e);
                                }
                            }
                        }
                    }
                }
                it.next();
            }

            if (!currentBatchIds.isEmpty()) {
                semaphore.acquire();
                stats.incrementActiveThreads();
                List<Long> idsToProcess = new ArrayList<>(currentBatchIds);
                List<PoiIndexRec> recsToProcess = new ArrayList<>(currentBatchRecs);
                ecs.submit(() -> {
                    try {
                        List<BuildingData> results = new ArrayList<>();
                        for (int i = 0; i < idsToProcess.size(); i++) {
                            long wayIdInner = idsToProcess.get(i);
                            PoiIndexRec recInner = recsToProcess.get(i);
                            try {
                                org.locationtech.jts.geom.Geometry geometry = buildGeometryFromWay(wayIdInner, nodeCache, wayIndexDb, stats);
                                if (geometry == null) continue;
                                if (!geometry.isValid()) {
                                    try {
                                        geometry = geometry.buffer(0);
                                    } catch (Exception e) {
                                        continue;
                                    }
                                }
                                if (geometry == null || geometry.isEmpty() || !geometry.isValid()) continue;
                                results.add(new BuildingData(wayIdInner, recInner.subtype, null, geometry));
                            } catch (Exception e) {
                                stats.recordError(ImportStatistics.Stage.PROCESSING_BUILDINGS, Kind.GEOMETRY, wayIdInner, "build-building-geometry-batch", e);
                            }
                        }
                        return results;
                    } finally {
                        semaphore.release();
                        stats.decrementActiveThreads();
                    }
                });
                submitted.incrementAndGet();
            }

            long remaining = submitted.get() - collected.get();
            for (int i = 0; i < remaining; i++) {
                try {
                    Future<List<BuildingData>> f = ecs.take();
                    List<BuildingData> batchResults = f.get();
                    Map<Long, List<BuildingData>> currentActiveBuffer = shardBufferRef.get();
                    for (BuildingData r : batchResults) {
                        Coordinate center = r.geometry().getEnvelopeInternal().centre();
                        long shardId = s2Helper.getShardId(center.getY(), center.getX());
                        currentActiveBuffer.computeIfAbsent(shardId, k -> new CopyOnWriteArrayList<>()).add(r);
                        stats.incrementBuildingsProcessed();
                    }
                } catch (Exception e) {
                    stats.recordError(ImportStatistics.Stage.PROCESSING_BUILDINGS, Kind.CONCURRENCY, null, "building-future-take", e);
                }
            }
            flushTask.run();
        } finally {
            executor.shutdown();
            executor.awaitTermination(30, TimeUnit.MINUTES);
        }
    }

    private PoiData createPoiDataFromIndex(PoiIndexRec rec, double lat, double lon, List<HierarchyCache.SimpleHierarchyItem> hierarchy, byte[] boundaryWkb) {
        AddressData addr = null;
        if (rec.street != null || rec.houseNumber != null || rec.postcode != null || rec.city != null || rec.country != null) {
            String city = rec.city, country = rec.country;
            if (city == null || country == null) {
                List<HierarchyCache.SimpleHierarchyItem> sorted = new ArrayList<>(hierarchy);
                sorted.sort(Comparator.comparingInt(HierarchyCache.SimpleHierarchyItem::level).reversed());
                for (HierarchyCache.SimpleHierarchyItem item : sorted) {
                    if (city == null && item.level() >= 6 && item.level() <= 10) city = item.name();
                    if (country == null && item.level() == 2) country = item.code();
                    if (city != null && country != null) break;
                }
            }
            addr = new AddressData(rec.street, rec.houseNumber, rec.postcode, city, country);
        }
        return new PoiData(rec.id, lat, lon, rec.type != null ? rec.type : "unknown", rec.subtype != null ? rec.subtype : "", rec.names != null ? rec.names : List.of(), addr, hierarchy, boundaryWkb);
    }

    private String intern(String s) {
        if (s == null) return null;
        return tagCache.computeIfAbsent(s, k -> k);
    }

    private boolean hasAddressTags(OsmEntity entity) {
        for (int i = 0; i < entity.getNumberOfTags(); i++) {
            if (entity.getTag(i).getKey().startsWith("addr:")) {
                return true;
            }
        }
        return false;
    }

    private boolean isBuildingWay(OsmWay way) {
        for (int i = 0; i < way.getNumberOfTags(); i++) {
            OsmTag tag = way.getTag(i);
            if ("building".equals(tag.getKey())) {
                String val = tag.getValue();
                return switch (val) {
                    case "yes", "commercial", "retail", "industrial",
                         "office", "apartments", "residential" -> true;
                    default -> false;
                };
            }
        }
        return false;
    }

    private void writeShardBatchAppendOnly(Map<Long, List<PoiData>> shardBuffer, RocksDB appendDb, ImportStatistics stats) throws Exception {
        FlatBufferBuilder builder = new FlatBufferBuilder(1024 * 32);

        try (WriteBatch batch = new WriteBatch(); WriteOptions writeOptions = new WriteOptions()) {

            for (Iterator<Map.Entry<Long, List<PoiData>>> it = shardBuffer.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<Long, List<PoiData>> entry = it.next();
                List<PoiData> pois = entry.getValue();
                if (pois.isEmpty()) {
                    it.remove();
                    continue;
                }

                builder.clear();

                int[] poiOffsets = new int[pois.size()];
                for (int i = 0; i < pois.size(); i++) {
                    poiOffsets[i] = serializePoiData(builder, pois.get(i));
                }
                int poisVectorOffset = POIList.createPoisVector(builder, poiOffsets);
                int poiListOffset = POIList.createPOIList(builder, poisVectorOffset);
                builder.finish(poiListOffset);

                long seq = sequence.incrementAndGet();
                byte[] key = new byte[16];
                ByteBuffer.wrap(key).putLong(entry.getKey()).putLong(seq);

                batch.put(key, builder.sizedByteArray());
                it.remove();
            }

            try {
                appendDb.write(writeOptions, batch);
            } catch (RocksDBException e) {
                stats.recordError(ImportStatistics.Stage.PROCESSING_POIS_SHARDING, Kind.STORE, null, "rocks-write:append_po", e);
            }
            stats.incrementRocksDbWrites();
        }
    }

    private int serializePoiData(FlatBufferBuilder builder, PoiData poi) {
        int typeOff = builder.createString(poi.type());
        int subtypeOff = builder.createString(poi.subtype());

        List<NameData> names = poi.names();
        int[] nameOffs = new int[names.size()];
        for (int j = 0; j < names.size(); j++) {
            NameData n = names.get(j);
            nameOffs[j] = Name.createName(builder, builder.createString(n.lang()), builder.createString(n.text()));
        }
        int namesVecOff = POI.createNamesVector(builder, nameOffs);

        int addressOff = 0;
        AddressData addr = poi.address();
        if (addr != null) {
            int streetOff = addr.street() != null ? builder.createString(addr.street()) : 0;
            int houseNumberOff = addr.houseNumber() != null ? builder.createString(addr.houseNumber()) : 0;
            int postcodeOff = addr.postcode() != null ? builder.createString(addr.postcode()) : 0;
            int cityOff = addr.city() != null ? builder.createString(addr.city()) : 0;
            int countryOff = addr.country() != null ? builder.createString(addr.country()) : 0;
            addressOff = com.dedicatedcode.paikka.flatbuffers.Address.createAddress(builder, streetOff, houseNumberOff, postcodeOff, cityOff, countryOff);
        }

        List<HierarchyCache.SimpleHierarchyItem> hierarchy = poi.hierarchy();

        int[] hierOffs = new int[hierarchy.size()];
        for (int j = 0; j < hierarchy.size(); j++) {
            HierarchyCache.SimpleHierarchyItem h = hierarchy.get(j);
            hierOffs[j] = com.dedicatedcode.paikka.flatbuffers.HierarchyItem.createHierarchyItem(builder, h.level(), builder.createString(h.type()), builder.createString(h.name()), h.osmId(), builder.createString(h.code()));
        }
        int hierVecOff = POI.createHierarchyVector(builder, hierOffs);

        int boundaryOff = 0;
        byte[] boundaryWkb = poi.boundaryWkb();
        if (boundaryWkb != null && boundaryWkb.length > 0) {
            int boundaryDataOff = Geometry.createDataVector(builder, boundaryWkb);
            boundaryOff = Geometry.createGeometry(builder, boundaryDataOff);
        }

        POI.startPOI(builder);
        POI.addId(builder, poi.id());
        POI.addLat(builder, (float) poi.lat());
        POI.addLon(builder, (float) poi.lon());
        POI.addType(builder, typeOff);
        POI.addSubtype(builder, subtypeOff);
        POI.addNames(builder, namesVecOff);
        if (addressOff != 0) POI.addAddress(builder, addressOff);
        POI.addHierarchy(builder, hierVecOff);
        if (boundaryOff != 0) POI.addBoundary(builder, boundaryOff);
        return POI.endPOI(builder);
    }

    private void compactBuildingShards(RocksDB appendDb, RocksDB buildingsDb, ImportStatistics stats) {
        stats.setCompactionStartTime(System.currentTimeMillis());
        stats.setCompactionEntriesTotal(buildingSequence.get());

        Building reusableBuilding = new Building();
        Geometry reusableGeom = new Geometry();

        try (RocksIterator iterator = appendDb.newIterator();
             WriteOptions writeOptions = new WriteOptions().setDisableWAL(true)) {

            iterator.seekToFirst();

            long currentShardId = Long.MIN_VALUE;
            List<byte[]> currentShardChunks = new ArrayList<>();

            while (iterator.isValid()) {
                byte[] key = iterator.key();
                long shardId = ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN).getLong();

                if (shardId != currentShardId && currentShardId != Long.MIN_VALUE) {
                    flushCompactedBuildingShard(currentShardChunks, currentShardId, buildingsDb, writeOptions, reusableBuilding, reusableGeom, stats);
                    stats.incrementCompactionEntriesProcessed(currentShardChunks.size());
                    currentShardChunks.clear();
                }

                currentShardId = shardId;

                byte[] value = iterator.value();
                byte[] valueCopy = new byte[value.length];
                System.arraycopy(value, 0, valueCopy, 0, value.length);
                currentShardChunks.add(valueCopy);

                iterator.next();
            }

            if (currentShardId != Long.MIN_VALUE && !currentShardChunks.isEmpty()) {
                flushCompactedBuildingShard(currentShardChunks, currentShardId, buildingsDb, writeOptions, reusableBuilding, reusableGeom, stats);
                stats.incrementCompactionEntriesProcessed(currentShardChunks.size());
            }
        }
    }

    private void flushCompactedBuildingShard(List<byte[]> chunks, long shardId, RocksDB buildingsDb, WriteOptions writeOptions, Building reusableBuilding, Geometry reusableGeom, ImportStatistics stats) {
        int totalBuildings = 0;
        try {
            for (byte[] chunk : chunks) {
                ByteBuffer buf = ByteBuffer.wrap(chunk);
                BuildingList buildingList = BuildingList.getRootAsBuildingList(buf);
                totalBuildings += buildingList.buildingsLength();
            }
        } catch (Exception e) {
            stats.recordError(ImportStatistics.Stage.COMPACTING_POIS, Kind.DECODE, null, "flatbuffers-read:BuildingList", e);
        }

        FlatBufferBuilder builder = new FlatBufferBuilder(Math.max(1024, totalBuildings * 256));
        int[] allOffsets = new int[totalBuildings];
        int idx = 0;

        for (byte[] chunk : chunks) {
            ByteBuffer buf = ByteBuffer.wrap(chunk);
            BuildingList buildingList = BuildingList.getRootAsBuildingList(buf);
            int count = buildingList.buildingsLength();

            for (int i = 0; i < count; i++) {
                buildingList.buildings(reusableBuilding, i);
                allOffsets[idx++] = copyBuildingFromFlatBuffer(builder, reusableBuilding, reusableGeom);
            }
        }

        int buildingsVec = BuildingList.createBuildingsVector(builder, allOffsets);
        int buildingList = BuildingList.createBuildingList(builder, buildingsVec);
        builder.finish(buildingList);
        try {
            buildingsDb.put(writeOptions, s2Helper.longToByteArray(shardId), builder.sizedByteArray());
        } catch (RocksDBException e) {
            stats.recordError(ImportStatistics.Stage.COMPACTING_POIS, Kind.STORE, null, "rocks-put:buildings", e);
        }
    }

    private int copyBuildingFromFlatBuffer(FlatBufferBuilder builder, Building building, Geometry reusableGeom) {
        String nameStr = building.name();
        String codeStr = building.code();
        int nameOff = nameStr != null ? builder.createString(nameStr) : 0;
        int codeOff = codeStr != null ? builder.createString(codeStr) : 0;

        int geometryOff = 0;
        if (building.geometry(reusableGeom) != null && reusableGeom.dataLength() > 0) {
            ByteBuffer geometryBuf = reusableGeom.dataAsByteBuffer();
            if (geometryBuf != null) {
                int geometryDataOff = Geometry.createDataVector(builder, geometryBuf);
                geometryOff = Geometry.createGeometry(builder, geometryDataOff);
            }
        }

        Building.startBuilding(builder);
        Building.addId(builder, building.id());
        if (nameOff != 0) Building.addName(builder, nameOff);
        if (codeOff != 0) Building.addCode(builder, codeOff);
        if (geometryOff != 0) Building.addGeometry(builder, geometryOff);
        return Building.endBuilding(builder);
    }

    private void writeBuildingShardBatchAppendOnly(Map<Long, List<BuildingData>> shardBuffer, RocksDB appendDb, ImportStatistics stats) throws Exception {
        FlatBufferBuilder builder = new FlatBufferBuilder(1024 * 32);

        try (WriteBatch batch = new WriteBatch(); WriteOptions writeOptions = new WriteOptions()) {

            for (Iterator<Map.Entry<Long, List<BuildingData>>> it = shardBuffer.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<Long, List<BuildingData>> entry = it.next();
                List<BuildingData> buildings = entry.getValue();
                if (buildings.isEmpty()) {
                    it.remove();
                    continue;
                }

                builder.clear();

                int[] buildingOffsets = new int[buildings.size()];
                for (int i = 0; i < buildings.size(); i++) {
                    buildingOffsets[i] = serializeBuildingData(builder, buildings.get(i));
                }
                int buildingsVectorOffset = BuildingList.createBuildingsVector(builder, buildingOffsets);
                int buildingListOffset = BuildingList.createBuildingList(builder, buildingsVectorOffset);
                builder.finish(buildingListOffset);

                long seq = buildingSequence.incrementAndGet();
                byte[] key = new byte[16];
                ByteBuffer.wrap(key).putLong(entry.getKey()).putLong(seq);

                batch.put(key, builder.sizedByteArray());
                it.remove();
            }

            try {
                appendDb.write(writeOptions, batch);
            } catch (RocksDBException e) {
                stats.recordError(ImportStatistics.Stage.PROCESSING_BUILDINGS, Kind.STORE, null, "rocks-write:append_building", e);
            }
            stats.incrementRocksDbWrites();
        }
    }

    private int serializeBuildingData(FlatBufferBuilder builder, BuildingData building) {
        int nameOff = building.name() != null ? builder.createString(building.name()) : 0;
        int codeOff = building.code() != null ? builder.createString(building.code()) : 0;

        int geometryOff = 0;
        if (building.geometry() != null) {
            byte[] wkb = new WKBWriter().write(building.geometry());
            int geometryDataOff = Geometry.createDataVector(builder, wkb);
            geometryOff = Geometry.createGeometry(builder, geometryDataOff);
        }

        Building.startBuilding(builder);
        Building.addId(builder, building.id());
        if (nameOff != 0) Building.addName(builder, nameOff);
        if (codeOff != 0) Building.addCode(builder, codeOff);
        if (geometryOff != 0) Building.addGeometry(builder, geometryOff);
        return Building.endBuilding(builder);
    }


    private void compactShards(RocksDB appendDb, RocksDB shardsDb, ImportStatistics stats) {
        stats.setCompactionStartTime(System.currentTimeMillis());
        stats.setCompactionEntriesTotal(sequence.get());

        POI reusablePoi = new POI();
        Name reusableName = new Name();
        HierarchyItem reusableHier = new HierarchyItem();
        Address reusableAddr = new Address();
        Geometry reusableGeom = new Geometry();

        try (RocksIterator iterator = appendDb.newIterator();
             WriteOptions writeOptions = new WriteOptions().setDisableWAL(true)) {

            iterator.seekToFirst();

            long currentShardId = Long.MIN_VALUE;
            List<byte[]> currentShardChunks = new ArrayList<>();

            while (iterator.isValid()) {
                byte[] key = iterator.key();
                long shardId = ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN).getLong();

                if (shardId != currentShardId && currentShardId != Long.MIN_VALUE) {
                    flushCompactedShard(currentShardChunks, currentShardId, shardsDb, writeOptions, reusablePoi, reusableName, reusableHier, reusableAddr, reusableGeom, stats);
                    stats.incrementCompactionEntriesProcessed(currentShardChunks.size());
                    currentShardChunks.clear();
                }

                currentShardId = shardId;

                byte[] value = iterator.value();
                byte[] valueCopy = new byte[value.length];
                System.arraycopy(value, 0, valueCopy, 0, value.length);
                currentShardChunks.add(valueCopy);

                iterator.next();
            }

            if (currentShardId != Long.MIN_VALUE && !currentShardChunks.isEmpty()) {
                flushCompactedShard(currentShardChunks, currentShardId, shardsDb, writeOptions, reusablePoi, reusableName, reusableHier, reusableAddr, reusableGeom, stats);
                stats.incrementCompactionEntriesProcessed(currentShardChunks.size());
            }
        }
    }

    private void flushCompactedShard(List<byte[]> chunks, long shardId, RocksDB shardsDb, WriteOptions writeOptions, POI reusablePoi, Name reusableName, HierarchyItem reusableHier, Address reusableAddr, Geometry reusableGeom, ImportStatistics stats) {

        int totalPois = 0;
        try {
            for (byte[] chunk : chunks) {
                ByteBuffer buf = ByteBuffer.wrap(chunk);
                POIList poiList = POIList.getRootAsPOIList(buf);
                totalPois += poiList.poisLength();
            }
        } catch (Exception e) {
            stats.recordError(ImportStatistics.Stage.COMPACTING_POIS, Kind.DECODE, null, "flatbuffers-read:POIList", e);
        }

        FlatBufferBuilder builder = new FlatBufferBuilder(Math.max(1024, totalPois * 256));
        int[] allOffsets = new int[totalPois];
        int idx = 0;

        for (byte[] chunk : chunks) {
            ByteBuffer buf = ByteBuffer.wrap(chunk);
            POIList poiList = POIList.getRootAsPOIList(buf);
            int count = poiList.poisLength();

            for (int i = 0; i < count; i++) {
                poiList.pois(reusablePoi, i);
                allOffsets[idx++] = copyPoiFromFlatBuffer(builder, reusablePoi, reusableName, reusableHier, reusableAddr, reusableGeom);
            }
        }

        int poisVec = POIList.createPoisVector(builder, allOffsets);
        int poiList = POIList.createPOIList(builder, poisVec);
        builder.finish(poiList);
        try {
            shardsDb.put(writeOptions, s2Helper.longToByteArray(shardId), builder.sizedByteArray());
        } catch (RocksDBException e) {
            stats.recordError(ImportStatistics.Stage.COMPACTING_POIS, Kind.STORE, null, "rocks-put:poi_shards", e);
        }
    }

    private int copyPoiFromFlatBuffer(FlatBufferBuilder builder, POI poi, Name reusableName, HierarchyItem reusableHier, Address reusableAddr, Geometry reusableGeom) {

        String typeStr = poi.type();
        String subtypeStr = poi.subtype();
        int typeOff = typeStr != null ? builder.createString(typeStr) : 0;
        int subtypeOff = subtypeStr != null ? builder.createString(subtypeStr) : 0;

        int namesLen = poi.namesLength();
        int namesVecOff;
        if (namesLen > 0) {
            int[] nameOffs = new int[namesLen];
            for (int j = 0; j < namesLen; j++) {
                poi.names(reusableName, j);
                String lang = reusableName.lang();
                String text = reusableName.text();
                int langOff = lang != null ? builder.createString(lang) : 0;
                int textOff = text != null ? builder.createString(text) : 0;
                nameOffs[j] = Name.createName(builder, langOff, textOff);
            }
            namesVecOff = POI.createNamesVector(builder, nameOffs);
        } else {
            namesVecOff = POI.createNamesVector(builder, new int[0]);
        }

        int addressOff = 0;
        if (poi.address(reusableAddr) != null) {
            String street = reusableAddr.street();
            String houseNumber = reusableAddr.houseNumber();
            String postcode = reusableAddr.postcode();
            String city = reusableAddr.city();
            String country = reusableAddr.country();

            int streetOff = street != null ? builder.createString(street) : 0;
            int houseNumberOff = houseNumber != null ? builder.createString(houseNumber) : 0;
            int postcodeOff = postcode != null ? builder.createString(postcode) : 0;
            int cityOff = city != null ? builder.createString(city) : 0;
            int countryOff = country != null ? builder.createString(country) : 0;

            addressOff = com.dedicatedcode.paikka.flatbuffers.Address.createAddress(builder, streetOff, houseNumberOff, postcodeOff, cityOff, countryOff);
        }

        int hierLen = poi.hierarchyLength();

        int hierVecOff;
        if (hierLen > 0) {
            int[] hierOffs = new int[hierLen];
            for (int j = 0; j < hierLen; j++) {
                poi.hierarchy(reusableHier, j);
                String hType = reusableHier.type();
                String hName = reusableHier.name();
                String hCode = reusableHier.code();
                int hTypeOff = hType != null ? builder.createString(hType) : 0;
                int hNameOff = hName != null ? builder.createString(hName) : 0;
                int hCodeOff = hCode != null ? builder.createString(hCode) : 0;
                hierOffs[j] = com.dedicatedcode.paikka.flatbuffers.HierarchyItem.createHierarchyItem(builder, reusableHier.level(), hTypeOff, hNameOff, reusableHier.osmId(), hCodeOff);
            }
            hierVecOff = POI.createHierarchyVector(builder, hierOffs);
        } else {
            hierVecOff = POI.createHierarchyVector(builder, new int[0]);
        }

        int boundaryOff = 0;
        if (poi.boundary(reusableGeom) != null && reusableGeom.dataLength() > 0) {
            ByteBuffer boundaryBuf = reusableGeom.dataAsByteBuffer();
            if (boundaryBuf != null) {
                int boundaryDataOff = Geometry.createDataVector(builder, boundaryBuf);
                boundaryOff = Geometry.createGeometry(builder, boundaryDataOff);
            } else {
                int len = reusableGeom.dataLength();
                byte[] boundaryBytes = new byte[len];
                for (int k = 0; k < len; k++) {
                    boundaryBytes[k] = (byte) reusableGeom.data(k);
                }
                int boundaryDataOff = Geometry.createDataVector(builder, boundaryBytes);
                boundaryOff = Geometry.createGeometry(builder, boundaryDataOff);
            }
        }

        POI.startPOI(builder);
        POI.addId(builder, poi.id());
        POI.addLat(builder, poi.lat());
        POI.addLon(builder, poi.lon());
        if (typeOff != 0) POI.addType(builder, typeOff);
        if (subtypeOff != 0) POI.addSubtype(builder, subtypeOff);
        POI.addNames(builder, namesVecOff);
        if (addressOff != 0) POI.addAddress(builder, addressOff);
        POI.addHierarchy(builder, hierVecOff);
        if (boundaryOff != 0) POI.addBoundary(builder, boundaryOff);
        return POI.endPOI(builder);
    }

    private org.locationtech.jts.geom.Geometry buildGeometryFromWay(long wayId, RocksDB nodeCache, RocksDB wayIndexDb, ImportStatistics stats) {
        try {
            byte[] nodeSeq = wayIndexDb.get(s2Helper.longToByteArray(wayId));
            List<Coordinate> coords = buildCoordinatesFromWay(nodeCache, nodeSeq);

            if (coords == null || coords.size() < 3) {
                return null;
            }

            if (!coords.getFirst().equals2D(coords.getLast())) {
                coords.add(new Coordinate(coords.getFirst()));
            }

            if (coords.size() < 4) {
                return null;
            }

            LinearRing shell = GEOMETRY_FACTORY.createLinearRing(coords.toArray(new Coordinate[0]));
            return GEOMETRY_FACTORY.createPolygon(shell);
        } catch (Exception e) {
            stats.recordError(ImportStatistics.Stage.PROCESSING_BUILDINGS, Kind.GEOMETRY, wayId, "build-building-geometry", e);
            return null;
        }
    }

    private org.locationtech.jts.geom.Geometry buildGeometryFromRelRec(RelRec rec, RocksDB nodeCache, RocksDB wayIndexDb, ImportStatistics stats) {
        List<List<Coordinate>> outerRings = buildConnectedRings(toList(rec.outer), nodeCache, wayIndexDb);
        List<List<Coordinate>> innerRings = buildConnectedRings(toList(rec.inner), nodeCache, wayIndexDb);
        if (outerRings.isEmpty()) return null;
        List<Polygon> validPolygons = new ArrayList<>();
        for (List<Coordinate> outerRing : outerRings) {
            try {
                LinearRing shell = GEOMETRY_FACTORY.createLinearRing(outerRing.toArray(new Coordinate[0]));
                List<LinearRing> holes = new ArrayList<>();
                for (List<Coordinate> innerRing : innerRings)
                    try {
                        holes.add(GEOMETRY_FACTORY.createLinearRing(innerRing.toArray(new Coordinate[0])));
                    } catch (Exception e) {
                        stats.recordError(ImportStatistics.Stage.PROCESSING_ADMIN_BOUNDARIES, Kind.READ, rec.osmId, "rocks-get:way_index", e);
                    }
                Polygon polygon = GEOMETRY_FACTORY.createPolygon(shell, holes.toArray(new LinearRing[0]));
                if (polygon.isValid()) {
                    validPolygons.add(polygon);
                } else {
                    try {
                        org.locationtech.jts.geom.Geometry repaired = polygon.buffer(0);
                        if (repaired != null && repaired.isValid() && !repaired.isEmpty()) {
                            for (int i = 0; i < repaired.getNumGeometries(); i++) {
                                org.locationtech.jts.geom.Geometry part = repaired.getGeometryN(i);
                                if (part instanceof Polygon && part.isValid()) {
                                    validPolygons.add((Polygon) part);
                                }
                            }
                        }
                    } catch (Exception repairEx) {
                        stats.recordError(ImportStatistics.Stage.PROCESSING_ADMIN_BOUNDARIES, Kind.GEOMETRY, rec.osmId, "repair-polygon", repairEx);
                    }
                }
            } catch (Exception e) {
                stats.recordError(ImportStatistics.Stage.PROCESSING_ADMIN_BOUNDARIES, Kind.GEOMETRY, null, "build-boundary-geometry", e);
            }
        }
        if (validPolygons.isEmpty()) return null;
        return validPolygons.size() == 1 ? validPolygons.getFirst() : GEOMETRY_FACTORY.createMultiPolygon(validPolygons.toArray(new Polygon[0]));
    }

    private List<Long> toList(long[] arr) {
        if (arr == null) return Collections.emptyList();
        List<Long> l = new ArrayList<>(arr.length);
        for (long v : arr) l.add(v);
        return l;
    }

    private List<Coordinate> buildCoordinatesFromWay(RocksDB nodeCache, byte[] nodeSeq) {
        try {
            if (nodeSeq == null) return null;
            long[] nodeIds = s2Helper.byteArrayToLongArray(nodeSeq);
            if (nodeIds.length < 2) return null;

            List<byte[]> keys = new ArrayList<>(nodeIds.length);
            for (long nid : nodeIds) keys.add(s2Helper.longToByteArray(nid));
            List<byte[]> values = nodeCache.multiGetAsList(keys);
            if (values.size() != keys.size()) return null;

            List<Coordinate> coordinates = new ArrayList<>(nodeIds.length);
            for (byte[] value : values) {
                if (value != null && value.length == 16) {
                    ByteBuffer buffer = ByteBuffer.wrap(value);
                    coordinates.add(new Coordinate(buffer.getDouble(8), buffer.getDouble(0)));
                } else return null;
            }
            return coordinates.size() >= 2 ? coordinates : null;
        } catch (Exception e) {
            return null;
        }
    }

    private boolean isPoiFastKey(String key) {
        return switch (key) {
            case "amenity", "shop", "tourism", "leisure", "natural", "office", "craft", "healthcare", "emergency",
                 "historic", "man_made", "place", "sport", "public_transport", "railway", "aeroway", "building" -> true;
            case null, default -> false;
        };
    }

    private boolean isPoi(OsmEntity entity) {
        if (entity.getNumberOfTags() == 0) return false;

        boolean isInterestingBuilding = false;

        for (int i = 0; i < entity.getNumberOfTags(); i++) {
            OsmTag tag = entity.getTag(i);
            String key = tag.getKey();
            String val = tag.getValue();

            switch (key) {
                case "amenity":
                    return switch (val) {
                        case "bench", "drinking_water", "waste_basket", "bicycle_parking",
                             "vending_machine", "parking_entrance", "fire_hydrant" -> false;
                        default -> true;
                    };

                case "healthcare":
                    return true;

                case "emergency":
                    return switch (val) {
                        case "fire_hydrant", "defibrillator", "fire_extinguisher",
                             "siren", "life_ring", "lifeline", "phone", "drinking_water" -> false;
                        default -> true;
                    };

                case "building":
                    isInterestingBuilding = true;
                    break;

                case "natural":
                    // Filter out natural features that are not useful as POIs
                    return switch (val) {
                        case "tree", "wood", "scrub", "heath", "grassland", "fell",
                             "bare_rock", "scree", "shingle", "sand", "mud" -> false;
                        default -> true;
                    };

                case "shop", "tourism", "leisure", "office", "craft", "place",
                     "historic", "public_transport", "aeroway":
                    return !key.equals("leisure") || !List.of("picnic_table", "swimming_pool").contains(val);

                case "railway":
                    if ("station".equals(val)) return true;
                    break;

                default:
                    if (isPoiFastKey(key)) return true;
            }
        }

        return isInterestingBuilding;
    }

    private boolean isAdministrativeBoundaryWay(OsmWay way) {
        for (int i = 0; i < way.getNumberOfTags(); i++) {
            OsmTag tag = way.getTag(i);
            if ("boundary".equals(tag.getKey()) && "administrative".equals(tag.getValue())) return true;
            if ("admin_level".equals(tag.getKey())) return true;
        }
        return false;
    }

    private boolean isAdministrativeBoundary(OsmRelation relation) {
        boolean hasBoundaryTag = false, hasAdminLevel = false;
        for (int i = 0; i < relation.getNumberOfTags(); i++) {
            OsmTag tag = relation.getTag(i);
            if ("boundary".equals(tag.getKey()) && "administrative".equals(tag.getValue())) hasBoundaryTag = true;
            if ("admin_level".equals(tag.getKey())) hasAdminLevel = true;
            if ("type".equals(tag.getKey()) && "boundary".equals(tag.getValue())) hasBoundaryTag = true;
        }
        return hasBoundaryTag && hasAdminLevel;
    }

    private List<List<Coordinate>> buildConnectedRings(List<Long> wayIds, RocksDB nodeCache, RocksDB wayIndexDb) {
        if (wayIds.isEmpty()) return Collections.emptyList();
        Map<Long, List<Coordinate>> wayCoordinates = new HashMap<>();
        for (Long wayId : wayIds) {
            byte[] nodeSeq = null;
            try {
                nodeSeq = wayIndexDb.get(s2Helper.longToByteArray(wayId));
            } catch (RocksDBException ignored) {

            }
            List<Coordinate> coords = buildCoordinatesFromWay(nodeCache, nodeSeq);
            if (coords != null && coords.size() >= 2) wayCoordinates.put(wayId, coords);
        }
        if (wayCoordinates.isEmpty()) return Collections.emptyList();
        List<List<Coordinate>> rings = new ArrayList<>();
        Set<Long> usedWays = new HashSet<>();
        while (usedWays.size() < wayCoordinates.size()) {
            Long startWayId = wayCoordinates.keySet().stream().filter(id -> !usedWays.contains(id)).findFirst().orElse(null);
            if (startWayId == null) break;
            List<Coordinate> ring = new ArrayList<>(wayCoordinates.get(startWayId));
            usedWays.add(startWayId);
            boolean found;
            do {
                found = false;
                Coordinate ringEnd = ring.getLast();
                for (var entry : wayCoordinates.entrySet()) {
                    if (usedWays.contains(entry.getKey())) continue;
                    List<Coordinate> nextWay = entry.getValue();
                    Coordinate nextStart = nextWay.getFirst(), nextEnd = nextWay.getLast();
                    if (ringEnd.equals2D(nextStart)) {
                        ring.addAll(nextWay.subList(1, nextWay.size()));
                        usedWays.add(entry.getKey());
                        found = true;
                        break;
                    } else if (ringEnd.equals2D(nextEnd)) {
                        Collections.reverse(nextWay);
                        ring.addAll(nextWay.subList(1, nextWay.size()));
                        usedWays.add(entry.getKey());
                        found = true;
                        break;
                    }
                }
            } while (found);
            if (ring.size() >= 3 && !ring.getFirst().equals2D(ring.getLast()))
                ring.add(new Coordinate(ring.getFirst()));
            if (ring.size() >= 4) rings.add(ring);
        }
        return rings;
    }

    private void storeBoundary(long osmId, int level, String name, String code,
                               org.locationtech.jts.geom.Geometry geometry,
                               RocksBatchWriter boundariesWriter, RocksDB gridsIndexDb) throws Exception {
        FlatBufferBuilder fbb = new FlatBufferBuilder(1024);
        byte[] wkb = new WKBWriter().write(geometry);
        int geomDataOffset = Geometry.createDataVector(fbb, wkb);
        int geomOffset = Geometry.createGeometry(fbb, geomDataOffset);
        Envelope mbr = geometry.getEnvelopeInternal();

        double mirMinX = 0, mirMinY = 0, mirMaxX = 0, mirMaxY = 0;
        boolean hasMir = false;
        try {
            MaximumInscribedCircle mic = new MaximumInscribedCircle(geometry, 0.00001);
            double radius = mic.getRadiusLine().getLength();
            if (radius > 0) {
                Coordinate center = mic.getCenter().getCoordinate();
                double offset = radius / Math.sqrt(2);
                mirMinX = center.x - offset;
                mirMinY = center.y - offset;
                mirMaxX = center.x + offset;
                mirMaxY = center.y + offset;
                hasMir = true;
            }
        } catch (Exception e) {
            // MIR computation failed
        }

        int nameOffset = fbb.createString(name != null ? name : "Unknown");
        int codeOffset = fbb.createString(code != null ? code : "");
        Boundary.startBoundary(fbb);
        Boundary.addOsmId(fbb, osmId);
        Boundary.addLevel(fbb, level);
        Boundary.addName(fbb, nameOffset);
        Boundary.addCode(fbb, codeOffset);
        Boundary.addMinX(fbb, mbr.getMinX());
        Boundary.addMinY(fbb, mbr.getMinY());
        Boundary.addMaxX(fbb, mbr.getMaxX());
        Boundary.addMaxY(fbb, mbr.getMaxY());
        if (hasMir) {
            Boundary.addMirMinX(fbb, mirMinX);
            Boundary.addMirMinY(fbb, mirMinY);
            Boundary.addMirMaxX(fbb, mirMaxX);
            Boundary.addMirMaxY(fbb, mirMaxY);
        }
        Boundary.addGeometry(fbb, geomOffset);
        int root = Boundary.endBoundary(fbb);
        fbb.finish(root);

        S2LatLng low = S2LatLng.fromDegrees(mbr.getMinY(), mbr.getMinX());
        S2LatLng high = S2LatLng.fromDegrees(mbr.getMaxY(), mbr.getMaxX());
        S2LatLngRect rect = S2LatLngRect.fromPointPair(low, high);
        S2RegionCoverer coverer = S2RegionCoverer.builder()
                .setMinLevel(S2Helper.GRID_LEVEL)
                .setMaxLevel(S2Helper.GRID_LEVEL)
                .setMaxCells(Integer.MAX_VALUE)
                .build();
        ArrayList<S2CellId> covering = new ArrayList<>();
        coverer.getCovering(rect, covering);

        batchUpdateGridIndex(gridsIndexDb, covering, osmId);

        boundariesWriter.put(s2Helper.longToByteArray(osmId), fbb.sizedByteArray());
    }

    private void batchUpdateGridIndex(RocksDB gridIndexDb, ArrayList<S2CellId> cells, long osmId) throws RocksDBException {
        final int CHUNK_SIZE = 5_000;
        for (int offset = 0; offset < cells.size(); offset += CHUNK_SIZE) {
            int end = Math.min(offset + CHUNK_SIZE, cells.size());

            List<byte[]> keys = new ArrayList<>(end - offset);
            for (int i = offset; i < end; i++) {
                keys.add(s2Helper.longToByteArray(cells.get(i).id()));
            }

            synchronized (this) {
                List<byte[]> existing = gridIndexDb.multiGetAsList(keys);

                try (WriteBatch batch = new WriteBatch();
                     WriteOptions wo = new WriteOptions().setDisableWAL(true)) {
                    for (int i = 0; i < keys.size(); i++) {
                        byte[] ex = existing.get(i);
                        long[] newArray;
                        if (ex == null) {
                            newArray = new long[]{osmId};
                        } else {
                            long[] old = s2Helper.byteArrayToLongArray(ex);
                            boolean found = false;
                            for (long id : old) {
                                if (id == osmId) {
                                    found = true;
                                    break;
                                }
                            }
                            if (found) continue;
                            newArray = Arrays.copyOf(old, old.length + 1);
                            newArray[old.length] = osmId;
                        }
                        batch.put(keys.get(i), s2Helper.longArrayToByteArray(newArray));
                    }
                    gridIndexDb.write(wo, batch);
                }
            }
        }
    }

    private void withPbfIterator(Path pbfFile, ConsumerWithException<OsmIterator> consumer) throws Exception {
        try (RandomAccessFile file = new RandomAccessFile(pbfFile.toFile(), "r"); FileChannel channel = file.getChannel()) {

            long fileSize = channel.size();

            InputStream inputStream = new InputStream() {
                private long position = 0;
                private ByteBuffer currentBuffer = null;

                private void refillBuffer() throws IOException {
                    if (position >= fileSize) {
                        currentBuffer = null;
                        return;
                    }

                    long remaining = fileSize - position;
                    int toRead = (int) Math.min(fileReadWindowSize, remaining);

                    currentBuffer = ByteBuffer.allocate(toRead);
                    int bytesRead = channel.read(currentBuffer, position);

                    if (bytesRead <= 0) {
                        currentBuffer = null;
                        return;
                    }

                    currentBuffer.flip();
                    position += bytesRead;
                }

                @Override
                public int read() throws IOException {
                    if (currentBuffer == null || !currentBuffer.hasRemaining()) {
                        refillBuffer();
                        if (currentBuffer == null || !currentBuffer.hasRemaining()) {
                            return -1;
                        }
                    }
                    return currentBuffer.get() & 0xFF;
                }

                @Override
                public int read(byte[] b, int off, int len) throws IOException {
                    if (currentBuffer == null || !currentBuffer.hasRemaining()) {
                        refillBuffer();
                        if (currentBuffer == null || !currentBuffer.hasRemaining()) {
                            return -1;
                        }
                    }

                    int available = currentBuffer.remaining();
                    int toRead = Math.min(len, available);
                    currentBuffer.get(b, off, toRead);
                    return toRead;
                }
            };

            consumer.accept(new PbfIterator(inputStream, false));
        }
    }

    private String centerText(String text) {
        int pad = (80 - text.length()) / 2;
        return " ".repeat(Math.max(0, pad)) + text;
    }

    private void printHeader(String pbfFilePath, String dataDir) {
        System.out.println("\n\033[1;34m" + "=".repeat(80) + "\n" + centerText("PAIKKA IMPORT STARTING") + "\n" + "=".repeat(80) + "\033[0m\n");
        System.out.println("PBF File: " + pbfFilePath);
        System.out.println("Data Dir: " + dataDir);
        System.out.println("Max Import Threads: " + config.getImportConfiguration().getThreads());
        long maxHeapBytes = Runtime.getRuntime().maxMemory();
        String maxHeapSize = (maxHeapBytes == Long.MAX_VALUE) ? "unlimited" : (maxHeapBytes / (1024 * 1024 * 1024)) + "GB";
        System.out.println("Max Heap: " + maxHeapSize);
        System.out.println("File window size: " + (this.fileReadWindowSize / (1024 * 1024)) + "MB");
        System.out.println("Sharding Chunk Size: " + this.config.getImportConfiguration().getChunkSize());
    }

    private ExecutorService createExecutorService(int maxThreads) {
        if (maxThreads <= 0) {
            return Executors.newVirtualThreadPerTaskExecutor();
        } else {
            return Executors.newFixedThreadPool(maxThreads);
        }
    }

    private void cleanupDatabase(Path dbPath) {
        if (Files.exists(dbPath)) {
            try {
                Files.walk(dbPath).sorted(Comparator.reverseOrder()).forEach(path -> {
                    try {
                        Files.delete(path);
                    } catch (IOException e) {
                        System.err.println("Warning: Could not delete " + path + ": " + e.getMessage());
                    }
                });
                System.out.println("Cleaned up existing database: " + dbPath.getFileName());
            } catch (IOException e) {
                System.err.println("Warning: Could not clean up database " + dbPath + ": " + e.getMessage());
            }
        }
    }

    private long computeDirectorySize(Path root) {
        if (root == null || !Files.exists(root)) return 0L;
        try (Stream<Path> s = Files.walk(root)) {
            return s.filter(p -> {
                try {
                    return Files.isRegularFile(p);
                } catch (Exception e) {
                    return false;
                }
            }).mapToLong(p -> {
                try {
                    return Files.size(p);
                } catch (IOException e) {
                    return 0L;
                }
            }).sum();
        } catch (IOException e) {
            return 0L;
        }
    }

    private void recordSizeMetrics(ImportStatistics stats,
                                   Path shardsDbPath,
                                   Path boundariesDbPath,
                                   Path buildingsDbPath,
                                   Path appendBuildingDbPath,
                                   Path gridIndexDbPath,
                                   Path buildingGridIndexDbPath,
                                   Path nodeCacheDbPath,
                                   Path wayIndexDbPath,
                                   Path boundaryWayIndexDbPath,
                                   Path neededNodesDbPath,
                                   Path relIndexDbPath,
                                   Path poiIndexDbPath,
                                   Path appendDbPath) {
        long shards = computeDirectorySize(shardsDbPath);
        long boundaries = computeDirectorySize(boundariesDbPath);
        long buildings = computeDirectorySize(buildingsDbPath);
        long dataset = shards + boundaries + buildings;

        long grid = computeDirectorySize(gridIndexDbPath);
        long buildingGrid = computeDirectorySize(buildingGridIndexDbPath);
        long node = computeDirectorySize(nodeCacheDbPath);
        long way = computeDirectorySize(wayIndexDbPath);
        long boundaryWay = computeDirectorySize(boundaryWayIndexDbPath);
        long needed = computeDirectorySize(neededNodesDbPath);
        long rel = computeDirectorySize(relIndexDbPath);
        long poi = computeDirectorySize(poiIndexDbPath);
        long append = computeDirectorySize(appendDbPath);
        long appendBuilding = computeDirectorySize(appendBuildingDbPath);
        long tmpTotal = grid + buildingGrid + node + way + boundaryWay + needed + rel + poi + append + appendBuilding;

        stats.setShardsBytes(shards);
        stats.setBoundariesBytes(boundaries);
        stats.setBuildingsBytes(buildings);
        stats.setDatasetBytes(dataset);

        stats.setTmpGridBytes(grid);
        stats.setTmpBuildingGridBytes(buildingGrid);
        stats.setTmpNodeBytes(node);
        stats.setTmpWayBytes(way);
        stats.setTmpBoundaryWayBytes(boundaryWay);
        stats.setTmpNeededBytes(needed);
        stats.setTmpRelBytes(rel);
        stats.setTmpPoiBytes(poi);
        stats.setTmpAppendBytes(append);
        stats.setTmpTotalBytes(tmpTotal);
    }


    private record PoiData(long id, double lat, double lon, String type, String subtype, List<NameData> names,
                           AddressData address, List<HierarchyCache.SimpleHierarchyItem> hierarchy,
                           byte[] boundaryWkb) {
    }

    private record NameData(String lang, String text) {
    }

    private record AddressData(String street, String houseNumber, String postcode, String city, String country) {
    }

    private record BoundaryResultLite(long osmId, int level, String name, String code, org.locationtech.jts.geom.Geometry geometry) {
    }

    private record BuildingData(long id, String name, String code, org.locationtech.jts.geom.Geometry geometry) {
    }

    private Long safeEntityId(EntityContainer container) {
        try {
            OsmEntity e = container.getEntity();
            return e != null ? e.getId() : null;
        } catch (Exception ignore) {
            return null;
        }
    }

    @FunctionalInterface
    private interface ConsumerWithException<T> {
        void accept(T t) throws Exception;
    }

    private static class RocksBatchWriter implements AutoCloseable {
        private final RocksDB db;
        private final WriteOptions writeOptions;
        private final ImportStatistics stats;
        private final int maxOps;
        private final Object lock = new Object();
        private final WriteBatch batch = new WriteBatch();
        private int ops = 0;

        RocksBatchWriter(RocksDB db, int maxOps, ImportStatistics stats) {
            this.db = db;
            this.maxOps = Math.max(1, maxOps);
            this.stats = stats;
            this.writeOptions = new WriteOptions().setDisableWAL(true);
        }

        public void put(byte[] key, byte[] value) throws RocksDBException {
            synchronized (lock) {
                batch.put(key, value);
                ops++;
                if (ops >= maxOps) {
                    flushInternal();
                }
            }
        }

        public void flush() throws RocksDBException {
            synchronized (lock) {
                if (ops > 0) {
                    db.write(writeOptions, batch);
                    stats.incrementRocksDbWrites();
                    batch.clear();
                    ops = 0;
                }
            }
        }

        private void flushInternal() throws RocksDBException {
            db.write(writeOptions, batch);
            stats.incrementRocksDbWrites();
            batch.clear();
            ops = 0;
        }

        @Override
        public void close() throws Exception {
            try {
                flush();
            } finally {
                batch.close();
                if (writeOptions != null) {
                    writeOptions.close();
                }
            }
        }
    }

    private static class PeriodicFlusher implements AutoCloseable {
        private final ScheduledExecutorService scheduler;

        private PeriodicFlusher(ScheduledExecutorService scheduler) {
            this.scheduler = scheduler;
        }

        public static PeriodicFlusher start(String name, long initialDelaySeconds, long periodSeconds, Runnable task) {
            ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, name);
                t.setDaemon(true);
                return t;
            });
            exec.scheduleAtFixedRate(task, initialDelaySeconds, periodSeconds, TimeUnit.SECONDS);
            return new PeriodicFlusher(exec);
        }

        @Override
        public void close() {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(60, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    private static class PoiIndexRec {
        byte kind;
        long id;
        double lat = Double.NaN;
        double lon = Double.NaN;
        String type;
        String subtype;
        List<NameData> names;
        String street;
        String houseNumber;
        String postcode;
        String city;
        String country;
    }

    private record PoiQueueItem(PoiIndexRec rec, long s2SortKey, byte[] cachedWayNodes) {
    }

    private static class RelRec {
        long osmId;
        int level;
        String name;
        String code;
        long[] outer;
        long[] inner;
    }

    private PoiIndexRec buildPoiIndexRecFromEntity(OsmEntity e) {
        PoiIndexRec rec = new PoiIndexRec();
        rec.type = "unknown";
        rec.subtype = "";

        boolean hasPoiType = false;
        boolean hasAddressTags = false;

        for (int i = 0; i < e.getNumberOfTags(); i++) {
            OsmTag t = e.getTag(i);
            String k = t.getKey();

            if (isPoiFastKey(k)) {
                rec.type = intern(t.getKey());
                rec.subtype = intern(t.getValue());
                hasPoiType = true;
            }

            if (k.startsWith("addr:")) {
                hasAddressTags = true;
            }
        }

        if (hasAddressTags && !hasPoiType) {
            rec.type = "address";
            rec.subtype = "node";
        }

        List<NameData> names = new ArrayList<>();
        Set<String> dedup = new HashSet<>();

        for (int i = 0; i < e.getNumberOfTags(); i++) {
            OsmTag t = e.getTag(i);
            String k = t.getKey(), v = t.getValue();
            if (v == null || v.trim().isEmpty()) continue;

            if ("name".equals(k)) {
                if (dedup.add("default:" + v)) names.add(new NameData("default", v));
            } else if (k.startsWith("name:")) {
                String lang = intern(k.substring(5));
                if (dedup.add(lang + ":" + v)) names.add(new NameData(lang, v));
            } else if (k.startsWith("addr:")) {
                switch (k) {
                    case "addr:street" -> rec.street = v;
                    case "addr:housenumber" -> rec.houseNumber = v;
                    case "addr:postcode" -> rec.postcode = v;
                    case "addr:city" -> rec.city = v;
                    case "addr:country" -> rec.country = v;
                }
            }
        }

        rec.names = names;
        return rec;
    }

    private RelRec buildRelRec(OsmRelation r) {
        List<Long> outer = new ArrayList<>();
        List<Long> inner = new ArrayList<>();
        for (int i = 0; i < r.getNumberOfMembers(); i++) {
            OsmRelationMember m = r.getMember(i);
            if (m.getType() == EntityType.Way) {
                String role = m.getRole();
                if ("outer".equals(role) || role == null || role.isEmpty()) outer.add(m.getId());
                else if ("inner".equals(role)) inner.add(m.getId());
            }
        }
        int level = 10;
        String name = null;
        String code = null;
        for (int i = 0; i < r.getNumberOfTags(); i++) {
            OsmTag t = r.getTag(i);
            switch (t.getKey()) {
                case "admin_level" -> {
                    try {
                        level = Integer.parseInt(t.getValue());
                    } catch (NumberFormatException ignore) {
                        level = 10;
                    }
                }
                case "name" -> name = t.getValue();
                case "ISO3166-1" -> code = t.getValue();
                case "ISO3166-1:alpha2" -> {
                    if (code == null) code = t.getValue();
                }
            }
        }
        RelRec rec = new RelRec();
        rec.osmId = r.getId();
        rec.level = level;
        rec.name = name;
        rec.code = code;
        rec.outer = outer.stream().mapToLong(x -> x).toArray();
        rec.inner = inner.stream().mapToLong(x -> x).toArray();
        return rec;
    }

    private byte[] encodePoiIndexRec(PoiIndexRec rec) {
        int namesSize = 4;
        for (NameData n : rec.names != null ? rec.names : List.<NameData>of()) {
            byte[] l = bytes(n.lang());
            byte[] t = bytes(n.text());
            namesSize += 4 + l.length + 4 + t.length;
        }
        byte[] typeB = bytes(rec.type);
        byte[] subtypeB = bytes(rec.subtype);
        byte[] streetB = bytes(rec.street);
        byte[] hnB = bytes(rec.houseNumber);
        byte[] pcB = bytes(rec.postcode);
        byte[] cityB = bytes(rec.city);
        byte[] countryB = bytes(rec.country);
        int cap = 16
                + 4 + typeB.length + 4 + subtypeB.length + namesSize + 4 + streetB.length + 4 + hnB.length + 4 + pcB.length + 4 + cityB.length + 4 + countryB.length;
        ByteBuffer bb = ByteBuffer.allocate(cap);
        bb.putDouble(rec.lat);
        bb.putDouble(rec.lon);
        putBytes(bb, typeB);
        putBytes(bb, subtypeB);
        bb.putInt(rec.names != null ? rec.names.size() : 0);
        if (rec.names != null) {
            for (NameData n : rec.names) {
                putBytes(bb, bytes(n.lang()));
                putBytes(bb, bytes(n.text()));
            }
        }
        putBytes(bb, streetB);
        putBytes(bb, hnB);
        putBytes(bb, pcB);
        putBytes(bb, cityB);
        putBytes(bb, countryB);
        return bb.array();
    }

    private PoiIndexRec decodePoiIndexRec(byte[] b) {
        ByteBuffer bb = ByteBuffer.wrap(b);
        PoiIndexRec rec = new PoiIndexRec();
        rec.lat = bb.getDouble();
        rec.lon = bb.getDouble();
        rec.type = getString(bb);
        rec.subtype = getString(bb);
        int n = bb.getInt();
        List<NameData> names = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            String lang = getString(bb);
            String text = getString(bb);
            names.add(new NameData(lang, text));
        }
        rec.names = names;
        rec.street = getString(bb);
        rec.houseNumber = getString(bb);
        rec.postcode = getString(bb);
        rec.city = getString(bb);
        rec.country = getString(bb);
        return rec;
    }

    private byte[] encodeRelRec(RelRec r) {
        byte[] nameB = bytes(r.name);
        byte[] codeB = bytes(r.code);
        int cap = 4
                + 4 + nameB.length
                + 4 + codeB.length
                + 4 + 8 * (r.outer != null ? r.outer.length : 0)
                + 4 + 8 * (r.inner != null ? r.inner.length : 0);
        ByteBuffer bb = ByteBuffer.allocate(cap);
        bb.putInt(r.level);
        putBytes(bb, nameB);
        putBytes(bb, codeB);
        bb.putInt(r.outer != null ? r.outer.length : 0);
        if (r.outer != null) for (long v : r.outer) bb.putLong(v);
        bb.putInt(r.inner != null ? r.inner.length : 0);
        if (r.inner != null) for (long v : r.inner) bb.putLong(v);
        return bb.array();
    }

    private RelRec decodeRelRec(byte[] b, long id) {
        ByteBuffer bb = ByteBuffer.wrap(b);
        RelRec r = new RelRec();
        r.osmId = id;
        r.level = bb.getInt();
        r.name = getString(bb);
        r.code = getString(bb);
        int oc = bb.getInt();
        r.outer = new long[oc];
        for (int i = 0; i < oc; i++) r.outer[i] = bb.getLong();
        int ic = bb.getInt();
        r.inner = new long[ic];
        for (int i = 0; i < ic; i++) r.inner[i] = bb.getLong();
        return r;
    }

    private byte[] buildPoiKey(byte kind, long id) {
        ByteBuffer bb = ByteBuffer.allocate(1 + 8);
        bb.put(kind);
        bb.putLong(id);
        return bb.array();
    }

    private long bytesToLong(byte[] b, int offset) {
        return ByteBuffer.wrap(b, offset, 8).getLong();
    }

    private static byte[] bytes(String s) {
        return s == null ? new byte[0] : s.getBytes(StandardCharsets.UTF_8);
    }

    private static void putBytes(ByteBuffer bb, byte[] data) {
        bb.putInt(data.length);
        if (data.length > 0) bb.put(data);
    }

    private static String getString(ByteBuffer bb) {
        int len = bb.getInt();
        if (len == 0) return null;
        byte[] dst = new byte[len];
        bb.get(dst);
        return new String(dst, StandardCharsets.UTF_8);
    }

}
