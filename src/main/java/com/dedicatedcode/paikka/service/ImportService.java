package com.dedicatedcode.paikka.service;

import com.dedicatedcode.paikka.config.PaikkaConfiguration;
import com.dedicatedcode.paikka.flatbuffers.*;
import com.dedicatedcode.paikka.flatbuffers.Geometry;
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
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Service responsible for importing OSM data into the Paikka spatial engine.
 * Implements the two-pass ingestion pipeline as described in the implementation blueprint.
 * Optimized for Java 25 using Virtual Threads and RocksDB WriteBatches.
 */
@Service
public class ImportService {

    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
    private final S2Helper s2Helper;
    private final GeometrySimplificationService geometrySimplificationService;
    private final PaikkaConfiguration config;

    // Simple cache to deduplicate common tag strings and reduce heap pressure
    private final Map<String, String> tagCache = new ConcurrentHashMap<>(1000);
    private final int fileReadWindowSize;

    public ImportService(S2Helper s2Helper, GeometrySimplificationService geometrySimplificationService, PaikkaConfiguration config) {
        this.s2Helper = s2Helper;
        this.geometrySimplificationService = geometrySimplificationService;
        this.config = config;
        this.fileReadWindowSize = calculateFileReadWindowSize();
    }

    private int calculateFileReadWindowSize() {
        long maxHeap = Runtime.getRuntime().maxMemory();
        if (maxHeap > 24L * 1024 * 1024 * 1024) {
            return  256 * 1024 * 1024;  // 256MB for 32GB+ heap
        } else if (maxHeap > 8L * 1024 * 1024 * 1024) {
            return  96 * 1024 * 1024;   // 96MB for 8-32GB heap
        } else {
            return 48 * 1024 * 1024;   // 48MB for <8GB heap
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
        //tmp databases only needed for the import
        Path gridIndexDbPath = dataDirectory.resolve("tmp/grid_index");
        Path nodeCacheDbPath = dataDirectory.resolve("tmp/node_cache");
        Path wayIndexDbPath = dataDirectory.resolve("tmp/way_index");
        Path neededNodesDbPath = dataDirectory.resolve("tmp/needed_nodes");

        // Clean up existing databases to ensure fresh start
        cleanupDatabase(shardsDbPath);
        cleanupDatabase(boundariesDbPath);
        cleanupDatabase(gridIndexDbPath);
        cleanupDatabase(nodeCacheDbPath);
        cleanupDatabase(wayIndexDbPath);
        cleanupDatabase(neededNodesDbPath);

        RocksDB.loadLibrary();
        ImportStatistics stats = new ImportStatistics();
        startProgressReporter(stats);

        try (Cache sharedCache = new LRUCache(2 * 1024 * 1024 * 1024L)) {
            BlockBasedTableConfig tableConfig = new BlockBasedTableConfig()
                    .setBlockCache(sharedCache)
                    .setFilterPolicy(new BloomFilter(10, false));

            Options persistentOpts = new Options()
                    .setCreateIfMissing(true)
                    .setTableFormatConfig(tableConfig)
                    .setCompressionType(CompressionType.LZ4_COMPRESSION)
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

            try (RocksDB shardsDb = RocksDB.open(persistentOpts, shardsDbPath.toString());
                 RocksDB boundariesDb = RocksDB.open(persistentOpts, boundariesDbPath.toString());
                 RocksDB gridIndexDb = RocksDB.open(gridOpts, gridIndexDbPath.toString());
                 RocksDB nodeCache = RocksDB.open(nodeOpts, nodeCacheDbPath.toString());
                 RocksDB wayIndexDb = RocksDB.open(wayIndexOpts, wayIndexDbPath.toString());
                 RocksDB neededNodesDb = RocksDB.open(neededNodesOpts, neededNodesDbPath.toString())) {

                printPhaseHeader("PASS 1: Node Harvesting & Admin Boundaries");
                long pass1Start = System.currentTimeMillis();
                pass1NodeHarvestingAndAdminProcessing(pbfFile, nodeCache, gridIndexDb, boundariesDb, wayIndexDb, neededNodesDb, stats);
                printPhaseSummary("PASS 1", pass1Start, stats);

                printPhaseHeader("PASS 2: POI Sharding & Hierarchy Baking");
                long pass2Start = System.currentTimeMillis();
                pass2PoiShardingAndBaking(pbfFile, nodeCache, gridIndexDb, shardsDb, boundariesDb, stats);
                printPhaseSummary("PASS 2", pass2Start, stats);

                stats.setTotalTime(System.currentTimeMillis() - totalStartTime);
                stats.stop();

                printFinalStatistics(stats);
                printSuccess();

            } catch (Exception e) {
                stats.stop();
                printError("IMPORT FAILED: " + e.getMessage());
                e.printStackTrace();
                throw e;
            }
        }
    }

    private void startProgressReporter(ImportStatistics stats) {
        boolean isTty = System.console() != null;

        Thread.ofPlatform().daemon().start(() -> {
            while (stats.isRunning()) {
                long elapsed = System.currentTimeMillis() - stats.getStartTime();
                double seconds = elapsed / 1000.0;

                String phase = stats.getCurrentPhase();
                StringBuilder sb = new StringBuilder();

                if (isTty) {
                    sb.append("\r\033[K"); // Carriage return and clear line
                }

                // Phase-specific progress display
                if (phase.contains("1.1.1")) {
                    long pbfPerSec = seconds > 0 ? (long)(stats.getEntitiesRead() / seconds) : 0;
                    sb.append(String.format("\033[1;36m[%s]\033[0m \033[1mScanning PBF Structure\033[0m", formatTime(elapsed)));
                    sb.append(String.format(" │ \033[32mPBF Entities:\033[0m %s \033[33m(%s/s)\033[0m",
                                            formatCompactNumber(stats.getEntitiesRead()), formatCompactNumber(pbfPerSec)));
                    sb.append(String.format(" │ \033[34mWays Found:\033[0m %s", formatCompactNumber(stats.getWaysProcessed())));
                    sb.append(String.format(" │ \033[37mNodes Found:\033[0m %s", formatCompactNumber(stats.getNodesFound())));
                    sb.append(String.format(" │ \033[35mRelations:\033[0m %s", formatCompactNumber(stats.getRelationsFound())));

                } else if (phase.contains("1.1.2")) {
                    long nodesPerSec = seconds > 0 ? (long)(stats.getNodesCached() / seconds) : 0;
                    sb.append(String.format("\033[1;36m[%s]\033[0m \033[1mCaching Node Coordinates\033[0m", formatTime(elapsed)));
                    sb.append(String.format(" │ \033[32mNodes Cached:\033[0m %s \033[33m(%s/s)\033[0m",
                                            formatCompactNumber(stats.getNodesCached()), formatCompactNumber(nodesPerSec)));
                    sb.append(String.format(" │ \033[36mQueue:\033[0m %s", formatCompactNumber(stats.getQueueSize())));
                    sb.append(String.format(" │ \033[37mThreads:\033[0m %d", stats.getActiveThreads()));

                } else if (phase.contains("1.2")) {
                    long boundsPerSec = seconds > 0 ? (long)(stats.getBoundariesProcessed() / seconds) : 0;
                    sb.append(String.format("\033[1;36m[%s]\033[0m \033[1mProcessing Admin Boundaries\033[0m", formatTime(elapsed)));
                    sb.append(String.format(" │ \033[32mBoundaries:\033[0m %s \033[33m(%s/s)\033[0m",
                                            formatCompactNumber(stats.getBoundariesProcessed()), formatCompactNumber(boundsPerSec)));
                    sb.append(String.format(" │ \033[37mThreads:\033[0m %d", stats.getActiveThreads()));

                } else if (phase.contains("2.1")) {
                    long poisPerSec = seconds > 0 ? (long)(stats.getPoisProcessed() / seconds) : 0;
                    sb.append(String.format("\033[1;36m[%s]\033[0m \033[1mProcessing POIs & Sharding\033[0m", formatTime(elapsed)));
                    sb.append(String.format(" │ \033[32mPOIs Processed:\033[0m %s \033[33m(%s/s)\033[0m",
                                            formatCompactNumber(stats.getPoisProcessed()), formatCompactNumber(poisPerSec)));
                    sb.append(String.format(" │ \033[36mQueue:\033[0m %s", formatCompactNumber(stats.getQueueSize())));
                    sb.append(String.format(" │ \033[37mThreads:\033[0m %d", stats.getActiveThreads()));

                } else {
                    sb.append(String.format("\033[1;36m[%s]\033[0m %s", formatTime(elapsed), phase));
                }

                sb.append(String.format(" │ \033[31mHeap:\033[0m %s", stats.getMemoryStats()));

                if (isTty) {
                    System.out.print(sb.toString());
                    System.out.flush();
                } else {
                    System.out.println(sb.toString());
                }

                try {
                    Thread.sleep(isTty ? 500 : 5000);
                } catch (InterruptedException e) {
                    break;
                }
            }
            if (isTty) System.out.println();
        });
    }

    private void updateGridIndexEntry(RocksDB gridIndexDb, long cellId, long osmId) throws Exception {
        byte[] key = s2Helper.longToByteArray(cellId);
        synchronized (this) {
            byte[] existingData = gridIndexDb.get(key);
            long[] newArray;
            if (existingData == null) {
                newArray = new long[]{osmId};
            } else {
                long[] oldArray = s2Helper.byteArrayToLongArray(existingData);
                if (Arrays.stream(oldArray).anyMatch(id -> id == osmId)) return;
                newArray = Arrays.copyOf(oldArray, oldArray.length + 1);
                newArray[oldArray.length] = osmId;
            }
            gridIndexDb.put(key, s2Helper.longArrayToByteArray(newArray));
        }
    }

    private void pass1NodeHarvestingAndAdminProcessing(Path pbfFile,
                                                       RocksDB nodeCache,
                                                       RocksDB gridIndexDb,
                                                       RocksDB boundariesDb,
                                                       RocksDB wayIndexDb,
                                                       RocksDB neededNodesDb,
                                                       ImportStatistics stats) throws Exception {

        stats.setCurrentPhase("1.1.1: Scanning PBF for structure");
        long scanStart = System.currentTimeMillis();
        harvestWaysAndMarkNeededNodes(pbfFile, wayIndexDb, neededNodesDb, stats);
        printSubPhaseSummary("PBF Structure Scan", scanStart, stats);

        stats.setCurrentPhase("1.1.2: Caching node coordinates");
        long cacheStart = System.currentTimeMillis();
        cacheNeededNodeCoordinates(pbfFile, neededNodesDb, nodeCache, stats);
        printSubPhaseSummary("Caching Node Coordinates", cacheStart, stats);

        stats.setCurrentPhase("1.2: Processing administrative boundaries");
        long subPhase2Start = System.currentTimeMillis();
        processAdministrativeBoundariesStreaming(pbfFile, nodeCache, wayIndexDb, gridIndexDb, boundariesDb, stats);
        printSubPhaseSummary("Boundary Processing", subPhase2Start, stats);
    }

    private void pass2PoiShardingAndBaking(Path pbfFile, RocksDB nodeCache, RocksDB gridIndexDb,
                                           RocksDB shardsDb, RocksDB boundariesDb, ImportStatistics stats) throws Exception {
        stats.setCurrentPhase("2.1: Processing POIs and building shards");
        final Map<Long, List<PoiData>> shardBuffer = new ConcurrentHashMap<>();

        Runnable flushTask = () -> {
            try {
                Map<Long, List<PoiData>> bufferToFlush = new HashMap<>();
                synchronized (shardBuffer) {
                    shardBuffer.forEach((key, value) -> {
                        if (!value.isEmpty()) {
                            bufferToFlush.put(key, new ArrayList<>(value));
                            value.clear();
                        }
                    });
                }
                if (!bufferToFlush.isEmpty()) {
                    writeShardBatch(bufferToFlush, shardsDb, stats);
                }
            } catch (Exception e) {
                // Silent fail for periodic flush
            }
        };
        // Generic periodic flusher (10s)
        try (PeriodicFlusher shardFlusher = PeriodicFlusher.start("shard-buffer-flush", 10, 10, flushTask)) {

            BlockingQueue<List<EntityContainer>> entityBatchQueue = new LinkedBlockingQueue<>(10_000);
            int numProcessors = config.getMaxImportThreads();
            CountDownLatch latch = new CountDownLatch(numProcessors);

            try (ExecutorService executor = createExecutorService(numProcessors)) {
                final ThreadLocal<HierarchyCache> hierarchyCacheThreadLocal = ThreadLocal.withInitial(
                        () -> new HierarchyCache(boundariesDb, gridIndexDb, s2Helper)
                );

                Thread readerThread = Thread.ofVirtual().start(() -> {
                    List<EntityContainer> spatialBuffer = new ArrayList<>(50000);
                    try {
                        withPbfIterator(pbfFile, iterator -> {
                            while (iterator.hasNext()) {
                                EntityContainer container = iterator.next();
                                stats.incrementEntitiesRead();
                                OsmEntity entity = container.getEntity();
                                if ((entity instanceof OsmNode && isPoi(entity)) || (entity instanceof OsmWay && isPoi(entity))) {
                                    spatialBuffer.add(container);
                                    if (spatialBuffer.size() >= 50000) {
                                        double[] coordinate = getCoordinate(entity, nodeCache);
                                        spatialBuffer.sort(Comparator.comparingLong(c -> s2Helper.getS2CellId(coordinate[1], coordinate[0], S2Helper.GRID_LEVEL)));
                                        queueBuffer(stats, entityBatchQueue, spatialBuffer, 500);
                                        spatialBuffer.clear();
                                    }

                                }
                            }
                            if (!spatialBuffer.isEmpty())  {
                                queueBuffer(stats, entityBatchQueue, spatialBuffer, 500);
                            }
                        });
                    } catch (Exception e) {
                        // Reader failed
                    } finally {
                        for (int i = 0; i < numProcessors; i++) {
                            try {
                                entityBatchQueue.put(Collections.singletonList(createPoisonPill()));
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                break;
                            }
                        }
                    }
                });

                for (int i = 0; i < numProcessors; i++) {
                    executor.submit(() -> {
                        stats.incrementActiveThreads();
                        final Map<Long, List<PoiData>> localShardBuffer = new HashMap<>();
                        final GeometryFactory geometryFactory = new GeometryFactory();
                        final HierarchyCache hierarchyCache = hierarchyCacheThreadLocal.get();
                        try {
                            while (true) {
                                List<EntityContainer> batch = entityBatchQueue.take();
                                stats.setQueueSize(entityBatchQueue.size());
                                if (isPoisonPill(batch)) break;
                                for (EntityContainer container : batch) {
                                    try {
                                        PoiData poiData = processPoi(container.getEntity(), nodeCache, hierarchyCache, geometryFactory);
                                        if (poiData != null) {
                                            localShardBuffer.computeIfAbsent(s2Helper.getShardId(poiData.lat(), poiData.lon()), k -> new ArrayList<>()).add(poiData);
                                        }
                                        stats.incrementPoisProcessed();
                                    } catch (Exception e) { /* Skip */ }
                                }

                                if (localShardBuffer.size() > 5000) {
                                    synchronized (shardBuffer) {
                                        localShardBuffer.forEach((shardId, poiList) -> shardBuffer.computeIfAbsent(shardId, k -> new ArrayList<>()).addAll(poiList));
                                    }
                                    localShardBuffer.clear();
                                }
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } finally {
                            synchronized (shardBuffer) {
                                localShardBuffer.forEach((shardId, poiList) -> shardBuffer.computeIfAbsent(shardId, k -> new ArrayList<>()).addAll(poiList));
                            }
                            hierarchyCacheThreadLocal.remove();
                            stats.decrementActiveThreads();
                            latch.countDown();
                        }
                    });
                }

                latch.await();
                readerThread.join();
            }
            // Final flush
            flushTask.run();
        }
    }

    private void queueBuffer(ImportStatistics stats, BlockingQueue<List<EntityContainer>> entityBatchQueue, List<EntityContainer> spatialBuffer, int queueSize) throws InterruptedException {
        for (int i = 0; i < spatialBuffer.size(); i += queueSize) {
            int end = Math.min(i + queueSize, spatialBuffer.size());
            List<EntityContainer> batch = new ArrayList<>(spatialBuffer.subList(i, end));
            entityBatchQueue.put(new ArrayList<>(batch));
            stats.setQueueSize(entityBatchQueue.size());
        }
    }

    private double[] getCoordinate(OsmEntity entity, RocksDB nodeCache) throws RocksDBException {
        if (entity instanceof OsmNode node) {
            return new double[]{node.getLatitude(), node.getLongitude()};
        } else if (entity instanceof OsmWay way) {
            long firstNodeId = way.getNodeId(0);
            byte[] value = nodeCache.get(s2Helper.longToByteArray(firstNodeId));
            if (value == null) return new double[]{0, 0};
            ByteBuffer buffer = ByteBuffer.wrap(value);
            return new double[]{buffer.getDouble(8), buffer.getDouble(0)};
        }
        return new double[]{0, 0};
    }

    // PHASE 1.1.1: Scan ways and mark needed nodes off-heap; store way->nodes to RocksDB way_index
    private void harvestWaysAndMarkNeededNodes(Path pbfFile, RocksDB wayIndexDb, RocksDB neededNodesDb, ImportStatistics stats) throws Exception {
        final byte[] ONE = new byte[]{1};

        try (RocksBatchWriter wayWriter = new RocksBatchWriter(wayIndexDb, 5_000, stats);
             RocksBatchWriter neededWriter = new RocksBatchWriter(neededNodesDb, 200_000, stats)) {

            withPbfIterator(pbfFile, iterator -> {
                while (iterator.hasNext()) {
                    EntityContainer container = iterator.next();
                    stats.incrementEntitiesRead();

                    if (container.getType() == EntityType.Way) {
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
                            byte[] value = s2Helper.longArrayToByteArray(nodeIds);
                            wayWriter.put(s2Helper.longToByteArray(way.getId()), value);
                        }
                    } else if (container.getType() == EntityType.Relation) {
                        if (isAdministrativeBoundary((OsmRelation) container.getEntity())) {
                            stats.incrementRelationsFound();
                        }
                    } else if (container.getType() == EntityType.Node) {
                        if (isPoi(container.getEntity())) {
                            stats.incrementNodesFound();
                        }
                    }
                }
            });

            wayWriter.flush();
            neededWriter.flush();
        }
    }

    // PHASE 1.1.2: Stream nodes in batches, check membership via needed_nodes multiGet, write coords to node_cache
    private void cacheNeededNodeCoordinates(Path pbfFile, RocksDB neededNodesDb, RocksDB nodeCache, ImportStatistics stats) throws Exception {
        final int BATCH_SIZE = 50_000;

        BlockingQueue<List<OsmNode>> nodeBatchQueue = new LinkedBlockingQueue<>(200);
        int numProcessors = Math.max(1, config.getMaxImportThreads());
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
                            buf.clear();
                            stats.setQueueSize(nodeBatchQueue.size());
                        }
                    });
                } catch (Exception e) {
                    // ignore
                } finally {
                    // poison
                    for (int i = 0; i < numProcessors; i++) {
                        try {
                            nodeBatchQueue.put(Collections.emptyList());
                        } catch (InterruptedException ex) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                }
            });

            for (int i = 0; i < numProcessors; i++) {
                executor.submit(() -> {
                    stats.incrementActiveThreads();
                    final ThreadLocal<RocksBatchWriter> nodeWriterLocal =
                            ThreadLocal.withInitial(() -> new RocksBatchWriter(nodeCache, 50_000, stats));
                    try {
                        while (true) {
                            List<OsmNode> nodes = nodeBatchQueue.take();
                            stats.setQueueSize(nodeBatchQueue.size());
                            if (nodes.isEmpty()) break;

                            // Build keys for multiGet on needed_nodes
                            List<byte[]> keys = new ArrayList<>(nodes.size());
                            for (OsmNode n : nodes) keys.add(s2Helper.longToByteArray(n.getId()));
                            List<byte[]> presence = neededNodesDb.multiGetAsList(keys);

                            RocksBatchWriter nodeWriter = nodeWriterLocal.get();
                            for (int idx = 0; idx < nodes.size(); idx++) {
                                if (presence.get(idx) != null) {
                                    OsmNode n = nodes.get(idx);
                                    byte[] val = ByteBuffer.allocate(16)
                                            .putDouble(n.getLatitude())
                                            .putDouble(n.getLongitude())
                                            .array();
                                    try {
                                        nodeWriter.put(s2Helper.longToByteArray(n.getId()), val);
                                        stats.incrementNodesCached();
                                    } catch (RocksDBException e) {
                                        // ignore single put failure
                                    }
                                }
                            }
                        }
                        // flush thread-local writer
                        try {
                            nodeWriterLocal.get().flush();
                        } catch (Exception ignore) { }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (RocksDBException e) {
                        throw new RuntimeException(e);
                    } finally {
                        try {
                            nodeWriterLocal.get().close();
                        } catch (Exception ignore) { }
                        nodeWriterLocal.remove();
                        stats.decrementActiveThreads();
                        latch.countDown();
                    }
                });
            }

            latch.await();
            reader.join();
        }
    }

    // PHASE 1.2: Stream relations and process admin boundaries reading way->nodes from RocksDB
    private void processAdministrativeBoundariesStreaming(Path pbfFile,
                                                          RocksDB nodeCache,
                                                          RocksDB wayIndexDb,
                                                          RocksDB gridsIndexDb,
                                                          RocksDB boundariesDb,
                                                          ImportStatistics stats) throws Exception {
        int maxConcurrentGeometries = 100;
        Semaphore semaphore = new Semaphore(maxConcurrentGeometries);

        int numThreads = Math.max(1, config.getMaxImportThreads());
        ExecutorService executor = createExecutorService(numThreads);
        ExecutorCompletionService<BoundaryResult> ecs = new ExecutorCompletionService<>(executor);
        AtomicInteger submitted = new AtomicInteger(0);

        try (RocksBatchWriter boundariesWriter = new RocksBatchWriter(boundariesDb, 500, stats)) {
            withPbfIterator(pbfFile, iterator -> {
                while (iterator.hasNext()) {
                    EntityContainer c = iterator.next();
                    if (c.getType() == EntityType.Relation) {
                        OsmRelation relation = (OsmRelation) c.getEntity();
                        if (!isAdministrativeBoundary(relation)) continue;

                        semaphore.acquire();
                        stats.incrementActiveThreads();
                        ecs.submit(() -> {
                            try {
                                org.locationtech.jts.geom.Geometry geometry = buildGeometryFromRelation(relation, nodeCache, wayIndexDb);
                                if (geometry != null && geometry.isValid()) {
                                    org.locationtech.jts.geom.Geometry simplified = geometrySimplificationService.simplifyByAdminLevel(geometry, getAdminLevel(relation));
                                    return new BoundaryResult(relation, simplified);
                                }
                            } finally {
                                // no-op
                            }
                            return null;
                        });
                        submitted.incrementAndGet();

                        // Opportunistically drain results to bound latency/memory
                        if (submitted.get() % maxConcurrentGeometries == 0) {
                            for (int i = 0; i < maxConcurrentGeometries; i++) {
                                try {
                                    Future<BoundaryResult> f = ecs.take();
                                    BoundaryResult r = f.get();
                                    if (r != null) {
                                        storeBoundary(r.relation(), r.geometry(), boundariesWriter, gridsIndexDb, stats);
                                        stats.incrementBoundariesProcessed();
                                    }
                                } catch (Exception e) {
                                    // ignore one failure
                                } finally {
                                    semaphore.release();
                                    stats.decrementActiveThreads();
                                }
                            }
                        }
                    }
                }
            });

            // Drain remaining
            for (int i = 0; i < submitted.get(); i++) {
                try {
                    Future<BoundaryResult> f = ecs.take();
                    BoundaryResult r = f.get();
                    if (r != null) {
                        storeBoundary(r.relation(), r.geometry(), boundariesWriter, gridsIndexDb, stats);
                        stats.incrementBoundariesProcessed();
                    }
                } catch (Exception e) {
                    // ignore
                } finally {
                    semaphore.release();
                    stats.decrementActiveThreads();
                }
            }
            // Ensure any pending boundary batch is flushed
            boundariesWriter.flush();

        } finally {
            executor.shutdown();
            executor.awaitTermination(30, TimeUnit.MINUTES);
        }
    }

    private PoiData processPoi(OsmEntity entity, RocksDB nodeCache, HierarchyCache hierarchyCache, GeometryFactory geometryFactory) {
        double[] coordinates = getPoiCoordinates(entity, nodeCache);
        if (coordinates == null) return null;
        double lat = coordinates[0], lon = coordinates[1];
        Point point = geometryFactory.createPoint(new Coordinate(lon, lat));
        List<HierarchyCache.SimpleHierarchyItem> hierarchy = hierarchyCache.resolve(point);

        byte[] boundaryWkb = getPoiBoundaryWkb(entity, nodeCache, geometryFactory);
        return createPoiData(entity, lat, lon, hierarchy, boundaryWkb);
    }

    private byte[] getPoiBoundaryWkb(OsmEntity entity, RocksDB nodeCache, GeometryFactory geometryFactory) {
        if (!(entity instanceof OsmWay way)) return null;

        List<Long> nodeIds = new ArrayList<>();
        for (int i = 0; i < way.getNumberOfNodes(); i++) nodeIds.add(way.getNodeId(i));
        if (nodeIds.size() < 3) return null;

        try {
            List<Coordinate> coords = new ArrayList<>(nodeIds.size());
            for (Long nodeId : nodeIds) {
                byte[] value = nodeCache.get(s2Helper.longToByteArray(nodeId));
                if (value == null || value.length != 16) return null;
                ByteBuffer buffer = ByteBuffer.wrap(value);
                coords.add(new Coordinate(buffer.getDouble(8), buffer.getDouble(0)));
            }

            if (!coords.getFirst().equals2D(coords.getLast())) coords.add(new Coordinate(coords.getFirst()));
            if (coords.size() < 4) return null;

            LinearRing shell = geometryFactory.createLinearRing(coords.toArray(new Coordinate[0]));
            Polygon polygon = geometryFactory.createPolygon(shell);
            if (!polygon.isValid()) return null;

            return new WKBWriter().write(polygon);
        } catch (Exception e) {
            return null;
        }
    }

    private PoiData createPoiData(OsmEntity entity, double lat, double lon, List<HierarchyCache.SimpleHierarchyItem> hierarchy, byte[] boundaryWkb) {
        String type = "unknown", subtype = "";
        for (int i = 0; i < entity.getNumberOfTags(); i++) {
            OsmTag tag = entity.getTag(i);
            if (isPoiFastKey(tag.getKey())) {
                type = intern(tag.getKey());
                subtype = intern(tag.getValue());
                break;
            }
        }
        List<NameData> names = new ArrayList<>();
        Set<String> processedNames = new HashSet<>();
        for (int i = 0; i < entity.getNumberOfTags(); i++) {
            OsmTag tag = entity.getTag(i);
            String key = tag.getKey(), value = tag.getValue();
            if (value == null || value.trim().isEmpty()) continue;
            String lang = "default";
            if (key.startsWith("name:")) lang = intern(key.substring(5));
            else if (!"name".equals(key)) continue;
            if (processedNames.add(lang + ":" + value)) names.add(new NameData(lang, value));
        }

        AddressData addressData = extractAddress(entity, hierarchy);
        return new PoiData(entity.getId(), lat, lon, type, subtype, names, addressData, hierarchy, boundaryWkb);
    }

    private String intern(String s) {
        if (s == null) return null;
        return tagCache.computeIfAbsent(s, k -> k);
    }

    private AddressData extractAddress(OsmEntity entity, List<HierarchyCache.SimpleHierarchyItem> hierarchy) {
        String street = null, houseNumber = null, postcode = null, city = null, country = null;
        for (int i = 0; i < entity.getNumberOfTags(); i++) {
            OsmTag tag = entity.getTag(i);
            String key = tag.getKey(), value = tag.getValue();
            if (value == null || value.trim().isEmpty()) continue;
            switch (key) {
                case "addr:street" -> street = value;
                case "addr:housenumber" -> houseNumber = value;
                case "addr:postcode" -> postcode = value;
                case "addr:city" -> city = value;
                case "addr:country" -> country = value;
            }
        }

        if (city == null || country == null) {
            List<HierarchyCache.SimpleHierarchyItem> sortedHierarchy = new ArrayList<>(hierarchy);
            sortedHierarchy.sort(Comparator.comparingInt(HierarchyCache.SimpleHierarchyItem::level).reversed());
            for (HierarchyCache.SimpleHierarchyItem item : sortedHierarchy) {
                if (city == null && item.level() >= 6 && item.level() <= 10) city = item.name();
                if (country == null && item.level() == 2) country = item.name();
                if (city != null && country != null) break;
            }
        }

        if (street == null && houseNumber == null && postcode == null && city == null && country == null) return null;
        return new AddressData(street, houseNumber, postcode, city, country);
    }

    private void writeShardBatch(Map<Long, List<PoiData>> shardBuffer, RocksDB shardsDb, ImportStatistics stats) throws Exception {
        try (WriteBatch batch = new WriteBatch()) {
            for (Map.Entry<Long, List<PoiData>> entry : shardBuffer.entrySet()) {
                List<PoiData> pois = entry.getValue();
                if (pois.isEmpty()) continue;

                FlatBufferBuilder builder = new FlatBufferBuilder(pois.size() * 512);
                int[] poiOffsets = new int[pois.size()];
                int i = 0;
                for (PoiData poi : pois) {
                    int typeOff = builder.createString(poi.type());
                    int subtypeOff = builder.createString(poi.subtype());
                    int[] nameOffs = poi.names().stream().mapToInt(n -> Name.createName(builder, builder.createString(n.lang()), builder.createString(n.text()))).toArray();
                    int namesVecOff = POI.createNamesVector(builder, nameOffs);

                    int addressOff = 0;
                    AddressData addr = poi.address();
                    if (addr != null) {
                        int streetOff = addr.street() != null ? builder.createString(addr.street()) : 0;
                        int houseNumberOff = addr.houseNumber() != null ? builder.createString(addr.houseNumber()) : 0;
                        int postcodeOff = addr.postcode() != null ? builder.createString(addr.postcode()) : 0;
                        int cityOff = addr.city() != null ? builder.createString(addr.city()) : 0;
                        int countryOff = addr.country() != null ? builder.createString(addr.country()) : 0;
                        addressOff = Address.createAddress(builder, streetOff, houseNumberOff, postcodeOff, cityOff, countryOff);
                    }

                    int[] hierOffs = poi.hierarchy().stream().mapToInt(h -> HierarchyItem.createHierarchyItem(builder, h.level(), builder.createString(h.type()), builder.createString(h.name()), h.osmId())).toArray();
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
                    POI.addAddress(builder, addressOff);
                    POI.addHierarchy(builder, hierVecOff);
                    if (boundaryOff > 0) POI.addBoundary(builder, boundaryOff);
                    poiOffsets[i++] = POI.endPOI(builder);
                }
                int poisVectorOffset = POIList.createPoisVector(builder, poiOffsets);
                int poiListOffset = POIList.createPOIList(builder, poisVectorOffset);
                builder.finish(poiListOffset);
                batch.put(s2Helper.longToByteArray(entry.getKey()), builder.sizedByteArray());
            }
            shardsDb.write(new WriteOptions(), batch);
            stats.incrementRocksDbWrites();
        }
    }

    // Modified to read way->nodes from RocksDB instead of in-memory map
    private org.locationtech.jts.geom.Geometry buildGeometryFromRelation(OsmRelation relation, RocksDB nodeCache, RocksDB wayIndexDb) {
        List<Long> outerWayIds = new ArrayList<>(), innerWayIds = new ArrayList<>();
        for (int i = 0; i < relation.getNumberOfMembers(); i++) {
            OsmRelationMember member = relation.getMember(i);
            if (member.getType() == EntityType.Way) {
                String role = member.getRole();
                if ("outer".equals(role) || role == null || role.isEmpty()) outerWayIds.add(member.getId());
                else if ("inner".equals(role)) innerWayIds.add(member.getId());
            }
        }
        List<List<Coordinate>> outerRings = buildConnectedRings(outerWayIds, nodeCache, wayIndexDb);
        List<List<Coordinate>> innerRings = buildConnectedRings(innerWayIds, nodeCache, wayIndexDb);
        if (outerRings.isEmpty()) return null;
        List<Polygon> validPolygons = new ArrayList<>();
        for (List<Coordinate> outerRing : outerRings) {
            try {
                LinearRing shell = GEOMETRY_FACTORY.createLinearRing(outerRing.toArray(new Coordinate[0]));
                List<LinearRing> holes = new ArrayList<>();
                for (List<Coordinate> innerRing : innerRings)
                    try {
                        holes.add(GEOMETRY_FACTORY.createLinearRing(innerRing.toArray(new Coordinate[0])));
                    } catch (Exception e) { }
                Polygon polygon = GEOMETRY_FACTORY.createPolygon(shell, holes.toArray(new LinearRing[0]));
                if (polygon.isValid()) validPolygons.add(polygon);
            } catch (Exception e) { }
        }
        if (validPolygons.isEmpty()) return null;
        return validPolygons.size() == 1 ? validPolygons.getFirst() : GEOMETRY_FACTORY.createMultiPolygon(validPolygons.toArray(new Polygon[0]));
    }

    private List<Coordinate> buildCoordinatesFromWay(Long wayId, RocksDB nodeCache, RocksDB wayIndexDb) {
        try {
            byte[] nodeSeq = wayIndexDb.get(s2Helper.longToByteArray(wayId));
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
            case "amenity", "shop", "tourism", "leisure", "natural", "office", "craft", "healthcare", "emergency", "historic", "man_made", "place", "sport", "public_transport", "railway", "aeroway", "building" -> true;
            case null, default -> false;
        };
    }

    private boolean isPoi(OsmEntity entity) {
        if (entity.getNumberOfTags() == 0) return false;
        for (int i = 0; i < entity.getNumberOfTags(); i++) {
            OsmTag tag = entity.getTag(i);
            String key = tag.getKey();
            switch (key) {
                case "amenity":
                    String av = tag.getValue();
                    return switch (av) {
                        case "fountain", "swimming_pool" -> false;
                        default -> true;
                    };
                case "shop", "tourism", "leisure":
                    return switch (tag.getValue()) {
                        case "picnic_table", "swimming_pool", "theatre", "water_point", "outdoor_seating" -> false;
                        default -> true;
                    };
                case "building":
                    String bv = tag.getValue();
                    return !("yes".equals(bv) || "house".equals(bv) || "residential".equals(bv));
                case "landuse":
                    String lv = tag.getValue();
                    return !("residential".equals(lv) || "commercial".equals(lv) || "industrial".equals(lv));
                case "natural":
                    return switch (tag.getValue()) {
                        case "tree", "grass" -> false;
                        default -> true;
                    };
                default:
                    if (isPoiFastKey(key)) return true;
            }
        }
        return false;
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

    private int getAdminLevel(OsmRelation r) {
        for (int i = 0; i < r.getNumberOfTags(); i++)
            if ("admin_level".equals(r.getTag(i).getKey())) try {
                return Integer.parseInt(r.getTag(i).getValue());
            } catch (NumberFormatException e) {
                return 10;
            }
        return 10;
    }

    private String getBoundaryName(OsmRelation r) {
        for (int i = 0; i < r.getNumberOfTags(); i++)
            if ("name".equals(r.getTag(i).getKey())) return r.getTag(i).getValue();
        return "Unknown";
    }

    private double[] getPoiCoordinates(OsmEntity entity, RocksDB nodeCache) {
        if (entity instanceof OsmNode node) {
            return new double[]{node.getLatitude(), node.getLongitude()};
        } else if (entity instanceof OsmWay way && way.getNumberOfNodes() > 0) {
            try {
                byte[] value = nodeCache.get(s2Helper.longToByteArray(way.getNodeId(0)));
                if (value != null && value.length == 16) {
                    ByteBuffer buffer = ByteBuffer.wrap(value);
                    return new double[]{buffer.getDouble(), buffer.getDouble()};
                }
            } catch (Exception e) { }
        }
        return null;
    }

    // Modified to use wayIndexDb and nodeCache
    private List<List<Coordinate>> buildConnectedRings(List<Long> wayIds, RocksDB nodeCache, RocksDB wayIndexDb) {
        if (wayIds.isEmpty()) return Collections.emptyList();
        Map<Long, List<Coordinate>> wayCoordinates = new HashMap<>();
        for (Long wayId : wayIds) {
            List<Coordinate> coords = buildCoordinatesFromWay(wayId, nodeCache, wayIndexDb);
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
            if (ring.size() >= 3 && !ring.getFirst().equals2D(ring.getLast())) ring.add(new Coordinate(ring.getFirst()));
            if (ring.size() >= 4) rings.add(ring);
        }
        return rings;
    }

    private void storeBoundary(OsmRelation relation,
                               org.locationtech.jts.geom.Geometry geometry,
                               RocksBatchWriter boundariesWriter,
                               RocksDB gridsIndexDb,
                               ImportStatistics stats) throws Exception {
        FlatBufferBuilder fbb = new FlatBufferBuilder(1024);
        byte[] wkb = new WKBWriter().write(geometry);
        int geomDataOffset = Geometry.createDataVector(fbb, wkb);
        int geomOffset = Geometry.createGeometry(fbb, geomDataOffset);
        Envelope mbr = geometry.getEnvelopeInternal();
        MaximumInscribedCircle mic = new MaximumInscribedCircle(geometry, 0.00001);
        double radius = mic.getRadiusLine().getLength();
        Coordinate center = mic.getCenter().getCoordinate();
        double offset = radius / Math.sqrt(2);
        int nameOffset = fbb.createString(getBoundaryName(relation));
        long osmId = relation.getId();
        Boundary.startBoundary(fbb);
        Boundary.addOsmId(fbb, osmId);
        Boundary.addLevel(fbb, getAdminLevel(relation));
        Boundary.addName(fbb, nameOffset);
        Boundary.addMinX(fbb, mbr.getMinX());
        Boundary.addMinY(fbb, mbr.getMinY());
        Boundary.addMaxX(fbb, mbr.getMaxX());
        Boundary.addMaxY(fbb, mbr.getMaxY());
        if (radius > 0) {
            Boundary.addMirMinX(fbb, center.x - offset);
            Boundary.addMirMinY(fbb, center.y - offset);
            Boundary.addMirMaxX(fbb, center.x + offset);
            Boundary.addMirMaxY(fbb, center.y + offset);
        }
        Boundary.addGeometry(fbb, geomOffset);
        int root = Boundary.endBoundary(fbb);
        fbb.finish(root);

        // Grid index updates (read-modify-write)
        S2LatLng low = S2LatLng.fromDegrees(mbr.getMinY(), mbr.getMinX());
        S2LatLng high = S2LatLng.fromDegrees(mbr.getMaxY(), mbr.getMaxX());
        S2LatLngRect rect = S2LatLngRect.fromPointPair(low, high);
        S2RegionCoverer coverer = S2RegionCoverer.builder().setMinLevel(S2Helper.GRID_LEVEL).setMaxLevel(S2Helper.GRID_LEVEL).build();
        ArrayList<S2CellId> covering = new ArrayList<>();
        coverer.getCovering(rect, covering);
        for (S2CellId cellId : covering) updateGridIndexEntry(gridsIndexDb, cellId.id(), osmId);

        // Boundary store (batched)
        boundariesWriter.put(s2Helper.longToByteArray(osmId), fbb.sizedByteArray());
    }

    private EntityContainer createPoisonPill() {
        return new EntityContainer(null, null);
    }

    private boolean isPoisonPill(List<EntityContainer> c) {
        return c.getFirst().getType() == null;
    }

    private void withPbfIterator(Path pbfFile, ConsumerWithException<OsmIterator> consumer) throws Exception {
        try (RandomAccessFile file = new RandomAccessFile(pbfFile.toFile(), "r");
             FileChannel channel = file.getChannel()) {

            long fileSize = channel.size();

            // Use a custom InputStream that reads through the channel in chunks
            InputStream inputStream = new InputStream() {
                private long position = 0;
                private ByteBuffer currentBuffer = null;

                private void refillBuffer() throws IOException {
                    if (position >= fileSize) {
                        currentBuffer = null;
                        return;
                    }

                    // Calculate how much to read
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

    private String formatTime(long ms) {
        long s = ms / 1000;
        return String.format("%d:%02d:%02d", s / 3600, (s % 3600) / 60, s % 60);
    }

    private String formatCompactNumber(long n) {
        if (n < 1000) return String.valueOf(n);
        if (n < 1_000_000) return String.format("%.1fk", n / 1000.0);
        return String.format("%.1fM", n / 1_000_000.0);
    }

    private String centerText(String text) {
        int pad = (80 - text.length()) / 2;
        return " ".repeat(Math.max(0, pad)) + text;
    }

    private void printHeader(String pbfFilePath, String dataDir) {
        System.out.println("\n\033[1;34m" + "=".repeat(80) + "\n" + centerText("PAIKKA IMPORT STARTING") + "\n" + "=".repeat(80) + "\033[0m\n");
        System.out.println("PBF File: " + pbfFilePath);
        System.out.println("Data Dir: " + dataDir);
        System.out.println("Max Import Threads: " + config.getMaxImportThreads());
        long maxHeapBytes = Runtime.getRuntime().maxMemory();
        String maxHeapSize = (maxHeapBytes == Long.MAX_VALUE) ? "unlimited" : (maxHeapBytes / (1024 * 1024 * 1024)) + "GB";
        System.out.println("Max Heap: " + maxHeapSize);
        System.out.println("File window size: " + (this.fileReadWindowSize / (1024 * 1024)) + "MB");
    }

    private void printPhaseHeader(String phase) {
        System.out.println("\n\033[1;36m" + "─".repeat(80) + "\n" + phase + "\n" + "─".repeat(80) + "\033[0m");
    }

    private void printSuccess() {
        System.out.println("\n\033[1;32m" + "=".repeat(80) + "\n" + centerText("IMPORT COMPLETED SUCCESSFULLY") + "\n" + "=".repeat(80) + "\033[0m");
    }

    private void printError(String message) {
        System.out.println("\n\033[1;31m" + "=".repeat(80) + "\n" + centerText(message) + "\n" + "=".repeat(80) + "\033[0m");
    }

    private void printPhaseSummary(String phaseName, long phaseStartTime, ImportStatistics stats) {
        long phaseTime = System.currentTimeMillis() - phaseStartTime;
        System.out.println(String.format("\n\033[1;32m✓ %s COMPLETED\033[0m \033[2m(%s)\033[0m", phaseName, formatTime(phaseTime)));
    }

    private void printSubPhaseSummary(String subPhaseName, long subPhaseStartTime, ImportStatistics stats) {
        long subPhaseTime = System.currentTimeMillis() - subPhaseStartTime;
        double seconds = subPhaseTime / 1000.0;

        System.out.println();
        if (subPhaseName.contains("Scan")) {
            long entitiesPerSec = seconds > 0 ? (long)(stats.getEntitiesRead() / seconds) : 0;
            System.out.printf("\033[1;32m✓ %s\033[0m \033[2m(%s)\033[0m │ \033[32mEntities:\033[0m %s \033[33m(%s/s)\033[0m │ \033[34mWays:\033[0m %s │ \033[37mNodes:\033[0m %s │ \033[35mRelations:\033[0m %s%n",
                              subPhaseName, formatTime(subPhaseTime),
                              formatCompactNumber(stats.getEntitiesRead()), formatCompactNumber(entitiesPerSec),
                              formatCompactNumber(stats.getWaysProcessed()), formatCompactNumber(stats.getNodesFound()), formatCompactNumber(stats.getRelationsFound()));
        } else if (subPhaseName.contains("Caching")) {
            long nodesPerSec = seconds > 0 ? (long)(stats.getNodesCached() / seconds) : 0;
            System.out.printf("\033[1;32m✓ %s\033[0m \033[2m(%s)\033[0m │ \033[32mNodes:\033[0m %s \033[33m(%s/s)\033[0m │ \033[36mDB Writes:\033[0m %s%n",
                              subPhaseName, formatTime(subPhaseTime),
                              formatCompactNumber(stats.getNodesCached()), formatCompactNumber(nodesPerSec),
                              formatCompactNumber(stats.getRocksDbWrites()));
        } else if (subPhaseName.contains("Boundary")) {
            long boundsPerSec = seconds > 0 ? (long)(stats.getBoundariesProcessed() / seconds) : 0;
            System.out.printf("\033[1;32m✓ %s\033[0m \033[2m(%s)\033[0m │ \033[32mBoundaries:\033[0m %s \033[33m(%s/s)\033[0m │ \033[36mDB Writes:\033[0m %s%n",
                              subPhaseName, formatTime(subPhaseTime),
                              formatCompactNumber(stats.getBoundariesProcessed()), formatCompactNumber(boundsPerSec),
                              formatCompactNumber(stats.getRocksDbWrites()));
        } else {
            System.out.printf("\033[1;32m✓ %s\033[0m \033[2m(%s)\033[0m%n", subPhaseName, formatTime(subPhaseTime));
        }
    }

    /**
     * Creates an appropriate ExecutorService based on the configured thread limit.
     * Uses virtual threads when no limit is set, platform threads with fixed pool when limited.
     */
    private ExecutorService createExecutorService(int maxThreads) {
        if (maxThreads <= 0) {
            return Executors.newVirtualThreadPerTaskExecutor();
        } else {
            return Executors.newFixedThreadPool(maxThreads);
        }
    }

    /**
     * Clean up existing RocksDB database directory to ensure fresh start.
     * Recursively deletes the directory and all its contents if it exists.
     */
    private void cleanupDatabase(Path dbPath) {
        if (Files.exists(dbPath)) {
            try {
                Files.walk(dbPath)
                        .sorted((a, b) -> b.compareTo(a)) // Delete files before directories
                        .forEach(path -> {
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

    private void printFinalStatistics(ImportStatistics stats) {
        System.out.println("\n\033[1;36m" + "═".repeat(80) + "\n" + centerText("🎯 FINAL IMPORT STATISTICS") + "\n" + "═".repeat(80) + "\033[0m");

        long totalTime = Math.max(1, stats.getTotalTime());
        double totalSeconds = totalTime / 1000.0;

        System.out.printf("\n\033[1;37m⏱️  Total Import Time:\033[0m \033[1;33m%s\033[0m%n%n", formatTime(stats.getTotalTime()));

        System.out.println("\033[1;37m📊 Processing Summary:\033[0m");
        System.out.println("┌─────────────────┬─────────────────┬─────────────────┐");
        System.out.println("│ \033[1mEntity Type\033[0m     │ \033[1mTotal Count\033[0m     │ \033[1mAvg Speed\033[0m       │");
        System.out.println("├─────────────────┼─────────────────┼─────────────────┤");
        System.out.printf("│ \033[32mPBF Entities\033[0m    │ %15s │ %13s/s │%n",
                          formatCompactNumber(stats.getEntitiesRead()),
                          formatCompactNumber((long)(stats.getEntitiesRead() / totalSeconds)));
        System.out.printf("│ \033[37mNodes Found\033[0m     │ %15s │ %13s/s │%n",
                          formatCompactNumber(stats.getNodesFound()),
                          formatCompactNumber((long)(stats.getNodesFound() / totalSeconds)));
        System.out.printf("│ \033[34mNodes Cached\033[0m    │ %15s │ %13s/s │%n",
                          formatCompactNumber(stats.getNodesCached()),
                          formatCompactNumber((long)(stats.getNodesCached() / totalSeconds)));
        System.out.printf("│ \033[35mWays Processed\033[0m  │ %15s │ %13s/s │%n",
                          formatCompactNumber(stats.getWaysProcessed()),
                          formatCompactNumber((long)(stats.getWaysProcessed() / totalSeconds)));
        System.out.printf("│ \033[36mBoundaries\033[0m      │ %15s │ %13s/s │%n",
                          formatCompactNumber(stats.getBoundariesProcessed()),
                          formatCompactNumber((long)(stats.getBoundariesProcessed() / totalSeconds)));
        System.out.printf("│ \033[33mPOIs Created\033[0m    │ %15s │ %13s/s │%n",
                          formatCompactNumber(stats.getPoisProcessed()),
                          formatCompactNumber((long)(stats.getPoisProcessed() / totalSeconds)));
        System.out.println("└─────────────────┴─────────────────┴─────────────────┘");

        long totalObjects = stats.getNodesFound() + stats.getNodesCached() + stats.getWaysProcessed() + stats.getBoundariesProcessed() + stats.getPoisProcessed();
        System.out.printf("%n\033[1;37m🚀 Overall Throughput:\033[0m \033[1;32m%s objects\033[0m processed at \033[1;33m%s objects/sec\033[0m%n",
                          formatCompactNumber(totalObjects),
                          formatCompactNumber((long)(totalObjects / totalSeconds)));

        System.out.printf("\033[1;37m💾 Database Operations:\033[0m \033[1;36m%s writes\033[0m (\033[33m%s writes/sec\033[0m)%n%n",
                          formatCompactNumber(stats.getRocksDbWrites()),
                          formatCompactNumber((long)(stats.getRocksDbWrites() / totalSeconds)));
    }

    private static class ImportStatistics {
        private final AtomicLong entitiesRead = new AtomicLong(0);
        private final AtomicLong nodesCached = new AtomicLong(0);
        private final AtomicLong nodesFound = new AtomicLong(0);
        private final AtomicLong waysProcessed = new AtomicLong(0);
        private final AtomicLong relationsFound = new AtomicLong(0);
        private final AtomicLong boundariesProcessed = new AtomicLong(0);
        private final AtomicLong poisProcessed = new AtomicLong(0);
        private final AtomicLong rocksDbWrites = new AtomicLong(0);
        private final AtomicLong queueSize = new AtomicLong(0);
        private final AtomicLong activeThreads = new AtomicLong(0);
        private volatile String currentPhase = "Initializing";
        private volatile boolean running = true;
        private final long startTime = System.currentTimeMillis();
        private long totalTime;

        public long getEntitiesRead() { return entitiesRead.get(); }
        public void incrementEntitiesRead() { entitiesRead.incrementAndGet(); }
        public long getNodesCached() { return nodesCached.get(); }
        public void incrementNodesCached() { nodesCached.incrementAndGet(); }
        public long getNodesFound() { return nodesFound.get(); }
        public void incrementNodesFound() { nodesFound.incrementAndGet(); }
        public long getWaysProcessed() { return waysProcessed.get(); }
        public void incrementWaysProcessed() { waysProcessed.incrementAndGet(); }
        public long getRelationsFound() { return relationsFound.get(); }
        public void incrementRelationsFound() { relationsFound.incrementAndGet(); }
        public long getBoundariesProcessed() { return boundariesProcessed.get(); }
        public void incrementBoundariesProcessed() { boundariesProcessed.incrementAndGet(); }
        public long getPoisProcessed() { return poisProcessed.get(); }
        public void incrementPoisProcessed() { poisProcessed.incrementAndGet(); }
        public long getRocksDbWrites() { return rocksDbWrites.get(); }
        public void incrementRocksDbWrites() { rocksDbWrites.incrementAndGet(); }
        public int getQueueSize() { return (int) queueSize.get(); }
        public void setQueueSize(int size) { queueSize.set(size); }
        public int getActiveThreads() { return (int) activeThreads.get(); }
        public void incrementActiveThreads() { activeThreads.incrementAndGet(); }
        public void decrementActiveThreads() { activeThreads.decrementAndGet(); }
        public String getCurrentPhase() { return currentPhase; }
        public void setCurrentPhase(String phase) { this.currentPhase = phase; }
        public boolean isRunning() { return running; }
        public void stop() { this.running = false; }
        public long getStartTime() { return startTime; }
        public long getTotalTime() { return totalTime; }
        public void setTotalTime(long t) { this.totalTime = t; }

        public String getMemoryStats() {
            Runtime r = Runtime.getRuntime();
            long used = (r.totalMemory() - r.freeMemory()) / 1024 / 1024 / 1024;
            long max = r.maxMemory() / 1024 / 1024 / 1024;
            return String.format("%dGB/%dGB", used, max);
        }
    }

    private record PoiData(long id, double lat, double lon, String type, String subtype, List<NameData> names,
                           AddressData address, List<HierarchyCache.SimpleHierarchyItem> hierarchy,
                           byte[] boundaryWkb) {
    }

    private record NameData(String lang, String text) {
    }

    private record AddressData(String street, String houseNumber, String postcode, String city, String country) {
    }

    private record BoundaryResult(OsmRelation relation, org.locationtech.jts.geom.Geometry geometry) {
    }

    @FunctionalInterface
    private interface ConsumerWithException<T> {
        void accept(T t) throws Exception;
    }

    /**
     * Generic batched writer for RocksDB using WriteBatch and a count threshold.
     * Thread-safe; increments stats once per flush.
     */
    private static class RocksBatchWriter implements AutoCloseable {
        private final RocksDB db;
        private final WriteOptions writeOptions;
        private final ImportStatistics stats;
        private final int maxOps;
        private final Object lock = new Object();
        private WriteBatch batch = new WriteBatch();
        private int ops = 0;

        RocksBatchWriter(RocksDB db, int maxOps, ImportStatistics stats) {
            this.db = db;
            this.maxOps = Math.max(1, maxOps);
            this.stats = stats;
            this.writeOptions = new WriteOptions();
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

        public void delete(byte[] key) throws RocksDBException {
            synchronized (lock) {
                batch.delete(key);
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
                if (batch != null) {
                    batch.close();
                }
                if (writeOptions != null) {
                    writeOptions.close();
                }
            }
        }
    }

    /**
     * Generic periodic flusher wrapper. Starts a daemon scheduler that runs the provided task.
     */
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

}