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

package com.dedicatedcode.paikka.service;

import com.dedicatedcode.paikka.config.PaikkaConfiguration;
import com.dedicatedcode.paikka.flatbuffers.*;
import com.dedicatedcode.paikka.flatbuffers.Geometry;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

@Service
public class ImportService {

    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
    private final S2Helper s2Helper;
    private final GeometrySimplificationService geometrySimplificationService;
    private final PaikkaConfiguration config;

    private final Map<String, String> tagCache = new ConcurrentHashMap<>(1000);
    private final int fileReadWindowSize;

    private int currentStep = 0;
    private static final int TOTAL_STEPS = 5;
    private final AtomicLong sequence = new AtomicLong(0);
    public ImportService(S2Helper s2Helper, GeometrySimplificationService geometrySimplificationService, PaikkaConfiguration config) {
        this.s2Helper = s2Helper;
        this.geometrySimplificationService = geometrySimplificationService;
        this.config = config;
        this.fileReadWindowSize = calculateFileReadWindowSize();
    }

    private int calculateFileReadWindowSize() {
        long maxHeap = Runtime.getRuntime().maxMemory();
        if (maxHeap > 24L * 1024 * 1024 * 1024) {
            return  256 * 1024 * 1024;
        } else if (maxHeap > 8L * 1024 * 1024 * 1024) {
            return  96 * 1024 * 1024;
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
        Path gridIndexDbPath = dataDirectory.resolve("tmp/grid_index");
        Path appendDbPath = dataDirectory.resolve("tmp/append_poi");
        Path nodeCacheDbPath = dataDirectory.resolve("tmp/node_cache");
        Path wayIndexDbPath = dataDirectory.resolve("tmp/way_index");
        Path neededNodesDbPath = dataDirectory.resolve("tmp/needed_nodes");
        Path relIndexDbPath = dataDirectory.resolve("tmp/rel_index");
        Path poiIndexDbPath = dataDirectory.resolve("tmp/poi_index");

        cleanupDatabase(shardsDbPath);
        cleanupDatabase(boundariesDbPath);
        cleanupDatabase(gridIndexDbPath);
        cleanupDatabase(nodeCacheDbPath);
        cleanupDatabase(wayIndexDbPath);
        cleanupDatabase(neededNodesDbPath);
        cleanupDatabase(relIndexDbPath);
        cleanupDatabase(poiIndexDbPath);
        cleanupDatabase(appendDbPath);

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
                    .setMaxWriteBufferNumber(6)
                    .setMinWriteBufferNumberToMerge(2)
                    .setLevel0FileNumCompactionTrigger(20)
                    .setLevel0SlowdownWritesTrigger(30)
                    .setLevel0StopWritesTrigger(40);
            Options appendOpts = new Options()
                                .setCreateIfMissing(true)
                                .setTableFormatConfig(tableConfig)
                                .setCompressionType(CompressionType.LZ4_COMPRESSION)
                                .setWriteBufferSize(512 * 1024 * 1024)
                                .setMaxWriteBufferNumber(6)
                                .setMinWriteBufferNumberToMerge(2)
                                .setLevel0FileNumCompactionTrigger(20)
                                .setLevel0SlowdownWritesTrigger(30)
                                .setLevel0StopWritesTrigger(40);

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
                    .setMaxWriteBufferNumber(6)
                    .setMinWriteBufferNumberToMerge(2)
                    .setLevel0FileNumCompactionTrigger(20)
                    .setLevel0SlowdownWritesTrigger(30)
                    .setLevel0StopWritesTrigger(40);

            Options neededNodesOpts = new Options()
                    .setCreateIfMissing(true)
                    .setTableFormatConfig(tableConfig)
                    .setCompressionType(CompressionType.LZ4_COMPRESSION)
                    .setWriteBufferSize(512 * 1024 * 1024)
                    .setMaxWriteBufferNumber(6)
                    .setMinWriteBufferNumberToMerge(2)
                    .setLevel0FileNumCompactionTrigger(20)
                    .setLevel0SlowdownWritesTrigger(30)
                    .setLevel0StopWritesTrigger(40);

            try (RocksDB shardsDb = RocksDB.open(persistentOpts, shardsDbPath.toString());
                 RocksDB boundariesDb = RocksDB.open(persistentOpts, boundariesDbPath.toString());
                 RocksDB gridIndexDb = RocksDB.open(gridOpts, gridIndexDbPath.toString());
                 RocksDB nodeCache = RocksDB.open(nodeOpts, nodeCacheDbPath.toString());
                 RocksDB wayIndexDb = RocksDB.open(wayIndexOpts, wayIndexDbPath.toString());
                 RocksDB neededNodesDb = RocksDB.open(neededNodesOpts, neededNodesDbPath.toString());
                 RocksDB relIndexDb = RocksDB.open(wayIndexOpts, relIndexDbPath.toString());
                 RocksDB poiIndexDb = RocksDB.open(poiIndexOpts, poiIndexDbPath.toString());
                 RocksDB appendDb = RocksDB.open(appendOpts, appendDbPath.toString())) {

                // PASS 1: Discovery & Indexing
                currentStep = 1;
                printPhaseHeader("PASS 1: Discovery & Indexing");
                long pass1Start = System.currentTimeMillis();
                stats.setCurrentPhase("1.1.1: Discovery & Indexing");
                pass1DiscoveryAndIndexing(pbfFile, wayIndexDb, neededNodesDb, relIndexDb, poiIndexDb, stats);
                printPhaseSummary("PASS 1", pass1Start);

                // PASS 2: Nodes Cache, Boundaries, POIs
                currentStep = 2;
                printPhaseHeader("PASS 2: Nodes Cache, Boundaries, POIs");
                long pass2Start = System.currentTimeMillis();
                stats.setCurrentPhase("1.1.2: Caching node coordinates");
                cacheNeededNodeCoordinates(pbfFile, neededNodesDb, nodeCache, stats);

                currentStep = 3;
                stats.setCurrentPhase("1.2: Processing administrative boundaries");
                processAdministrativeBoundariesFromIndex(relIndexDb, nodeCache, wayIndexDb, gridIndexDb, boundariesDb, stats);
                currentStep = 4;
                stats.setCurrentPhase("2.1: Processing POIs & Sharding");
                pass2PoiShardingFromIndex(nodeCache, wayIndexDb, appendDb, boundariesDb, poiIndexDb, gridIndexDb, stats);
                currentStep = 5;

                stats.setCurrentPhase("2.2: Compacting POIs");
                compactShards(appendDb, shardsDb, stats);
                stats.stop();
                printPhaseSummary("PASS 2", pass2Start);

                shardsDb.compactRange();
                boundariesDb.compactRange();


                stats.setTotalTime(System.currentTimeMillis() - totalStartTime);


                recordSizeMetrics(stats,
                                  shardsDbPath,
                                  appendDbPath,
                                  boundariesDbPath,
                                  gridIndexDbPath,
                                  nodeCacheDbPath,
                                  wayIndexDbPath,
                                  neededNodesDbPath,
                                  relIndexDbPath,
                                  poiIndexDbPath);

                shardsDb.flush(new FlushOptions().setWaitForFlush(true));
                boundariesDb.flush(new FlushOptions().setWaitForFlush(true));
                printFinalStatistics(stats);
                printSuccess();
                writeMetadataFile(pbfFile, dataDirectory);

            } catch (Exception e) {
                stats.stop();
                printError("IMPORT FAILED: " + e.getMessage());
                e.printStackTrace();
                throw e;
            }
            try {
                cleanupDatabase(tmpDirectory);
                System.out.println("\n\033[1;90mTemporary databases deleted.\033[0m");
            } catch (Exception e) {
                System.err.println("Warning: Failed to delete temporary databases: " + e.getMessage());
            }
        }
    }

    private void writeMetadataFile(Path pbfFile, Path dataDirectory) throws IOException {
        Path metadataPath = dataDirectory.resolve("paikka_metadata.json");
        Instant now = Instant.now();
        String importTimestamp = DateTimeFormatter.ISO_INSTANT.format(now);
        String dataVersion = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss").withZone(ZoneOffset.UTC).format(now);
        ObjectMapper objectMapper = new ObjectMapper();
        PaikkaMetadata metadata = new PaikkaMetadata(
                importTimestamp,
                dataVersion,
                pbfFile.getFileName().toString(),
                S2Helper.GRID_LEVEL,
                "1.0.0"
        );

        objectMapper.writeValue(metadataPath.toFile(), metadata);
        System.out.println("\n\033[1;32mMetadata file written to: " + metadataPath + "\033[0m");
    }

    private void startProgressReporter(ImportStatistics stats) {
        boolean isTty = System.console() != null;

        Thread.ofPlatform().daemon().start(() -> {
            while (stats.isRunning()) {
                long elapsed = System.currentTimeMillis() - stats.getStartTime();
                long phaseElapsed = System.currentTimeMillis() - stats.getPhaseStartTime();
                double phaseSeconds = phaseElapsed / 1000.0;

                String phase = stats.getCurrentPhase();
                StringBuilder sb = new StringBuilder();

                if (isTty) {
                    sb.append("\r\033[K");
                }

                // Add step indicator
                sb.append(String.format("\033[1;90m[%d/%d]\033[0m ", currentStep, TOTAL_STEPS));

                if (phase.contains("1.1.1")) {
                    long pbfPerSec = phaseSeconds > 0 ? (long)(stats.getEntitiesRead() / phaseSeconds) : 0;
                    sb.append(String.format("\033[1;36m[%s]\033[0m \033[1mScanning PBF Structure\033[0m", formatTime(elapsed)));
                    sb.append(String.format(" │ \033[32mPBF Entities:\033[0m %s \033[33m(%s/s)\033[0m",
                                            formatCompactNumber(stats.getEntitiesRead()), formatCompactRate(pbfPerSec)));
                    sb.append(String.format(" │ \033[37mNodes Found:\033[0m %s", formatCompactNumber(stats.getNodesFound())));
                    sb.append(String.format(" │ \033[34mWays Found:\033[0m %s", formatCompactNumber(stats.getWaysProcessed())));
                    sb.append(String.format(" │ \033[35mRelations:\033[0m %s", formatCompactNumber(stats.getRelationsFound())));

                } else if (phase.contains("1.1.2")) {
                    long nodesPerSec = phaseSeconds > 0 ? (long)(stats.getNodesCached() / phaseSeconds) : 0;
                    sb.append(String.format("\033[1;36m[%s]\033[0m \033[1mCaching Node Coordinates\033[0m", formatTime(elapsed)));
                    sb.append(String.format(" │ \033[32mNodes Cached:\033[0m %s \033[33m(%s/s)\033[0m",
                                            formatCompactNumber(stats.getNodesCached()), formatCompactRate(nodesPerSec)));
                    sb.append(String.format(" │ \033[36mQueue:\033[0m %s", formatCompactNumber(stats.getQueueSize())));
                    sb.append(String.format(" │ \033[37mThreads:\033[0m %d", stats.getActiveThreads()));

                } else if (phase.contains("1.2")) {
                    long boundsPerSec = phaseSeconds > 0 ? (long)(stats.getBoundariesProcessed() / phaseSeconds) : 0;
                    sb.append(String.format("\033[1;36m[%s]\033[0m \033[1mProcessing Admin Boundaries\033[0m", formatTime(elapsed)));
                    sb.append(String.format(" │ \033[32mBoundaries:\033[0m %s \033[33m(%s/s)\033[0m",
                                            formatCompactNumber(stats.getBoundariesProcessed()), formatCompactRate(boundsPerSec)));
                    sb.append(String.format(" │ \033[37mThreads:\033[0m %d", stats.getActiveThreads()));

                } else if (phase.contains("2.1")) {
                    long poisPerSec = phaseSeconds > 0 ? (long)(stats.getPoisProcessed() / phaseSeconds) : 0;
                    long poisReadSec = phaseSeconds > 0 ? (long)(stats.getPoiIndexRecRead() / phaseSeconds) : 0;
                    sb.append(String.format("\033[1;36m[%s]\033[0m \033[1mProcessing POIs & Sharding\033[0m", formatTime(elapsed)));
                    sb.append(String.format(" │ \033[32mPOI Index Rec Read:\033[0m %s \033[33m%s\033[0m", formatCompactNumber(stats.getPoiIndexRecRead()), stats.isPoiIndexRecReadDone() ? "(done)" : String.format("(%s/s)",formatCompactRate(poisReadSec))));
                    sb.append(String.format(" │ \033[32mPOIs Processed:\033[0m %s \033[33m(%s/s)\033[0m", formatCompactNumber(stats.getPoisProcessed()), formatCompactRate(poisPerSec)));
                    sb.append(String.format(" │ \033[36mQueue:\033[0m %s", formatCompactNumber(stats.getQueueSize())));
                    sb.append(String.format(" │ \033[37mThreads:\033[0m %d", stats.getActiveThreads()));

                } else if (phase.contains("2.2")) {
                    sb.append(String.format("\033[1;36m[%s]\033[0m \033[1mProcessing POIs & Sharding\033[0m", formatTime(elapsed)));

                } else {
                    sb.append(String.format("\033[1;36m[%s]\033[0m %s", formatTime(elapsed), phase));
                }

                sb.append(String.format(" │ \033[31mHeap:\033[0m %s", stats.getMemoryStats()));

                if (isTty) {
                    System.out.print(sb);
                    System.out.flush();
                } else {
                    System.out.println(sb);
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

    private void pass1DiscoveryAndIndexing(Path pbfFile,
                                           RocksDB wayIndexDb,
                                           RocksDB neededNodesDb,
                                           RocksDB relIndexDb,
                                           RocksDB poiIndexDb,
                                           ImportStatistics stats) throws Exception {

        final byte[] ONE = new byte[]{1};
        try (RocksBatchWriter wayWriter = new RocksBatchWriter(wayIndexDb, 10_000, stats);
             RocksBatchWriter neededWriter = new RocksBatchWriter(neededNodesDb, 500_000, stats);
             RocksBatchWriter relWriter = new RocksBatchWriter(relIndexDb, 2_000, stats);
             RocksBatchWriter poiWriter = new RocksBatchWriter(poiIndexDb, 20_000, stats)) {

            withPbfIterator(pbfFile, iterator -> {
                while (iterator.hasNext()) {
                    EntityContainer container = iterator.next();
                    stats.incrementEntitiesRead();
                    EntityType type = container.getType();

                    if (type == EntityType.Node) {
                        OsmNode node = (OsmNode) container.getEntity();
                        if (isPoi(node)) {
                            PoiIndexRec rec = buildPoiIndexRecFromEntity(node);
                            rec.lat = node.getLatitude();
                            rec.lon = node.getLongitude();
                            byte[] key = buildPoiKey((byte) 'N', node.getId());
                            poiWriter.put(key, encodePoiIndexRec(rec));
                            neededWriter.put(s2Helper.longToByteArray(node.getId()), ONE);
                            stats.incrementNodesFound();
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
                            wayWriter.put(s2Helper.longToByteArray(way.getId()), s2Helper.longArrayToByteArray(nodeIds));
                            if (isPoi) {
                                PoiIndexRec rec = buildPoiIndexRecFromEntity(way);
                                // lat/lon remain NaN for ways — resolved in Pass 2 reader
                                byte[] key = buildPoiKey((byte) 'W', way.getId());
                                poiWriter.put(key, encodePoiIndexRec(rec));
                            }
                        }

                    } else if (type == EntityType.Relation) {
                        OsmRelation relation = (OsmRelation) container.getEntity();
                        if (isAdministrativeBoundary(relation)) {
                            stats.incrementRelationsFound();
                            RelRec rec = buildRelRec(relation);
                            relWriter.put(s2Helper.longToByteArray(relation.getId()), encodeRelRec(rec));
                        }
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
        stats.setCurrentPhase("2.1: Processing POIs & Sharding");
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
                    writeShardBatchAppendOnly(bufferToFlush, appendDb, stats);
                }
            } catch (Exception _) {
            }
        };

        try (PeriodicFlusher _ = PeriodicFlusher.start("shard-buffer-flush", 5, 5, flushTask)) {
            BlockingQueue<List<PoiQueueItem>> queue = new LinkedBlockingQueue<>(200);
            int numReaders = Math.max(1, config.getImportConfiguration().getThreads());
            int chunkSize = config.getImportConfiguration().getChunkSize();
            int localPoiBufferSize = 1000;

            CountDownLatch latch = new CountDownLatch(numReaders);

            try (ExecutorService executor = createExecutorService(numReaders);
                 ReadOptions ro = new ReadOptions().setReadaheadSize(2 * 1024 * 1024)) {
                com.github.benmanes.caffeine.cache.Cache<Long, HierarchyCache.CachedBoundary> globalBoundaryCache = Caffeine.newBuilder()
                        .maximumSize(1000)
                        .recordStats()
                        .build();
                ThreadLocal<HierarchyCache> hierarchyCacheThreadLocal = ThreadLocal.withInitial(
                        () -> new HierarchyCache(boundariesDb, gridIndexDb, s2Helper, globalBoundaryCache)
                );
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

                                // For way POIs, lat/lon is NaN — resolve from nodeCache/wayIndexDb
                                byte[] cacheWayNodes = null;
                                if (Double.isNaN(rec.lat) && kind == 'W') {
                                    cacheWayNodes = resolveWayCenter(rec, nodeCache, wayIndexDb);
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

                        } catch (Exception ignored) {
                        }
                    });
                    readerThreads.add(readerThread);

                }

                // ─── Worker threads: hierarchy resolution + sharding (unchanged) ───

                for (int i = 0; i < numReaders; i++) {
                    executor.submit(() -> {
                        stats.incrementActiveThreads();
                        final GeometryFactory geometryFactory = new GeometryFactory();
                        final HierarchyCache hierarchyCache = hierarchyCacheThreadLocal.get();
                        final Map<Long, List<PoiData>> localShardBuffer = new HashMap<>();
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

                                        // For way POIs, still compute boundary WKB geometry
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
                                    } catch (Exception ignored) {
                                    }
                                }
                                stats.incrementPoisProcessed(localCount);
                                if (localShardBuffer.size() > localPoiBufferSize) {
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

    /**
     * Sorts a chunk of POI items by S2CellId (Hilbert curve order) for spatial
     * locality, then emits to the processing queue in batches.
     * Memory stays bounded: only one chunk is in memory at a time.
     */
    private void sortAndEmitChunk(List<PoiQueueItem> chunk,
                                  BlockingQueue<List<PoiQueueItem>> queue,
                                  ImportStatistics stats) throws InterruptedException {
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

    private byte[] resolveWayCenter(PoiIndexRec rec, RocksDB nodeCache, RocksDB wayIndexDb) {
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
        } catch (Exception ignored) {
        }
        return null;
    }

    private void cacheNeededNodeCoordinates(Path pbfFile, RocksDB neededNodesDb, RocksDB nodeCache, ImportStatistics stats) throws Exception {
        final int BATCH_SIZE = 50_000;

        // The reader thread produces batches of nodes
        BlockingQueue<List<OsmNode>> nodeBatchQueue = new LinkedBlockingQueue<>(200);
        int numProcessors = Math.max(1, config.getImportConfiguration().getThreads());
        CountDownLatch latch = new CountDownLatch(numProcessors);

        try (ExecutorService executor = createExecutorService(numProcessors)) {
            // The reader logic is good, no changes needed here.
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
                    // Log the exception
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
                    // --- OPTIMIZATION 1: ThreadLocal for reusable objects to reduce GC pressure ---
                    final ThreadLocal<RocksBatchWriter> nodeWriterLocal =
                            ThreadLocal.withInitial(() -> new RocksBatchWriter(nodeCache, 50_000, stats));
                    // Reuse a ByteBuffer for writing values
                    final ThreadLocal<ByteBuffer> valueBufferLocal = ThreadLocal.withInitial(() -> ByteBuffer.allocate(16));
                    // Reuse a List of byte[] for multiGet keys. We only need one per thread.
                    final ThreadLocal<List<byte[]>> keysListLocal = ThreadLocal.withInitial(ArrayList::new);

                    try {
                        while (true) {
                            List<OsmNode> nodes = nodeBatchQueue.take();
                            stats.setQueueSize(nodeBatchQueue.size());
                            if (nodes.isEmpty()) break;

                            // Reuse the keys list
                            List<byte[]> keys = keysListLocal.get();
                            keys.clear(); // Clear previous batch's keys
                            for (OsmNode n : nodes) {
                                keys.add(s2Helper.longToByteArray(n.getId()));
                            }

                            List<byte[]> presence = neededNodesDb.multiGetAsList(keys);

                            RocksBatchWriter nodeWriter = nodeWriterLocal.get();
                            ByteBuffer valueBuffer = valueBufferLocal.get(); // Get the reusable buffer

                            // --- OPTIMIZATION 2: Combine loops ---
                            for (int idx = 0; idx < nodes.size(); idx++) {
                                if (presence.get(idx) != null) {
                                    OsmNode n = nodes.get(idx);

                                    // Reset buffer position and write new data
                                    valueBuffer.clear();
                                    valueBuffer.putDouble(n.getLatitude());
                                    valueBuffer.putDouble(n.getLongitude());

                                    // The key is already in the 'keys' list at the same index
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
                        } catch (Exception ignore) { }
                        nodeWriterLocal.remove();
                        valueBufferLocal.remove(); // Clean up thread-local
                        keysListLocal.remove();    // Clean up thread-local
                        stats.decrementActiveThreads();
                        latch.countDown();
                    }
                });
            }

            latch.await();
            reader.join();
        }
    }

    private void processAdministrativeBoundariesFromIndex(RocksDB relIndexDb,
                                                          RocksDB nodeCache,
                                                          RocksDB wayIndexDb,
                                                          RocksDB gridsIndexDb,
                                                          RocksDB boundariesDb,
                                                          ImportStatistics stats) throws Exception {
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
                            org.locationtech.jts.geom.Geometry geometry = buildGeometryFromRelRec(rec, nodeCache, wayIndexDb);
                            if (geometry != null && geometry.isValid()) {
                                org.locationtech.jts.geom.Geometry simplified = geometrySimplificationService.simplifyByAdminLevel(geometry, rec.level);
                                return new BoundaryResultLite(rec.osmId, rec.level, rec.name, simplified);
                            }
                            return null;
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
                                storeBoundary(r.osmId(), r.level(), r.name(), r.geometry(), boundariesWriter, gridsIndexDb);
                                stats.incrementBoundariesProcessed();
                            }
                        } catch (Exception e) {
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
                        storeBoundary(r.osmId(), r.level(), r.name(), r.geometry(), boundariesWriter, gridsIndexDb);
                        stats.incrementBoundariesProcessed();
                    }
                } catch (Exception e) {
                }
            }
            boundariesWriter.flush();

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
                    if (country == null && item.level() == 2) country = item.name();
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

    private void writeShardBatchAppendOnly(Map<Long, List<PoiData>> shardBuffer,
                                           RocksDB appendDb,
                                           ImportStatistics stats) throws Exception {
        FlatBufferBuilder builder = new FlatBufferBuilder(1024 * 32);

        try (WriteBatch batch = new WriteBatch();
             WriteOptions writeOptions = new WriteOptions()) {

            for (Iterator<Map.Entry<Long, List<PoiData>>> it = shardBuffer.entrySet().iterator();
                 it.hasNext(); ) {
                Map.Entry<Long, List<PoiData>> entry = it.next();
                List<PoiData> pois = entry.getValue();
                if (pois.isEmpty()) { it.remove(); continue; }

                builder.clear();

                // Serialize ONLY the new POIs (no reading existing!)
                int[] poiOffsets = new int[pois.size()];
                for (int i = 0; i < pois.size(); i++) {
                    poiOffsets[i] = serializePoiData(builder, pois.get(i));
                }
                int poisVectorOffset = POIList.createPoisVector(builder, poiOffsets);
                int poiListOffset = POIList.createPOIList(builder, poisVectorOffset);
                builder.finish(poiListOffset);

                // Key = shardId (8 bytes) + sequence (8 bytes) — unique, no collision
                long seq = sequence.incrementAndGet();
                byte[] key = new byte[16];
                ByteBuffer.wrap(key).putLong(entry.getKey()).putLong(seq);

                batch.put(key, builder.sizedByteArray());
                it.remove();
            }

            appendDb.write(writeOptions, batch);
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
            nameOffs[j] = Name.createName(builder,
                                          builder.createString(n.lang()),
                                          builder.createString(n.text()));
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
            addressOff = com.dedicatedcode.paikka.flatbuffers.Address.createAddress(
                    builder, streetOff, houseNumberOff, postcodeOff, cityOff, countryOff);
        }

        List<HierarchyCache.SimpleHierarchyItem> hierarchy = poi.hierarchy();
        if (poi.id == 432751852L) {
            System.out.println("Hierarchy in append: " + hierarchy);
        }
        int[] hierOffs = new int[hierarchy.size()];
        for (int j = 0; j < hierarchy.size(); j++) {
            HierarchyCache.SimpleHierarchyItem h = hierarchy.get(j);
            hierOffs[j] = com.dedicatedcode.paikka.flatbuffers.HierarchyItem.createHierarchyItem(
                    builder, h.level(),
                    builder.createString(h.type()),
                    builder.createString(h.name()),
                    h.osmId());
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


    private void compactShards(RocksDB appendDb, RocksDB shardsDb, ImportStatistics stats) throws Exception {

        // Reusable FlatBuffer accessor objects
        POI reusablePoi = new POI();
        Name reusableName = new Name();
        HierarchyItem reusableHier = new HierarchyItem();
        Address reusableAddr = new Address();
        Geometry reusableGeom = new Geometry();

        try (RocksIterator iterator = appendDb.newIterator();
             WriteOptions writeOptions = new WriteOptions()) {

            iterator.seekToFirst();

            long currentShardId = Long.MIN_VALUE;
            // Collect raw byte[] chunks per shard, build FlatBuffer only at flush time
            List<byte[]> currentShardChunks = new ArrayList<>();
            long shardsCompacted = 0;

            while (iterator.isValid()) {
                byte[] key = iterator.key();
                long shardId = ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN).getLong();

                // Shard boundary — flush previous shard
                if (shardId != currentShardId && currentShardId != Long.MIN_VALUE) {
                    flushCompactedShard(currentShardChunks, currentShardId, shardsDb,
                                        writeOptions, reusablePoi, reusableName, reusableHier,
                                        reusableAddr, reusableGeom);
                    currentShardChunks.clear();
                    shardsCompacted++;

                    if (shardsCompacted % 10000 == 0) {
                        System.out.println("Compacted " + shardsCompacted + " shards...");
                    }
                }

                currentShardId = shardId;

                // IMPORTANT: copy the value bytes — RocksIterator may reuse the buffer
                byte[] value = iterator.value();
                byte[] valueCopy = new byte[value.length];
                System.arraycopy(value, 0, valueCopy, 0, value.length);
                currentShardChunks.add(valueCopy);

                iterator.next();
            }

            // Flush last shard
            if (currentShardId != Long.MIN_VALUE && !currentShardChunks.isEmpty()) {
                flushCompactedShard(currentShardChunks, currentShardId, shardsDb,
                                    writeOptions, reusablePoi, reusableName, reusableHier,
                                    reusableAddr, reusableGeom);
                shardsCompacted++;
            }

            System.out.println("Compaction complete: " + shardsCompacted + " shards written.");
        }
    }

    /**
     * Takes all raw FlatBuffer chunks for a single shard, reads each chunk's POIs,
     * copies them into a fresh FlatBufferBuilder, and writes the merged result.
     */
    private void flushCompactedShard(List<byte[]> chunks, long shardId,
                                     RocksDB shardsDb, WriteOptions writeOptions,
                                     POI reusablePoi, Name reusableName,
                                     HierarchyItem reusableHier, Address reusableAddr,
                                     Geometry reusableGeom) throws Exception {

        // Count total POIs first
        int totalPois = 0;
        for (byte[] chunk : chunks) {
            ByteBuffer buf = ByteBuffer.wrap(chunk);
            POIList poiList = POIList.getRootAsPOIList(buf);
            totalPois += poiList.poisLength();
        }

        // Fresh builder per shard — no stale offsets
        FlatBufferBuilder builder = new FlatBufferBuilder(Math.max(1024, totalPois * 256));
        int[] allOffsets = new int[totalPois];
        int idx = 0;

        // Process each chunk: the source ByteBuffer stays alive while we read from it
        for (byte[] chunk : chunks) {
            ByteBuffer buf = ByteBuffer.wrap(chunk);
            POIList poiList = POIList.getRootAsPOIList(buf);
            int count = poiList.poisLength();

            for (int i = 0; i < count; i++) {
                poiList.pois(reusablePoi, i);
                allOffsets[idx++] = copyPoiFromFlatBuffer(builder, reusablePoi,
                                                          reusableName, reusableHier, reusableAddr, reusableGeom);
            }
        }

        int poisVec = POIList.createPoisVector(builder, allOffsets);
        int poiList = POIList.createPOIList(builder, poisVec);
        builder.finish(poiList);
        shardsDb.put(writeOptions, s2Helper.longToByteArray(shardId), builder.sizedByteArray());
    }

    private int copyPoiFromFlatBuffer(FlatBufferBuilder builder, POI poi,
                                      Name reusableName, HierarchyItem reusableHier,
                                      Address reusableAddr, Geometry reusableGeom) {

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

            addressOff = com.dedicatedcode.paikka.flatbuffers.Address.createAddress(
                    builder, streetOff, houseNumberOff, postcodeOff, cityOff, countryOff);
        }

        int hierLen = poi.hierarchyLength();
        if (poi.id() == 432751852L) {
            System.out.println("Hierarchy in after: " + poi.hierarchyLength());
        }
        int hierVecOff;
        if (hierLen > 0) {
            int[] hierOffs = new int[hierLen];
            for (int j = 0; j < hierLen; j++) {
                poi.hierarchy(reusableHier, j);
                String hType = reusableHier.type();
                String hName = reusableHier.name();
                int hTypeOff = hType != null ? builder.createString(hType) : 0;
                int hNameOff = hName != null ? builder.createString(hName) : 0;
                hierOffs[j] = com.dedicatedcode.paikka.flatbuffers.HierarchyItem.createHierarchyItem(
                        builder, reusableHier.level(), hTypeOff, hNameOff, reusableHier.osmId());
            }
            hierVecOff = POI.createHierarchyVector(builder, hierOffs);
        } else {
            hierVecOff = POI.createHierarchyVector(builder, new int[0]);
        }

        // Boundary geometry — use ByteBuffer slice for zero-copy transfer
        int boundaryOff = 0;
        if (poi.boundary(reusableGeom) != null && reusableGeom.dataLength() > 0) {
            ByteBuffer boundaryBuf = reusableGeom.dataAsByteBuffer();
            if (boundaryBuf != null) {
                int boundaryDataOff = Geometry.createDataVector(builder, boundaryBuf);
                boundaryOff = Geometry.createGeometry(builder, boundaryDataOff);
            } else {
                // Fallback: manual copy if dataAsByteBuffer() returns null
                int len = reusableGeom.dataLength();
                byte[] boundaryBytes = new byte[len];
                for (int k = 0; k < len; k++) {
                    boundaryBytes[k] = (byte) reusableGeom.data(k);
                }
                int boundaryDataOff = Geometry.createDataVector(builder, boundaryBytes);
                boundaryOff = Geometry.createGeometry(builder, boundaryDataOff);
            }
        }

        // --- Now build the POI table ---
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

    private org.locationtech.jts.geom.Geometry buildGeometryFromRelRec(RelRec rec, RocksDB nodeCache, RocksDB wayIndexDb) {
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
                    } catch (Exception e) { }
                Polygon polygon = GEOMETRY_FACTORY.createPolygon(shell, holes.toArray(new LinearRing[0]));
                if (polygon.isValid()) validPolygons.add(polygon);
            } catch (Exception e) { }
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
            case "amenity", "shop", "tourism", "leisure", "natural", "office", "craft", "healthcare", "emergency", "historic", "man_made", "place", "sport", "public_transport", "railway", "aeroway", "building" -> true;
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
                        case "fire_hydrant", "defibrillator", "fire_extinguisher", "siren", "life_ring", "lifeline",
                             "phone", "drinking_water" -> false;
                        default -> true;
                    };
                case "building":
                    if (switch (val) {
                        case "yes", "commercial", "retail", "industrial", "office", "apartments" -> true;
                        default -> false;
                    }) {
                        isInterestingBuilding = true;
                    }
                    break;

                case "shop", "tourism", "leisure", "office", "craft", "place",
                     "historic", "public_transport", "aeroway":
                    // Exclude the specific sub-leisure types you mentioned earlier
                    return !key.equals("leisure") || !List.of("picnic_table", "swimming_pool").contains(val);

                case "railway":
                    if ("station".equals(val)) return true;
                    break;

                default:
                    // If it's any other key in your fast-key list (like 'natural'), allow it
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
            if (ring.size() >= 3 && !ring.getFirst().equals2D(ring.getLast())) ring.add(new Coordinate(ring.getFirst()));
            if (ring.size() >= 4) rings.add(ring);
        }
        return rings;
    }

    private void storeBoundary(long osmId,
                               int level,
                               String name,
                               org.locationtech.jts.geom.Geometry geometry,
                               RocksBatchWriter boundariesWriter,
                               RocksDB gridsIndexDb) throws Exception {
        FlatBufferBuilder fbb = new FlatBufferBuilder(1024);
        byte[] wkb = new WKBWriter().write(geometry);
        int geomDataOffset = Geometry.createDataVector(fbb, wkb);
        int geomOffset = Geometry.createGeometry(fbb, geomDataOffset);
        Envelope mbr = geometry.getEnvelopeInternal();
        MaximumInscribedCircle mic = new MaximumInscribedCircle(geometry, 0.00001);
        double radius = mic.getRadiusLine().getLength();
        Coordinate center = mic.getCenter().getCoordinate();
        double offset = radius / Math.sqrt(2);
        int nameOffset = fbb.createString(name != null ? name : "Unknown");
        Boundary.startBoundary(fbb);
        Boundary.addOsmId(fbb, osmId);
        Boundary.addLevel(fbb, level);
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

        S2LatLng low = S2LatLng.fromDegrees(mbr.getMinY(), mbr.getMinX());
        S2LatLng high = S2LatLng.fromDegrees(mbr.getMaxY(), mbr.getMaxX());
        S2LatLngRect rect = S2LatLngRect.fromPointPair(low, high);
        S2RegionCoverer coverer = S2RegionCoverer.builder().setMinLevel(S2Helper.GRID_LEVEL).setMaxLevel(S2Helper.GRID_LEVEL).build();
        ArrayList<S2CellId> covering = new ArrayList<>();
        coverer.getCovering(rect, covering);
        for (S2CellId cellId : covering) updateGridIndexEntry(gridsIndexDb, cellId.id(), osmId);

        boundariesWriter.put(s2Helper.longToByteArray(osmId), fbb.sizedByteArray());
    }

    private void withPbfIterator(Path pbfFile, ConsumerWithException<OsmIterator> consumer) throws Exception {
        try (RandomAccessFile file = new RandomAccessFile(pbfFile.toFile(), "r");
             FileChannel channel = file.getChannel()) {

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

    private String formatTime(long ms) {
        long s = ms / 1000;
        return String.format("%d:%02d:%02d", s / 3600, (s % 3600) / 60, s % 60);
    }

    private String formatCompactNumber(long n) {
        if (n < 1000) return String.valueOf(n);
        if (n < 1_000_000) return String.format("%.2fk", n / 1000.0);
        return String.format("%.3fM", n / 1_000_000.0);
    }
    private String formatCompactRate(long n) {
        if (n < 1000) return String.valueOf(n);
        if (n < 1_000_000) return String.format("%.1fk", n / 1000.0);
        return String.format("%.1fM", n / 1_000_000.0);
    }

    private String formatSize(long bytes) {
        if (bytes < 1024) return bytes + " B";
        double kb = bytes / 1024.0;
        if (kb < 1024) return String.format("%.1f KB", kb);
        double mb = kb / 1024.0;
        if (mb < 1024) return String.format("%.1f MB", mb);
        double gb = mb / 1024.0;
        if (gb < 1024) return String.format("%.2f GB", gb);
        double tb = gb / 1024.0;
        return String.format("%.2f TB", tb);
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

    private void printPhaseHeader(String phase) {
        System.out.println("\n\033[1;36m" + "─".repeat(80) + "\n" + phase + "\n" + "─".repeat(80) + "\033[0m");
    }

    private void printSuccess() {
        System.out.println("\n\033[1;32m" + "=".repeat(80) + "\n" + centerText("IMPORT COMPLETED SUCCESSFULLY") + "\n" + "=".repeat(80) + "\033[0m");
    }

    private void printError(String message) {
        System.out.println("\n\033[1;31m" + "=".repeat(80) + "\n" + centerText(message) + "\n" + "=".repeat(80) + "\033[0m");
    }

    private void printPhaseSummary(String phaseName, long phaseStartTime) {
        long phaseTime = System.currentTimeMillis() - phaseStartTime;
        System.out.println(String.format("\n\033[1;32m✓ %s COMPLETED\033[0m \033[2m(%s)\033[0m", phaseName, formatTime(phaseTime)));
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
                Files.walk(dbPath)
                        .sorted((a, b) -> b.compareTo(a))
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

    private long computeDirectorySize(Path root) {
        if (root == null || !Files.exists(root)) return 0L;
        try (Stream<Path> s = Files.walk(root)) {
            return s.filter(p -> {
                        try {
                            return Files.isRegularFile(p);
                        } catch (Exception e) {
                            return false;
                        }
                    })
                    .mapToLong(p -> {
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
                                   Path gridIndexDbPath,
                                   Path nodeCacheDbPath,
                                   Path wayIndexDbPath,
                                   Path neededNodesDbPath,
                                   Path relIndexDbPath,
                                   Path poiIndexDbPath,
                                   Path appendDbPath) {
        long shards = computeDirectorySize(shardsDbPath);
        long boundaries = computeDirectorySize(boundariesDbPath);
        long dataset = shards + boundaries;

        long grid = computeDirectorySize(gridIndexDbPath);
        long node = computeDirectorySize(nodeCacheDbPath);
        long way = computeDirectorySize(wayIndexDbPath);
        long needed = computeDirectorySize(neededNodesDbPath);
        long rel = computeDirectorySize(relIndexDbPath);
        long poi = computeDirectorySize(poiIndexDbPath);
        long append = computeDirectorySize(appendDbPath);
        long tmpTotal = grid + node + way + needed + rel + poi + append;

        stats.setShardsBytes(shards);
        stats.setBoundariesBytes(boundaries);
        stats.setDatasetBytes(dataset);

        stats.setTmpGridBytes(grid);
        stats.setTmpNodeBytes(node);
        stats.setTmpWayBytes(way);
        stats.setTmpNeededBytes(needed);
        stats.setTmpRelBytes(rel);
        stats.setTmpPoiBytes(poi);
        stats.setTmpAppendBytes(append);
        stats.setTmpTotalBytes(tmpTotal);
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

        System.out.printf("\033[1;37m💾 Database Operations:\033[0m \033[1;36m%s writes\033[0m%n",
                          formatCompactNumber(stats.getRocksDbWrites()));

        System.out.println("\n\033[1;37m📦 Dataset Size:\033[0m " + formatSize(stats.getDatasetBytes()));
        System.out.println("  • poi_shards:  " + formatSize(stats.getShardsBytes()));
        System.out.println("  • boundaries:  " + formatSize(stats.getBoundariesBytes()));

        System.out.println("\n\033[1;37m🧹 Temporary DBs:\033[0m " + formatSize(stats.getTmpTotalBytes()));
        System.out.println("  • grid_index:  " + formatSize(stats.getTmpGridBytes()));
        System.out.println("  • node_cache:  " + formatSize(stats.getTmpNodeBytes()));
        System.out.println("  • way_index:   " + formatSize(stats.getTmpWayBytes()));
        System.out.println("  • needed_nodes:" + formatSize(stats.getTmpNeededBytes()));
        System.out.println("  • rel_index:   " + formatSize(stats.getTmpRelBytes()));
        System.out.println("  • poi_index:   " + formatSize(stats.getTmpPoiBytes()));
        System.out.println("  • append_index:   " + formatSize(stats.getTmpAppendBytes()));
        System.out.println();
    }

    private static class ImportStatistics {
        private final AtomicLong entitiesRead = new AtomicLong(0);
        private final AtomicLong nodesCached = new AtomicLong(0);
        private final AtomicLong nodesFound = new AtomicLong(0);
        private final AtomicLong waysProcessed = new AtomicLong(0);
        private final AtomicLong relationsFound = new AtomicLong(0);
        private final AtomicLong boundariesProcessed = new AtomicLong(0);
        private final AtomicLong poisProcessed = new AtomicLong(0);
        private final AtomicLong poiIndexRecRead = new AtomicLong(0);
        private final AtomicBoolean poiIndexRecReadDone = new AtomicBoolean(false);
        private final AtomicLong rocksDbWrites = new AtomicLong(0);
        private final AtomicLong queueSize = new AtomicLong(0);
        private final AtomicLong activeThreads = new AtomicLong(0);
        private volatile String currentPhase = "Initializing";
        private volatile boolean running = true;
        private final long startTime = System.currentTimeMillis();
        private volatile long phaseStartTime = System.currentTimeMillis();
        private long totalTime;

        private volatile long datasetBytes;
        private volatile long shardsBytes;
        private volatile long boundariesBytes;
        private volatile long tmpGridBytes;
        private volatile long tmpNodeBytes;
        private volatile long tmpWayBytes;
        private volatile long tmpNeededBytes;
        private volatile long tmpRelBytes;
        private volatile long tmpPoiBytes;
        private volatile long tmpTotalBytes;
        private volatile long tmpAppendBytes;

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

        public void incrementPoisProcessed(int count) {
            poisProcessed.addAndGet(count);
        }
        public long getPoiIndexRecRead() { return poiIndexRecRead.get(); }
        public void incrementPoiIndexRecRead() { poiIndexRecRead.incrementAndGet(); }
        public boolean isPoiIndexRecReadDone() { return poiIndexRecReadDone.get(); }
        public void setPoiIndexRecReadDone() { poiIndexRecReadDone.set(true); }
        public long getRocksDbWrites() { return rocksDbWrites.get(); }
        public void incrementRocksDbWrites() { rocksDbWrites.incrementAndGet(); }
        public int getQueueSize() { return (int) queueSize.get(); }
        public void setQueueSize(int size) { queueSize.set(size); }
        public int getActiveThreads() { return (int) activeThreads.get(); }
        public void incrementActiveThreads() { activeThreads.incrementAndGet(); }
        public void decrementActiveThreads() { activeThreads.decrementAndGet(); }
        public String getCurrentPhase() { return currentPhase; }
        public void setCurrentPhase(String phase) { 
            this.currentPhase = phase; 
            this.phaseStartTime = System.currentTimeMillis();
        }
        public long getPhaseStartTime() { return phaseStartTime; }
        public boolean isRunning() { return running; }
        public void stop() { this.running = false; }
        public long getStartTime() { return startTime; }
        public long getTotalTime() { return totalTime; }
        public void setTotalTime(long t) { this.totalTime = t; }

        public long getDatasetBytes() { return datasetBytes; }
        public void setDatasetBytes(long v) { this.datasetBytes = v; }
        public long getShardsBytes() { return shardsBytes; }
        public void setShardsBytes(long v) { this.shardsBytes = v; }
        public long getBoundariesBytes() { return boundariesBytes; }
        public void setBoundariesBytes(long v) { this.boundariesBytes = v; }
        public long getTmpGridBytes() { return tmpGridBytes; }
        public void setTmpGridBytes(long v) { this.tmpGridBytes = v; }
        public long getTmpNodeBytes() { return tmpNodeBytes; }
        public void setTmpNodeBytes(long v) { this.tmpNodeBytes = v; }
        public long getTmpWayBytes() { return tmpWayBytes; }
        public void setTmpWayBytes(long v) { this.tmpWayBytes = v; }
        public long getTmpNeededBytes() { return tmpNeededBytes; }
        public void setTmpNeededBytes(long v) { this.tmpNeededBytes = v; }
        public long getTmpRelBytes() { return tmpRelBytes; }
        public void setTmpRelBytes(long v) { this.tmpRelBytes = v; }
        public long getTmpPoiBytes() { return tmpPoiBytes; }
        public void setTmpPoiBytes(long v) { this.tmpPoiBytes = v; }
        public long getTmpAppendBytes() { return tmpAppendBytes; }
        public void setTmpAppendBytes(long v) { this.tmpAppendBytes = v; }
        public long getTmpTotalBytes() { return tmpTotalBytes; }
        public void setTmpTotalBytes(long v) { this.tmpTotalBytes = v; }

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

    private record BoundaryResultLite(long osmId, int level, String name, org.locationtech.jts.geom.Geometry geometry) {
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
                if (batch != null) {
                    batch.close();
                }
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
        long[] outer;
        long[] inner;
    }

    private PoiIndexRec buildPoiIndexRecFromEntity(OsmEntity e) {
        PoiIndexRec rec = new PoiIndexRec();
        rec.type = "unknown";
        rec.subtype = "";
        for (int i = 0; i < e.getNumberOfTags(); i++) {
            OsmTag t = e.getTag(i);
            if (isPoiFastKey(t.getKey())) {
                rec.type = intern(t.getKey());
                rec.subtype = intern(t.getValue());
                break;
            }
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
        for (int i = 0; i < r.getNumberOfTags(); i++) {
            OsmTag t = r.getTag(i);
            if ("admin_level".equals(t.getKey())) {
                try { level = Integer.parseInt(t.getValue()); } catch (NumberFormatException ignore) { level = 10; }
            } else if ("name".equals(t.getKey())) {
                name = t.getValue();
            }
        }
        RelRec rec = new RelRec();
        rec.osmId = r.getId();
        rec.level = level;
        rec.name = name;
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
        int cap = 16 // ← NEW: 8 bytes lat + 8 bytes lon
                + 4 + typeB.length + 4 + subtypeB.length + namesSize
                + 4 + streetB.length + 4 + hnB.length + 4 + pcB.length + 4 + cityB.length + 4 + countryB.length;
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
        int cap = 4 + nameB.length + 4 + 8 * (r.outer != null ? r.outer.length : 0) + 4 + 8 * (r.inner != null ? r.inner.length : 0) + 4;
        ByteBuffer bb = ByteBuffer.allocate(cap);
        bb.putInt(r.level);
        putBytes(bb, nameB);
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
