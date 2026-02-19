package com.dedicatedcode.paikka.service;

import com.dedicatedcode.paikka.flatbuffers.*;
import com.dedicatedcode.paikka.flatbuffers.Geometry;
import com.google.flatbuffers.FlatBufferBuilder;
import de.topobyte.osm4j.core.access.OsmIterator;
import de.topobyte.osm4j.core.model.iface.*;
import de.topobyte.osm4j.pbf.seq.PbfIterator;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.index.strtree.STRtree;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.rocksdb.*;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
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

    // Simple cache to deduplicate common tag strings and reduce heap pressure
    private final Map<String, String> tagCache = new ConcurrentHashMap<>(1000);

    // ======== Data-Holding Records for Processing Pipeline ========
    private record PoiData(long id, double lat, double lon, String type, String subtype, List<NameData> names,
                           AddressData address, List<HierarchyCache.SimpleHierarchyItem> hierarchy,
                           byte[] boundaryWkb) {
    }

    private record NameData(String lang, String text) {
    }

    private record AddressData(String street, String houseNumber, String postcode, String city, String country) {
    }

    private record BoundaryResult(OsmRelation relation, org.locationtech.jts.geom.Geometry geometry,
                                  WKBWriter wkbWriter) {
    }

    @FunctionalInterface
    private interface ConsumerWithException<T> {
        void accept(T t) throws Exception;
    }

    public ImportService(S2Helper s2Helper, GeometrySimplificationService geometrySimplificationService) {
        this.s2Helper = s2Helper;
        this.geometrySimplificationService = geometrySimplificationService;
    }

    public void importData(String pbfFilePath, String dataDir) throws Exception {
        long totalStartTime = System.currentTimeMillis();
        printHeader(pbfFilePath, dataDir);

        Path pbfFile = Paths.get(pbfFilePath);
        Path dataDirectory = Paths.get(dataDir);
        dataDirectory.toFile().mkdirs();
        Path shardsDbPath = dataDirectory.resolve("poi_shards");
        Path boundariesDbPath = dataDirectory.resolve("boundaries");

        RocksDB.loadLibrary();
        ImportStatistics stats = new ImportStatistics();

        try (Options options = new Options().setCreateIfMissing(true)
                .setCompressionType(CompressionType.LZ4_COMPRESSION);
             RocksDB shardsDb = RocksDB.open(options, shardsDbPath.toString());
             RocksDB boundariesDb = RocksDB.open(options, boundariesDbPath.toString());
             RocksDB nodeCache = createNodeCache(dataDirectory)) {

            printPhaseHeader("PASS 1: Node Harvesting & Admin Boundaries");
            STRtree adminTree = new STRtree();
            pass1NodeHarvestingAndAdminProcessing(pbfFile, nodeCache, adminTree, boundariesDb, stats);

            printPhaseHeader("PASS 2: POI Sharding & Hierarchy Baking");
            pass2PoiShardingAndBaking(pbfFile, nodeCache, adminTree, shardsDb, boundariesDb, stats);

            stats.setTotalTime(System.currentTimeMillis() - totalStartTime);
            printFinalStatistics(stats);
            printSuccess();

        } catch (Exception e) {
            printError("IMPORT FAILED: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }

    private void pass1NodeHarvestingAndAdminProcessing(Path pbfFile, RocksDB nodeCache, STRtree adminTree, RocksDB boundariesDb, ImportStatistics stats) throws Exception {
        Map<Long, List<Long>> wayNodeSequences = new ConcurrentHashMap<>();

        printSubPhase("1.1: Harvesting node coordinates, way data, and admin relations");
        AtomicLong nodeCount = new AtomicLong(0);
        List<OsmRelation> adminRelations = Collections.synchronizedList(new ArrayList<>());
        harvestNodeCoordinatesWaysAndRelationsParallel(pbfFile, nodeCache, wayNodeSequences, adminRelations, nodeCount);
        stats.setNodesProcessed(nodeCount.get());
        stats.setWaysProcessed(wayNodeSequences.size());

        printSubPhase("1.2: Processing administrative boundaries");
        AtomicLong boundaryCount = new AtomicLong(0);
        processAdministrativeBoundariesParallel(nodeCache, wayNodeSequences, adminTree, boundariesDb, boundaryCount, adminRelations);
        stats.setBoundariesProcessed(boundaryCount.get());

        printPassComplete(nodeCount.get(), wayNodeSequences.size(), adminTree.size());
    }

    private void pass2PoiShardingAndBaking(Path pbfFile, RocksDB nodeCache, STRtree adminTree,
                                           RocksDB shardsDb, RocksDB boundariesDb, ImportStatistics stats) throws Exception {
        long startTime = System.currentTimeMillis();
        printSubPhase("2.1: Processing POIs and building shards (Planet-Scale Mode)");

        final Map<Long, List<PoiData>> shardBuffer = new ConcurrentHashMap<>();

        ScheduledExecutorService flushingExecutor = Executors.newSingleThreadScheduledExecutor();
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
                    writeShardBatch(bufferToFlush, shardsDb);
                }
            } catch (Exception e) {
                System.err.println("\n\u001B[31m[Flusher] Error during periodic flush: " + e.getMessage() + "\u001B[0m");
            }
        };
        flushingExecutor.scheduleAtFixedRate(flushTask, 10, 10, TimeUnit.SECONDS);

        AtomicLong processedCount = new AtomicLong(0);
        AtomicLong readerCount = new AtomicLong(0);
        BlockingQueue<EntityContainer> entityQueue = new LinkedBlockingQueue<>(200_000);
        
        // Use Virtual Threads for processing
        int numProcessors = Runtime.getRuntime().availableProcessors() * 2;
        CountDownLatch latch = new CountDownLatch(numProcessors);
        
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            final ThreadLocal<HierarchyCache> hierarchyCacheThreadLocal = ThreadLocal.withInitial(
                () -> new HierarchyCache(adminTree, boundariesDb, s2Helper)
            );

            Thread readerThread = Thread.ofVirtual().start(() -> {
                try {
                    withPbfIterator(pbfFile, iterator -> {
                        while (iterator.hasNext()) {
                            EntityContainer container = iterator.next();
                            OsmEntity entity = container.getEntity();
                            if ((entity instanceof OsmNode && isPoi(entity)) || (entity instanceof OsmWay && isPoi(entity))) {
                                entityQueue.put(container);
                                readerCount.incrementAndGet();
                            }
                        }
                    });
                } catch (Exception e) {
                    System.err.println("Reader thread failed: " + e.getMessage());
                } finally {
                    for (int i = 0; i < numProcessors; i++) {
                        try {
                            entityQueue.put(createPoisonPill());
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                }
            });

            for (int i = 0; i < numProcessors; i++) {
                executor.submit(() -> {
                    final Map<Long, List<PoiData>> localShardBuffer = new HashMap<>();
                    final WKBReader wkbReader = new WKBReader();
                    final GeometryFactory geometryFactory = new GeometryFactory();
                    final HierarchyCache hierarchyCache = hierarchyCacheThreadLocal.get();
                    try {
                        while (true) {

                            EntityContainer container = entityQueue.take();
                            if (isPoisonPill(container)) break;
                            try {
                                PoiData poiData = processPoi(container.getEntity(), nodeCache, hierarchyCache, geometryFactory, wkbReader);
                                if (poiData != null) {
                                    localShardBuffer.computeIfAbsent(s2Helper.getShardId(poiData.lat(), poiData.lon()), k -> new ArrayList<>()).add(poiData);
                                }
                            } catch (Exception e) { /* Skip failed POI */ }

                            long count = processedCount.incrementAndGet();
                            if (count > 0 && count % 25000 == 0) {
                                updateProgress(startTime, readerCount.get(), count, entityQueue.size());
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
                        latch.countDown();
                    }
                });
            }

            latch.await();
            readerThread.join();
        }

        System.out.print("\033[5B"); // Move cursor down to clear progress area
        System.out.println("\nProcessing complete. Performing final flush...");
        flushingExecutor.shutdown();
        if (!flushingExecutor.awaitTermination(5, TimeUnit.MINUTES)) flushingExecutor.shutdownNow();
        flushTask.run();

        stats.setPoisProcessed(processedCount.get());
        printPhaseComplete(processedCount.get(), System.currentTimeMillis() - startTime);
    }

    private void updateProgress(long startTime, long currentReaderCount, long count, int queueSize) {
        long elapsed = System.currentTimeMillis() - startTime;
        long readerPerSec = elapsed > 0 ? (currentReaderCount * 1000L) / elapsed : 0;
        long poisPerSec = elapsed > 0 ? (count * 1000L) / elapsed : 0;

        System.out.print("\033[2K\r"); // Clear line
        System.out.println("\033[35mPOI Processing Progress:\033[0m");
        System.out.printf("  \033[32mReader:\033[0m          %,d found (\033[33m%s/sec\033[0m)\n", currentReaderCount, formatCompactNumber(readerPerSec));
        System.out.printf("  \033[32mProcessors:\033[0m      %,d processed (\033[33m%s/sec\033[0m)\n", count, formatCompactNumber(poisPerSec));
        System.out.printf("  \033[32mQueue size:\033[0m      %,d\n", queueSize);
        System.out.printf("  \033[32mElapsed:\033[0m         %s\n", formatTime(elapsed));
        System.out.print("\033[5A"); // Move cursor up for next update
        System.out.flush();
    }

    private void harvestNodeCoordinatesWaysAndRelationsParallel(Path pbfFile, RocksDB nodeCache, Map<Long, List<Long>> wayNodeSequences,
                                                                List<OsmRelation> adminRelations, AtomicLong nodeCount) throws Exception {
        Roaring64Bitmap usedNodes = new Roaring64Bitmap();
        System.out.println("First pass: Scanning PBF for ways, relations, and needed nodes...");
        long startTime = System.currentTimeMillis();
        AtomicLong entitiesRead = new AtomicLong(0);
        AtomicLong poiWayCount = new AtomicLong(0);
        AtomicLong adminWayCount = new AtomicLong(0);

        withPbfIterator(pbfFile, iterator -> {
            while (iterator.hasNext()) {
                EntityContainer container = iterator.next();
                long currentCount = entitiesRead.incrementAndGet();
                if (container.getType() == EntityType.Way) {
                    OsmWay way = (OsmWay) container.getEntity();
                    boolean isPoi = isPoi(way);
                    boolean isAdmin = isAdministrativeBoundaryWay(way);
                    if (isPoi || isAdmin) {
                        if (isPoi) poiWayCount.incrementAndGet();
                        if (isAdmin) adminWayCount.incrementAndGet();
                        List<Long> nodeIds = new ArrayList<>();
                        for (int j = 0; j < way.getNumberOfNodes(); j++) nodeIds.add(way.getNodeId(j));
                        wayNodeSequences.put(way.getId(), nodeIds);
                        usedNodes.add(nodeIds.stream().mapToLong(Long::longValue).toArray());
                    }
                } else if (container.getType() == EntityType.Relation && isAdministrativeBoundary((OsmRelation) container.getEntity())) {
                    adminRelations.add((OsmRelation) container.getEntity());
                } else if (container.getType() == EntityType.Node && isPoi(container.getEntity())) {
                    usedNodes.add(container.getEntity().getId());
                }

                if (currentCount > 0 && currentCount % 2_000_000 == 0) {
                    long elapsed = System.currentTimeMillis() - startTime;
                    System.out.print("\033[2K\r\033[36mFirst Pass Scan:\033[0m\n");
                    System.out.printf("  \033[32mEntities read:\033[0m   %,d (\033[33m%s/sec\033[0m)\n", currentCount, formatCompactNumber((currentCount * 1000L) / Math.max(1, elapsed)));
                    System.out.printf("  \033[32mPOI/Admin Ways:\033[0m  %,d / %,d\n", poiWayCount.get(), adminWayCount.get());
                    System.out.printf("  \033[32mAdmin Relations:\033[0m %,d\n", adminRelations.size());
                    System.out.printf("  \033[32mNeeded Nodes:\033[0m    %,d\n", usedNodes.getLongCardinality());
                    System.out.printf("  \033[32mElapsed:\033[0m         %s\n\033[6A", formatTime(elapsed));
                    System.out.flush();
                }
            }
        });
        System.out.print("\033[6B");
        usedNodes.runOptimize();
        System.out.printf("✓ First pass scan complete. Needed nodes identified: %,d\n", usedNodes.getLongCardinality());

        System.out.println("Second pass: Caching needed node coordinates...");
        BlockingQueue<EntityContainer> nodeQueue = new LinkedBlockingQueue<>(200_000);
        int numProcessors = Runtime.getRuntime().availableProcessors();
        CountDownLatch latch = new CountDownLatch(numProcessors);

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Thread readerThread = Thread.ofVirtual().start(() -> {
                try {
                    withPbfIterator(pbfFile, iterator -> {
                        while (iterator.hasNext()) {
                            EntityContainer container = iterator.next();
                            if (container.getType() == EntityType.Node && usedNodes.contains(((OsmNode) container.getEntity()).getId())) {
                                nodeQueue.put(container);
                            }
                        }
                    });
                } catch (Exception e) {
                    System.err.println("Node reader failed: " + e.getMessage());
                } finally {
                    for (int i = 0; i < numProcessors; i++) {
                        try {
                            nodeQueue.put(createPoisonPill());
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
            });

            for (int i = 0; i < numProcessors; i++) {
                executor.submit(() -> {
                    try (WriteBatch batch = new WriteBatch()) {
                        int batchSize = 0;
                        while (true) {
                            EntityContainer container = nodeQueue.take();
                            if (isPoisonPill(container)) break;
                            OsmNode node = (OsmNode) container.getEntity();
                            byte[] value = ByteBuffer.allocate(16).putDouble(node.getLatitude()).putDouble(node.getLongitude()).array();
                            batch.put(s2Helper.longToByteArray(node.getId()), value);
                            batchSize++;

                            long currentCount = nodeCount.incrementAndGet();
                            if (currentCount % 100_000 == 0) {
                                long elapsed = System.currentTimeMillis() - startTime;
                                System.out.printf("\r  \033[32mNodes Cached:\033[0m %,d (\033[33m%s/sec\033[0m)", currentCount, formatCompactNumber((currentCount * 1000L) / Math.max(1, elapsed)));
                            }

                            if (batchSize >= 1000) {
                                nodeCache.write(new WriteOptions(), batch);
                                batch.clear();
                                batchSize = 0;
                            }
                        }
                        if (batchSize > 0) nodeCache.write(new WriteOptions(), batch);
                    } catch (Exception e) { /* Skip storage */ } finally {
                        latch.countDown();
                    }
                });
            }
            latch.await();
            readerThread.join();
        }
        System.out.println();
    }

    private void processAdministrativeBoundariesParallel(RocksDB nodeCache, Map<Long, List<Long>> wayNodeSequences, STRtree adminTree,
                                                         RocksDB boundariesDb, AtomicLong boundaryCount, List<OsmRelation> adminRelations) throws Exception {
        if (adminRelations.isEmpty()) return;
        System.out.println("Processing " + adminRelations.size() + " administrative boundaries...");
        
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<BoundaryResult>> futures = new ArrayList<>();
            adminRelations.forEach(relation -> futures.add(executor.submit(() -> {
                org.locationtech.jts.geom.Geometry geometry = buildGeometryFromRelation(relation, nodeCache, wayNodeSequences);
                if (geometry != null && geometry.isValid()) {
                    org.locationtech.jts.geom.Geometry simplified = geometrySimplificationService.simplifyByAdminLevel(geometry, getAdminLevel(relation));
                    return new BoundaryResult(relation, simplified, new WKBWriter());
                }
                return null;
            })));

            int count = 0;
            for (Future<BoundaryResult> future : futures) {
                try {
                    BoundaryResult result = future.get();
                    if (result != null) {
                        storeBoundary(result.relation(), result.geometry(), result.wkbWriter(), boundariesDb);
                        adminTree.insert(result.geometry().getEnvelopeInternal(), result.relation().getId());
                        if (++count % 100 == 0) System.out.printf("\r  Boundaries indexed: %,d", count);
                    }
                } catch (Exception e) { /* Skip */ }
            }
            System.out.printf("\r  Boundaries indexed: %,d\n", count);
            boundaryCount.set(count);
        }
        System.out.println("Building STR-Tree index for boundaries...");
        adminTree.build();
    }

    private PoiData processPoi(OsmEntity entity, RocksDB nodeCache, HierarchyCache hierarchyCache, GeometryFactory geometryFactory, WKBReader wkbReader) {
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

        // Fill missing city/country from hierarchy if not present in tags
        if (city == null || country == null) {
            // Sort hierarchy by level descending (lowest level first) to find the most specific city/country
            List<HierarchyCache.SimpleHierarchyItem> sortedHierarchy = new ArrayList<>(hierarchy);
            sortedHierarchy.sort(Comparator.comparingInt(HierarchyCache.SimpleHierarchyItem::level).reversed());

            for (HierarchyCache.SimpleHierarchyItem item : sortedHierarchy) {
                // Infer city from administrative levels 6-10 (municipality, city, town, village levels)
                if (city == null && item.level() >= 6 && item.level() <= 10) {
                    city = item.name();
                }
                // Infer country from administrative level 2 (country level)
                if (country == null && item.level() == 2) {
                    country = item.name();
                }
                if (city != null && country != null) break;
            }
        }

        if (street == null && houseNumber == null && postcode == null && city == null && country == null) return null;
        return new AddressData(street, houseNumber, postcode, city, country);
    }

    private void writeShardBatch(Map<Long, List<PoiData>> shardBuffer, RocksDB shardsDb) throws Exception {
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
                        boundaryOff = Geometry.createGeometry(builder, builder.createString("Polygon"), boundaryDataOff);
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
        }
    }

    private org.locationtech.jts.geom.Geometry buildGeometryFromRelation(OsmRelation relation, RocksDB nodeCache, Map<Long, List<Long>> wayNodeSequences) {
        List<Long> outerWayIds = new ArrayList<>(), innerWayIds = new ArrayList<>();
        for (int i = 0; i < relation.getNumberOfMembers(); i++) {
            OsmRelationMember member = relation.getMember(i);
            if (member.getType() == EntityType.Way) {
                String role = member.getRole();
                if ("outer".equals(role) || role == null || role.isEmpty()) outerWayIds.add(member.getId());
                else if ("inner".equals(role)) innerWayIds.add(member.getId());
            }
        }
        List<List<Coordinate>> outerRings = buildConnectedRings(outerWayIds, nodeCache, wayNodeSequences);
        List<List<Coordinate>> innerRings = buildConnectedRings(innerWayIds, nodeCache, wayNodeSequences);
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

    private List<Coordinate> buildCoordinatesFromWay(Long wayId, RocksDB nodeCache, Map<Long, List<Long>> wayNodeSequences) {
        List<Long> nodeIds = wayNodeSequences.get(wayId);
        if (nodeIds == null || nodeIds.isEmpty()) return null;
        try {
            List<byte[]> keys = nodeIds.stream().map(s2Helper::longToByteArray).toList();
            List<byte[]> values = nodeCache.multiGetAsList(keys);
            if (values.size() != keys.size()) return null;
            List<Coordinate> coordinates = new ArrayList<>(nodeIds.size());
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

    private List<List<Coordinate>> buildConnectedRings(List<Long> wayIds, RocksDB nodeCache, Map<Long, List<Long>> wayNodeSequences) {
        if (wayIds.isEmpty()) return Collections.emptyList();
        Map<Long, List<Coordinate>> wayCoordinates = new HashMap<>();
        for (Long wayId : wayIds) {
            List<Coordinate> coords = buildCoordinatesFromWay(wayId, nodeCache, wayNodeSequences);
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

    private void storeBoundary(OsmRelation relation, org.locationtech.jts.geom.Geometry geometry, WKBWriter wkbWriter, RocksDB boundariesDb) throws Exception {
        FlatBufferBuilder builder = new FlatBufferBuilder(1024);
        int wkbOffset = Geometry.createDataVector(builder, wkbWriter.write(geometry));
        int geomOffset = Geometry.createGeometry(builder, builder.createString(geometry.getGeometryType()), wkbOffset);
        int boundaryOffset = Boundary.createBoundary(builder, relation.getId(), getAdminLevel(relation), builder.createString(getBoundaryName(relation)), geomOffset);
        builder.finish(boundaryOffset);
        boundariesDb.put(s2Helper.longToByteArray(relation.getId()), builder.sizedByteArray());
    }

    private RocksDB createNodeCache(Path dataDir) throws Exception {
        return RocksDB.open(new Options().setCreateIfMissing(true)
                .setWriteBufferSize(512 * 1024 * 1024)
                .setCompressionType(CompressionType.LZ4_COMPRESSION), 
                dataDir.resolve("node_cache").toString());
    }

    private EntityContainer createPoisonPill() {
        return new EntityContainer(null, null);
    }

    private boolean isPoisonPill(EntityContainer c) {
        return c.getType() == null;
    }

    private void withPbfIterator(Path pbfFile, ConsumerWithException<OsmIterator> consumer) throws Exception {
        try (RandomAccessFile file = new RandomAccessFile(pbfFile.toFile(), "r"); FileChannel channel = file.getChannel()) {
            ByteBuffer mappedBuffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
            InputStream inputStream = new InputStream() {
                @Override
                public int read() { return mappedBuffer.hasRemaining() ? mappedBuffer.get() & 0xFF : -1; }
                @Override
                public int read(byte[] b, int off, int len) {
                    if (!mappedBuffer.hasRemaining()) return -1;
                    len = Math.min(len, mappedBuffer.remaining());
                    mappedBuffer.get(b, off, len);
                    return len;
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
        System.out.println("\n\033[1;34m" + "=".repeat(80) + "\n" + centerText("PAIKKA IMPORT STARTING (JAVA 25 OPTIMIZED)") + "\n" + "=".repeat(80) + "\033[0m\n");
        System.out.println("PBF File: " + pbfFilePath);
        System.out.println("Data Dir: " + dataDir);
        System.out.println("Threads: Virtual Threads Enabled");
        long maxHeapBytes = Runtime.getRuntime().maxMemory();
        String maxHeapSize = (maxHeapBytes == Long.MAX_VALUE) ? "unlimited" : (maxHeapBytes / (1024 * 1024 * 1024)) + "GB";
        System.out.println("Max Heap: " + maxHeapSize);
    }

    private void printPhaseHeader(String phase) {
        System.out.println("\n\033[1;36m" + "─".repeat(80) + "\n" + phase + "\n" + "─".repeat(80) + "\033[0m");
    }

    private void printSubPhase(String subPhase) {
        System.out.println("\n\033[1;33m▶ " + subPhase + "\033[0m");
    }

    private void printPhaseComplete(long count, long ms) {
        System.out.printf("\n\u001B[32m✓ POI processing complete:\u001B[0m %,d processed in %s%n", count, formatTime(ms));
    }

    private void printPassComplete(long nodes, long ways, long boundaries) {
        System.out.printf("\n\u001B[1;32m✓ PASS 1 COMPLETE\u001B[0m%n  Nodes cached: %,d | Ways cached: %,d | Boundaries indexed: %,d%n", nodes, ways, boundaries);
    }

    private void printSuccess() {
        System.out.println("\n\033[1;32m" + "=".repeat(80) + "\n" + centerText("IMPORT COMPLETED SUCCESSFULLY") + "\n" + "=".repeat(80) + "\033[0m");
    }

    private void printError(String message) {
        System.out.println("\n\033[1;31m" + "=".repeat(80) + "\n" + centerText(message) + "\n" + "=".repeat(80) + "\033[0m");
    }

    private void printFinalStatistics(ImportStatistics stats) {
        System.out.println("\n\033[1;36m" + "─".repeat(80) + "\n" + centerText("IMPORT STATISTICS") + "\n" + "─".repeat(80) + "\033[0m");
        long totalTime = Math.max(1, stats.getTotalTime());
        System.out.printf("\n\u001B[1mTotal time:\u001B[0m %s%n\n\033[1mObjects processed:\u001B[0m%n", formatTime(stats.getTotalTime()));
        System.out.printf("  \u001B[32mNodes:\u001B[0m      %,d (\u001B[33m%s/sec\u001B[0m)%n", stats.getNodesProcessed(), formatCompactNumber(stats.getNodesProcessed() * 1000L / totalTime));
        System.out.printf("  \u001B[32mWays:\u001B[0m       %,d (\u001B[33m%s/sec\u001B[0m)%n", stats.getWaysProcessed(), formatCompactNumber(stats.getWaysProcessed() * 1000L / totalTime));
        System.out.printf("  \u001B[32mBoundaries:\u001B[0m %,d (\u001B[33m%s/sec\u001B[0m)%n", stats.getBoundariesProcessed(), formatCompactNumber(stats.getBoundariesProcessed() * 1000L / totalTime));
        System.out.printf("  \u001B[32mPOIs:\u001B[0m       %,d (\u001B[33m%s/sec\u001B[0m)%n\n", stats.getPoisProcessed(), formatCompactNumber(stats.getPoisProcessed() * 1000L / totalTime));
        long totalObjects = stats.getNodesProcessed() + stats.getWaysProcessed() + stats.getBoundariesProcessed() + stats.getPoisProcessed();
        System.out.printf("\u001B[1mTotal objects:\u001B[0m %,d (\u001B[33m%s/sec\u001B[0m)%n\n", totalObjects, formatCompactNumber(totalObjects * 1000L / totalTime));
    }

    private static class ImportStatistics {
        long nodesProcessed, waysProcessed, boundariesProcessed, poisProcessed, totalTime;
        public long getNodesProcessed() { return nodesProcessed; }
        public void setNodesProcessed(long n) { this.nodesProcessed = n; }
        public long getWaysProcessed() { return waysProcessed; }
        public void setWaysProcessed(long w) { this.waysProcessed = w; }
        public long getBoundariesProcessed() { return boundariesProcessed; }
        public void setBoundariesProcessed(long b) { this.boundariesProcessed = b; }
        public long getPoisProcessed() { return poisProcessed; }
        public void setPoisProcessed(long p) { this.poisProcessed = p; }
        public long getTotalTime() { return totalTime; }
        public void setTotalTime(long t) { this.totalTime = t; }
    }
}
