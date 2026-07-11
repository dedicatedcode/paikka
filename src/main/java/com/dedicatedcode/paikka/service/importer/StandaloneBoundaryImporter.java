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

import com.uber.h3core.H3Core;
import com.uber.h3core.util.LatLng;
import de.topobyte.osm4j.core.model.iface.*;
import de.topobyte.osm4j.pbf.seq.PbfIterator;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.WKBWriter;
import org.rocksdb.*;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Standalone H3-based administrative boundary importer for Paikka.
 * <p>
 * Reads a pre-filtered boundaries_only.pbf (Nodes -> Ways -> Relations ordered)
 * and produces three RocksDB databases for offline mobile lookup:
 * - h3_to_osm       : H3_CELL_ID (uint64) -> List[OSM_ID] (raw byte array)
 * - region_metadata : OSM_ID -> total cell count (int)
 * - region_geometry : OSM_ID -> simplified WKB (bytes)
 */
@Service
public class StandaloneBoundaryImporter {

    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
    private static final int H3_RESOLUTION = 9;
    private static final double BUFFER_DISTANCE = 0.0001; // ~11m at equator, ensures border cells

    private final GeometrySimplificationService geometrySimplificationService;
    private final H3Core h3;
    private final BoundaryImportStatistics stats;

    public StandaloneBoundaryImporter(GeometrySimplificationService geometrySimplificationService) throws Exception {
        this.geometrySimplificationService = geometrySimplificationService;
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

        cleanup(nodeCachePath);
        cleanup(wayCachePath);
        cleanup(h3ToOsmPath);
        cleanup(regionMetaPath);
        cleanup(regionGeomPath);

        // Shared RocksDB options (inline with ImportService style)
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
        Options finalOpts = new Options()
                .setCreateIfMissing(true)
                .setTableFormatConfig(tableCfg)
                .setCompressionType(CompressionType.ZSTD_COMPRESSION)
                .setWriteBufferSize(256 * 1024 * 1024);
        WriteOptions wo = new WriteOptions().setDisableWAL(true);

        stats.startProgressReporter();

        try (
                RocksDB nodeCache = RocksDB.open(cacheOpts, nodeCachePath.toString());
                RocksDB wayCache = RocksDB.open(cacheOpts, wayCachePath.toString());
                RocksDB h3ToOsm = RocksDB.open(finalOpts, h3ToOsmPath.toString());
                RocksDB regionMeta = RocksDB.open(finalOpts, regionMetaPath.toString());
                RocksDB regionGeom = RocksDB.open(finalOpts, regionGeomPath.toString())
        ) {
            for (String pbfPath : pbfPaths) {
                stats.setCurrentPhase(1, "1.1: Caching Nodes & Ways");
                // ---------- SINGLE PASS ----------
                PbfIterator iterator = new PbfIterator(Files.newInputStream(Paths.get(pbfPath)), false);

                // Phase 1 & 2: Stream nodes and ways (cached)
                WriteBatch nodeBatch = new WriteBatch();
                WriteBatch wayBatch = new WriteBatch();
                AtomicLong phaseCounter = new AtomicLong();

                while (iterator.hasNext()) {
                    EntityContainer c = iterator.next();
                    if (c.getType() == EntityType.Node) {
                        // PHASE 1: Cache node coordinates (lat, lon) as 16-byte double pair
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
                        // PHASE 2: Cache way node-id sequences (long[] as raw bytes)
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
                        // PHASE 3: Relations (ways already fully cached above)
                        break; // Relations come after ways in ordered PBF; switch mode
                    }
                }
                nodeCache.write(wo, nodeBatch);
                wayCache.write(wo, wayBatch);
                nodeBatch.close();
                wayBatch.close();

                stats.setCurrentPhase(2, "2.1: Processing Relations & H3");
                // Re-open iterator for Phase 3 (or use two iterators; here we reuse file)

                // Phase 3: Process Relations (separate iterator pass is fine since PBF is local)
                try (InputStream is = Files.newInputStream(Paths.get(pbfPath))) {
                    PbfIterator relIter = new PbfIterator(is, false);

                    List<RelationStub> relations = new ArrayList<>();
                    while (relIter.hasNext()) {
                        EntityContainer c = relIter.next();
                        if (c.getType() == EntityType.Relation) {
                            OsmRelation r = (OsmRelation) c.getEntity();
                            if (isAdministrativeBoundary(r)) {
                                relations.add(buildRelationStub(r));
                                stats.incrementRelationsFound();
                            }
                        }
                    }

                    // Process each relation: stitch geometry, H3 polyfill, write outputs
                    WriteBatch h3Batch = new WriteBatch();
                    WriteBatch metaBatch = new WriteBatch();
                    WriteBatch geomBatch = new WriteBatch();

                    for (RelationStub stub : relations) {
                        Geometry geom = buildMultiPolygon(stub, nodeCache, wayCache);
                        if (geom == null || geom.isEmpty() || !geom.isValid()) continue;

                        // Buffer to include border-touching cells
                        Geometry buffered = geom.buffer(BUFFER_DISTANCE);
                        Geometry simplified = geometrySimplificationService.simplifyByAdminLevel(buffered, stub.adminLevel);
                        if (simplified == null || simplified.isEmpty()) simplified = buffered;

                        // ---- H3 Polyfill ----
                        List<Long> cells = polygonToCellsH3(simplified);
                        if (cells.isEmpty()) continue;

                        stats.incrementRelationsProcessed();
                        stats.addH3CellsGenerated(cells.size());

                        // h3_to_osm : append OSM_ID to each cell (dedup)
                        for (long cell : cells) {
                            byte[] key = longToBytes(cell);
                            byte[] existing = h3ToOsm.get(key);
                            byte[] updated = appendOsmIdToArray(existing, stub.osmId);
                            h3Batch.put(key, updated);
                        }

                        // region_metadata : OSM_ID -> cell count (int)
                        metaBatch.put(longToBytes(stub.osmId), intToBytes(cells.size()));

                        // region_geometry : OSM_ID -> simplified WKB
                        byte[] wkb = new WKBWriter().write(simplified);
                        geomBatch.put(longToBytes(stub.osmId), wkb);
                    }

                    h3ToOsm.write(wo, h3Batch);
                    regionMeta.write(wo, metaBatch);
                    regionGeom.write(wo, geomBatch);
                    h3Batch.close();
                    metaBatch.close();
                    geomBatch.close();
                    wo.close();
                }

            }

            // Compact finals
            h3ToOsm.compactRange();
            regionMeta.compactRange();
            regionGeom.compactRange();
        }

        stats.stop();
        stats.setTotalTime(System.currentTimeMillis() - stats.getStartTime());
        stats.printFinalStatistics();
        stats.printOutcomeAndErrors();

        // Cleanup tmp
        cleanup(tmp);
        System.out.println("[StandaloneBoundaryImporter] Import complete. Temporary caches removed.");
    }

    // ============================ GEOMETRY STITCHING ============================

    /**
     * Builds a JTS MultiPolygon from relation outer/inner way members.
     * Rings are stitched by coordinate continuation (same logic as ImportService.buildConnectedRings).
     */
    private Geometry buildMultiPolygon(RelationStub stub, RocksDB nodeCache, RocksDB wayCache) {
        List<List<Coordinate>> outerRings = stitchRings(stub.outerWays, nodeCache, wayCache);
        List<List<Coordinate>> innerRings = stitchRings(stub.innerWays, nodeCache, wayCache);
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
                        stats.recordError(BoundaryImportStatistics.Stage.PROCESSING_RELATIONS, BoundaryImportStatistics.Kind.GEOMETRY, stub.osmId, "createLinearRing-inner", e);
                    }
                }
                Polygon p = GEOMETRY_FACTORY.createPolygon(shell, holes.toArray(new LinearRing[0]));
                if (p.isValid()) polygons.add(p);
            } catch (Exception e) {
                stats.recordError(BoundaryImportStatistics.Stage.PROCESSING_RELATIONS, BoundaryImportStatistics.Kind.GEOMETRY, stub.osmId, "buildMultiPolygon", e);
            }
        }
        if (polygons.isEmpty()) return null;
        return polygons.size() == 1 ? polygons.get(0) : GEOMETRY_FACTORY.createMultiPolygon(polygons.toArray(new Polygon[0]));
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
                Coordinate end = ring.get(ring.size() - 1);
                for (Map.Entry<Long, List<Coordinate>> e : wayCoords.entrySet()) {
                    if (used.contains(e.getKey())) continue;
                    List<Coordinate> w = e.getValue();
                    if (end.equals2D(w.get(0))) {
                        ring.addAll(w.subList(1, w.size()));
                        used.add(e.getKey());
                        extended = true;
                        break;
                    } else if (end.equals2D(w.get(w.size() - 1))) {
                        List<Coordinate> rev = new ArrayList<>(w);
                        Collections.reverse(rev);
                        ring.addAll(rev.subList(1, rev.size()));
                        used.add(e.getKey());
                        extended = true;
                        break;
                    }
                }
            } while (extended);
            if (ring.size() >= 3 && !ring.get(0).equals2D(ring.get(ring.size() - 1)))
                ring.add(new Coordinate(ring.get(0)));
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
     * Converts a JTS Geometry to H3 cells at resolution 9.
     * Uses h3.polygonToCells with LatLng vertices. Multipolygons are expanded.
     */
    private List<Long> polygonToCellsH3(Geometry geom) {
        List<Long> cells = new ArrayList<>();
        int num = geom.getNumGeometries();
        for (int i = 0; i < num; i++) {
            Geometry part = geom.getGeometryN(i);
            if (!(part instanceof Polygon poly)) continue;
            List<LatLng> outer = toLatLng(poly.getExteriorRing().getCoordinates());
            List<List<LatLng>> holes = new ArrayList<>();
            for (int h = 0; h < poly.getNumInteriorRing(); h++) {
                holes.add(toLatLng(poly.getInteriorRingN(h).getCoordinates()));
            }
            try {
                List<Long> partCells = h3.polygonToCells(outer, holes, H3_RESOLUTION);
                cells.addAll(partCells);
            } catch (Exception e) {
                stats.recordError(BoundaryImportStatistics.Stage.PROCESSING_RELATIONS, BoundaryImportStatistics.Kind.GEOMETRY, null, "polygonToCellsH3", e);
            }
        }
        return cells;
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

    private long bytesToLong(byte[] b) {
        return ByteBuffer.wrap(b).order(ByteOrder.BIG_ENDIAN).getLong();
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

    private byte[] intToBytes(int v) {
        return ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(v).array();
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
}
