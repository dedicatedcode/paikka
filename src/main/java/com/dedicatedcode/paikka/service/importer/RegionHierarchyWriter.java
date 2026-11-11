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
 * You should have received a copy of the GNU Affero General Public License
 * along with Paikka. If not, see <https://www.gnu.org/licenses/>.
 */

package com.dedicatedcode.paikka.service.importer;

import com.uber.h3core.H3Core;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.WKBReader;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Builds the administrative region tree from the relation membership edges collected during the
 * PBF pass and writes it as a Postgres-COPY compatible side-car file:
 *
 * <pre>
 * osm_id  parent_osm_id  kind  admin_level  resolution  total_cells  depth  child_count  continent
 * </pre>
 */
public class RegionHierarchyWriter {

    private static final Logger log = LoggerFactory.getLogger(RegionHierarchyWriter.class);
    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
    private static final int[] PARENT_RESOLUTIONS = {9, 6, 4};
    private static final int GEOMETRY_CACHE_SIZE = 3000;

    private final WKBReader wkbReader = new WKBReader(GEOMETRY_FACTORY);
    private final Map<Long, Geometry> geometryCache = new LinkedHashMap<>(GEOMETRY_CACHE_SIZE, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<Long, Geometry> eldest) {
            return size() > GEOMETRY_CACHE_SIZE;
        }
    };

    public int write(Path output,
                     RocksDB tmpRegionMeta,
                     RocksDB tmpRegionGeom,
                     RocksDB h3ToOsm,
                     H3Core h3,
                     Map<Long, String> isoCodes,
                     Map<Long, List<Long>> subareaEdges) throws IOException, RocksDBException {
        Map<Long, RegionInfo> regions = readRegions(tmpRegionMeta);
        Map<Long, Long> parentByChild = resolveParents(regions, subareaEdges, tmpRegionGeom, h3ToOsm, h3);
        Map<Long, Integer> childCounts = countChildren(parentByChild);
        Map<Long, Integer> depths = new HashMap<>(regions.size());
        Map<Long, String> continents = new HashMap<>(regions.size());
        int fallbackContinents = 0;

        for (RegionInfo region : regions.values()) {
            depth(region.osmId(), parentByChild, depths);
            Continent continent = resolveContinent(region.osmId(), parentByChild, isoCodes, tmpRegionGeom);
            if (continent.fromGeometry()) {
                fallbackContinents++;
            }
            continents.put(region.osmId(), continent.code());
        }

        List<RegionInfo> sorted = new ArrayList<>(regions.values());
        sorted.sort(Comparator.comparingLong(RegionInfo::osmId));

        try (BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
            for (RegionInfo region : sorted) {
                Long parentId = parentByChild.get(region.osmId());
                writer.write(Long.toString(region.osmId()));
                writer.write('\t');
                if (parentId != null) {
                    writer.write(Long.toString(parentId));
                }
                writer.write('\t');
                writer.write("ADMIN\t");
                writer.write(Integer.toString(region.adminLevel()));
                writer.write('\t');
                writer.write(Integer.toString(region.resolution()));
                writer.write('\t');
                writer.write(Long.toString(region.totalCells()));
                writer.write('\t');
                writer.write(Integer.toString(depths.getOrDefault(region.osmId(), 0)));
                writer.write('\t');
                writer.write(Integer.toString(childCounts.getOrDefault(region.osmId(), 0)));
                writer.write('\t');
                writer.write(continents.getOrDefault(region.osmId(), ContinentReference.UNKNOWN));
                writer.write('\n');
            }
        }

        log.info("Wrote hierarchy for {} regions ({} roots, {} continent assignments via geometry fallback)",
                regions.size(), regions.size() - parentByChild.size(), fallbackContinents);
        return regions.size();
    }

    private Map<Long, RegionInfo> readRegions(RocksDB tmpRegionMeta) throws RocksDBException {
        Map<Long, RegionInfo> regions = new HashMap<>();
        try (RocksIterator iterator = tmpRegionMeta.newIterator()) {
            iterator.seekToFirst();
            while (iterator.isValid()) {
                long osmId = ByteBuffer.wrap(iterator.key()).order(ByteOrder.BIG_ENDIAN).getLong();
                ByteBuffer value = ByteBuffer.wrap(iterator.value()).order(ByteOrder.BIG_ENDIAN);
                long totalCells = value.getLong();
                int resolution = value.getInt();
                int adminLevel = value.getInt();
                regions.put(osmId, new RegionInfo(osmId, adminLevel, resolution, totalCells));
                iterator.next();
            }
        }
        return regions;
    }

    /**
     * Picks the most specific valid parent for every child. A parent must have a strictly lower
     * admin level than the child, which also guarantees a cycle-free tree.
     * <p>
     * OSM subarea membership is incomplete in practice (e.g. Schleswig-Holstein does not list its
     * Kreise), so regions without a tree parent are resolved spatially using the H3 index.
     */
    private Map<Long, Long> resolveParents(Map<Long, RegionInfo> regions,
                                           Map<Long, List<Long>> subareaEdges,
                                           RocksDB tmpRegionGeom,
                                           RocksDB h3ToOsm,
                                           H3Core h3) throws RocksDBException {
        Map<Long, List<Long>> candidatesByChild = new HashMap<>();
        for (Map.Entry<Long, List<Long>> entry : subareaEdges.entrySet()) {
            Long parentId = entry.getKey();
            RegionInfo parent = regions.get(parentId);
            if (parent == null) {
                continue;
            }
            for (Long childId : entry.getValue()) {
                RegionInfo child = regions.get(childId);
                if (child == null || childId.equals(parentId) || child.adminLevel() <= parent.adminLevel()) {
                    continue;
                }
                candidatesByChild.computeIfAbsent(childId, ignored -> new ArrayList<>()).add(parentId);
            }
        }

        Map<Long, Long> parentByChild = new HashMap<>();
        for (Map.Entry<Long, List<Long>> entry : candidatesByChild.entrySet()) {
            entry.getValue().stream()
                    .min(Comparator
                            .comparingInt((Long parentId) -> regions.get(parentId).adminLevel()).reversed()
                            .thenComparingLong(parentId -> parentId))
                    .ifPresent(parentId -> parentByChild.put(entry.getKey(), parentId));
        }

        int spatialParents = 0;
        for (RegionInfo region : regions.values()) {
            if (parentByChild.containsKey(region.osmId())) {
                continue;
            }
            Long parentId = findSpatialParent(region, regions, tmpRegionGeom, h3ToOsm, h3);
            if (parentId != null) {
                parentByChild.put(region.osmId(), parentId);
                spatialParents++;
            }
        }
        if (spatialParents > 0) {
            log.info("Resolved {} region parents spatially (missing OSM subarea membership)", spatialParents);
        }
        return parentByChild;
    }

    private Long findSpatialParent(RegionInfo region,
                                   Map<Long, RegionInfo> regions,
                                   RocksDB tmpRegionGeom,
                                   RocksDB h3ToOsm,
                                   H3Core h3) throws RocksDBException {
        Geometry geometry = geometry(region.osmId(), tmpRegionGeom);
        if (geometry == null) {
            return null;
        }
        Point interior;
        try {
            interior = geometry.getInteriorPoint();
        } catch (Exception e) {
            interior = geometry.getCentroid();
        }
        if (interior == null || interior.isEmpty()) {
            return null;
        }
        final Point point = interior;

        Long parentId = selectParent(region, regions, tmpRegionGeom, h3ToOsm, h3, point, 1);
        if (parentId == null && region.adminLevel() >= 6) {
            parentId = selectParent(region, regions, tmpRegionGeom, h3ToOsm, h3, point, 2);
        }
        return parentId;
    }

    private Long selectParent(RegionInfo region,
                              Map<Long, RegionInfo> regions,
                              RocksDB tmpRegionGeom,
                              RocksDB h3ToOsm,
                              H3Core h3,
                              Point point,
                              int neighbourRadius) throws RocksDBException {
        Set<Long> candidates = new LinkedHashSet<>();
        for (int resolution : PARENT_RESOLUTIONS) {
            long cell = h3.latLngToCell(point.getY(), point.getX(), resolution);
            for (long neighbour : h3.gridDisk(cell, neighbourRadius)) {
                byte[] value = h3ToOsm.get(longToBytes(neighbour));
                if (value == null) {
                    continue;
                }
                ByteBuffer buffer = ByteBuffer.wrap(value).order(ByteOrder.BIG_ENDIAN);
                while (buffer.hasRemaining()) {
                    candidates.add(buffer.getLong());
                }
            }
        }

        return candidates.stream()
                .map(regions::get)
                .filter(candidate -> candidate != null && candidate.osmId() != region.osmId())
                .filter(candidate -> candidate.adminLevel() < region.adminLevel())
                .sorted(Comparator
                        .comparingInt(RegionInfo::adminLevel).reversed()
                        .thenComparingLong(RegionInfo::totalCells))
                .filter(candidate -> {
                    Geometry candidateGeometry = geometry(candidate.osmId(), tmpRegionGeom);
                    return candidateGeometry != null && candidateGeometry.covers(point);
                })
                .map(RegionInfo::osmId)
                .findFirst()
                .orElse(null);
    }

    private Geometry geometry(long osmId, RocksDB tmpRegionGeom) {
        Geometry cached = geometryCache.get(osmId);
        if (cached != null) {
            return cached;
        }
        try {
            byte[] wkb = tmpRegionGeom.get(longToBytes(osmId));
            if (wkb == null || wkb.length == 0) {
                return null;
            }
            Geometry geometry = wkbReader.read(wkb);
            if (geometry == null || geometry.isEmpty()) {
                return null;
            }
            if (!geometry.isValid()) {
                geometry = geometry.buffer(0);
            }
            geometryCache.put(osmId, geometry);
            return geometry;
        } catch (Exception e) {
            log.debug("Could not read geometry for OSM ID {}: {}", osmId, e.getMessage());
            return null;
        }
    }

    private byte[] longToBytes(long value) {
        return ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(value).array();
    }

    private Map<Long, Integer> countChildren(Map<Long, Long> parentByChild) {
        Map<Long, Integer> childCounts = new HashMap<>();
        for (Long parentId : parentByChild.values()) {
            childCounts.merge(parentId, 1, Integer::sum);
        }
        return childCounts;
    }

    private int depth(long osmId, Map<Long, Long> parentByChild, Map<Long, Integer> depths) {
        Integer cached = depths.get(osmId);
        if (cached != null) {
            return cached;
        }
        Long parentId = parentByChild.get(osmId);
        int depth = parentId == null ? 0 : depth(parentId, parentByChild, depths) + 1;
        depths.put(osmId, depth);
        return depth;
    }

    private Continent resolveContinent(long osmId,
                                       Map<Long, Long> parentByChild,
                                       Map<Long, String> isoCodes,
                                       RocksDB tmpRegionGeom) {
        Set<Long> visited = new HashSet<>();
        Long current = osmId;
        while (current != null && visited.add(current)) {
            String continent = ContinentReference.continentForIso(isoCodes.get(current));
            if (continent != null) {
                return new Continent(continent, false);
            }
            current = parentByChild.get(current);
        }
        return new Continent(continentForGeometry(osmId, tmpRegionGeom), true);
    }

    private String continentForGeometry(long osmId, RocksDB tmpRegionGeom) {
        Geometry geometry = geometry(osmId, tmpRegionGeom);
        if (geometry == null) {
            return ContinentReference.UNKNOWN;
        }
        Point point;
        try {
            point = geometry.getInteriorPoint();
        } catch (Exception e) {
            point = geometry.getCentroid();
        }
        return ContinentReference.continentForPoint(point);
    }

    private record RegionInfo(long osmId, int adminLevel, int resolution, long totalCells) {
    }

    private record Continent(String code, boolean fromGeometry) {
    }
}
