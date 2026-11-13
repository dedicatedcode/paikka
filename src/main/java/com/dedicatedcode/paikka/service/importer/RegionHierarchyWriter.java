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

    public HierarchyResult write(Path output,
                                 RocksDB tmpRegionMeta,
                                 RocksDB tmpRegionGeom,
                                 RocksDB h3ToOsm,
                                 H3Core h3,
                                 Map<Long, String> isoCodes,
                                 Map<Long, List<Long>> subareaEdges,
                                 Map<Long, StandaloneBoundaryImporter.CountryHints> countryHints,
                                 Map<String, Long> regionByIso3166_2,
                                 Map<String, String> countryIsoByName,
                                 Map<Long, StandaloneBoundaryImporter.CountryStub> countryStubs,
                                 BoundaryImportStatistics stats) throws IOException, RocksDBException {
        Map<Long, RegionInfo> regions = readRegions(tmpRegionMeta);
        Set<Long> placeholderCountries = new HashSet<>();
        for (StandaloneBoundaryImporter.CountryStub country : countryStubs.values()) {
            if (!regions.containsKey(country.osmId())) {
                regions.put(country.osmId(), new RegionInfo(country.osmId(), 2, 0, 0));
                placeholderCountries.add(country.osmId());
                tmpRegionMeta.put(longToBytes(country.osmId()),
                        ByteBuffer.allocate(16).order(ByteOrder.BIG_ENDIAN)
                                .putLong(0L).putInt(0).putInt(2).array());
            }
        }
        stats.startHierarchy(regions.size());
        Map<Long, Long> parentByChild = resolveParents(regions, subareaEdges, tmpRegionGeom, h3ToOsm, h3, stats);

        Map<String, Long> countryRootByIso = new HashMap<>();
        Map<Long, String> countryByRegion = new HashMap<>();
        for (RegionInfo region : regions.values()) {
            String iso = isoCodes.get(region.osmId());
            if (region.adminLevel() == 2 && iso != null) {
                countryRootByIso.put(iso, region.osmId());
                countryByRegion.put(region.osmId(), iso);
            }
        }

        // Hints must be visible before the inheritance pass. Otherwise a region whose country comes
        // from a hint (for example a federal subject without a resolvable parent) cannot pass that
        // country on to its children, and the whole subtree is dropped even though it was assigned.
        Map<Long, String> hintedCountries = new HashMap<>();
        for (RegionInfo region : regions.values()) {
            if (countryByRegion.containsKey(region.osmId())) {
                continue;
            }
            String iso = countryFromHints(countryHints.get(region.osmId()), countryRootByIso, countryIsoByName);
            if (iso != null) {
                hintedCountries.put(region.osmId(), iso);
            }
        }

        inheritCountries(regions.values(), parentByChild, countryByRegion, hintedCountries);
        for (Map.Entry<Long, String> hint : hintedCountries.entrySet()) {
            countryByRegion.putIfAbsent(hint.getKey(), hint.getValue());
        }

        // Last resort: if the tree cannot prove a country but the region's interior point lies in a
        // country polygon we do have, keep the region instead of silently dropping it.
        int geometryCountryFallbacks = assignCountriesByGeometry(regions.values(), countryByRegion,
                                                                 tmpRegionGeom, countryRootByIso);
        if (geometryCountryFallbacks > 0) {
            // Descendants of a region assigned by geometry may still be missing a country.
            inheritCountries(regions.values(), parentByChild, countryByRegion, Map.of());
        }
        if (!hintedCountries.isEmpty() || geometryCountryFallbacks > 0) {
            log.info("Country assignment: {} hinted regions, {} geometry fallbacks",
                     hintedCountries.size(), geometryCountryFallbacks);
        }

        Map<Long, Long> repairedParents = repairParents(regions, parentByChild, countryByRegion,
                                                        countryRootByIso, countryHints, regionByIso3166_2);
        Set<Long> unassigned = new HashSet<>(regions.keySet());
        unassigned.removeAll(countryByRegion.keySet());

        Map<Long, Integer> childCounts = countChildren(repairedParents);
        Map<Long, Integer> depths = new HashMap<>(regions.size());
        Map<Long, String> continents = new HashMap<>(regions.size());
        int fallbackContinents = 0;
        for (RegionInfo region : regions.values()) {
            if (!countryByRegion.containsKey(region.osmId())) {
                continue;
            }
            depth(region.osmId(), repairedParents, depths);
            Continent continent = resolveContinent(region.osmId(), repairedParents, isoCodes, tmpRegionGeom);
            if (continent.fromGeometry()) {
                fallbackContinents++;
            }
            continents.put(region.osmId(), continent.code());
        }

        List<RegionInfo> sorted = new ArrayList<>(regions.values());
        sorted.sort(Comparator.comparingLong(RegionInfo::osmId));
        int roots = 0;
        try (BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
            for (RegionInfo region : sorted) {
                if (!countryByRegion.containsKey(region.osmId())) {
                    continue;
                }
                Long parentId = repairedParents.get(region.osmId());
                if (parentId == null) {
                    roots++;
                }
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

        writeUnassignedDiagnostics(output.resolveSibling("region_unassigned.tsv"), regions, unassigned, parentByChild);
        stats.setHierarchyResults(roots, stats.getHierarchySpatialParents(), fallbackContinents);
        return new HierarchyResult(regions.size() - unassigned.size(), roots, unassigned,
                                   countryByRegion, placeholderCountries);
    }

    static void inheritCountries(Iterable<RegionInfo> regions,
                                 Map<Long, Long> parentByChild,
                                 Map<Long, String> countryByRegion,
                                 Map<Long, String> hintedCountries) {
        for (RegionInfo region : regions) {
            if (countryByRegion.containsKey(region.osmId())) {
                continue;
            }
            String inherited = inheritedCountry(region.osmId(), parentByChild, countryByRegion, hintedCountries);
            if (inherited != null) {
                countryByRegion.put(region.osmId(), inherited);
            }
        }
    }

    private static String inheritedCountry(long osmId,
                                           Map<Long, Long> parentByChild,
                                           Map<Long, String> countryByRegion,
                                           Map<Long, String> hintedCountries) {
        Set<Long> visited = new HashSet<>();
        Long current = parentByChild.get(osmId);
        while (current != null && visited.add(current)) {
            String country = countryByRegion.get(current);
            if (country == null) {
                country = hintedCountries.get(current);
            }
            if (country != null) {
                return country;
            }
            current = parentByChild.get(current);
        }
        return null;
    }

    /**
     * Assigns otherwise unassigned regions to the country polygon that contains their interior
     * point. This is the fallback for regions whose parent chain is broken because the source
     * extract clipped a boundary; without it such regions are dropped even though they are plainly
     * inside a country.
     */
    int assignCountriesByGeometry(Iterable<RegionInfo> regions,
                                          Map<Long, String> countryByRegion,
                                          RocksDB tmpRegionGeom,
                                          Map<String, Long> countryRootByIso) {
        List<Map.Entry<String, Geometry>> countryGeometries = new ArrayList<>();
        for (Map.Entry<String, Long> country : countryRootByIso.entrySet()) {
            Geometry geometry = geometry(country.getValue(), tmpRegionGeom);
            if (geometry != null) {
                countryGeometries.add(Map.entry(country.getKey(), geometry));
            }
        }
        if (countryGeometries.isEmpty()) {
            return 0;
        }

        int assigned = 0;
        for (RegionInfo region : regions) {
            if (countryByRegion.containsKey(region.osmId())) {
                continue;
            }
            Geometry geometry = geometry(region.osmId(), tmpRegionGeom);
            Point point = interiorPoint(geometry);
            if (point == null) {
                continue;
            }
            for (Map.Entry<String, Geometry> country : countryGeometries) {
                if (!country.getValue().getEnvelopeInternal().contains(point.getCoordinate())) {
                    continue;
                }
                if (country.getValue().covers(point)) {
                    countryByRegion.put(region.osmId(), country.getKey());
                    assigned++;
                    break;
                }
            }
        }
        return assigned;
    }

    private static Point interiorPoint(Geometry geometry) {
        if (geometry == null) {
            return null;
        }
        Point point;
        try {
            point = geometry.getInteriorPoint();
        } catch (Exception e) {
            point = geometry.getCentroid();
        }
        return point == null || point.isEmpty() ? null : point;
    }

    private String countryFromHints(StandaloneBoundaryImporter.CountryHints hints,
                                    Map<String, Long> countryRootByIso,
                                    Map<String, String> countryIsoByName) {
        if (hints == null) {
            return null;
        }
        String iso = null;
        if (hints.iso3166_2() != null) {
            int separator = hints.iso3166_2().indexOf('-');
            if (separator > 0) {
                iso = hints.iso3166_2().substring(0, separator).toUpperCase(java.util.Locale.ROOT);
            }
        }
        if (iso == null && hints.countryCode() != null) {
            iso = hints.countryCode();
        }
        if (iso == null && hints.countryName() != null) {
            iso = countryIsoByName.get(normalizeCountryName(hints.countryName()));
        }
        if (iso == null && hints.countryNameEn() != null) {
            iso = countryIsoByName.get(normalizeCountryName(hints.countryNameEn()));
        }
        return iso != null && countryRootByIso.containsKey(iso) ? iso : null;
    }

    private static String normalizeCountryName(String value) {
        return value == null ? null : value.trim().toLowerCase(java.util.Locale.ROOT);
    }

    private Map<Long, Long> repairParents(Map<Long, RegionInfo> regions,
                                          Map<Long, Long> parentByChild,
                                          Map<Long, String> countryByRegion,
                                          Map<String, Long> countryRootByIso,
                                          Map<Long, StandaloneBoundaryImporter.CountryHints> countryHints,
                                          Map<String, Long> regionByIso3166_2) {
        Map<Long, Long> repaired = new HashMap<>(parentByChild);
        for (RegionInfo region : regions.values()) {
            String country = countryByRegion.get(region.osmId());
            if (country == null) {
                continue;
            }
            if (region.adminLevel() == 2) {
                repaired.remove(region.osmId());
                continue;
            }
            Long parent = repaired.get(region.osmId());
            String parentCountry = parent == null ? null : countryByRegion.get(parent);
            boolean parentValid = parent != null
                    && country.equals(parentCountry)
                    && regions.get(parent).adminLevel() < region.adminLevel();
            if (parentValid) {
                continue;
            }

            Long isoParent = null;
            StandaloneBoundaryImporter.CountryHints hints = countryHints.get(region.osmId());
            if (hints != null && hints.iso3166_2() != null) {
                Long candidate = regionByIso3166_2.get(hints.iso3166_2());
                if (candidate != null && !candidate.equals(region.osmId())) {
                    RegionInfo candidateRegion = regions.get(candidate);
                    if (candidateRegion != null
                            && candidateRegion.adminLevel() < region.adminLevel()
                            && country.equals(countryByRegion.get(candidate))) {
                        isoParent = candidate;
                    }
                }
            }
            Long target = isoParent != null ? isoParent : countryRootByIso.get(country);
            if (target != null && !target.equals(region.osmId())
                    && regions.get(target).adminLevel() < region.adminLevel()) {
                repaired.put(region.osmId(), target);
            } else {
                repaired.remove(region.osmId());
                countryByRegion.remove(region.osmId());
            }
        }
        return repaired;
    }

    private void writeUnassignedDiagnostics(Path output,
                                            Map<Long, RegionInfo> regions,
                                            Set<Long> unassigned,
                                            Map<Long, Long> parentByChild) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
            for (Long osmId : unassigned.stream().sorted().toList()) {
                RegionInfo region = regions.get(osmId);
                writer.write(Long.toString(osmId));
                writer.write('\t');
                writer.write(Integer.toString(region.adminLevel()));
                writer.write('\t');
                writer.write(parentByChild.containsKey(osmId) ? "no-country-in-ancestors" : "no-parent");
                writer.write('\n');
            }
        }
    }

    public record HierarchyResult(int regionsWritten, int roots, Set<Long> unassignedIds,
                                  Map<Long, String> countryByRegion, Set<Long> placeholderCountryIds) {
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
                                           H3Core h3,
                                           BoundaryImportStatistics stats) throws RocksDBException {
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
            if (!parentByChild.containsKey(region.osmId())) {
                Long parentId = findSpatialParent(region, regions, tmpRegionGeom, h3ToOsm, h3);
                if (parentId != null) {
                    parentByChild.put(region.osmId(), parentId);
                    spatialParents++;
                }
            }
            stats.incrementHierarchyRegionsProcessed();
        }
        stats.setHierarchyResults(regions.size() - parentByChild.size(), spatialParents, 0);
        return parentByChild;
    }

    private Long findSpatialParent(RegionInfo region,
                                   Map<Long, RegionInfo> regions,
                                   RocksDB tmpRegionGeom,
                                   RocksDB h3ToOsm,
                                   H3Core h3) throws RocksDBException {
        Geometry geometry = geometry(region.osmId(), tmpRegionGeom);
        Point point = interiorPoint(geometry);
        if (point == null) {
            return null;
        }

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

    record RegionInfo(long osmId, int adminLevel, int resolution, long totalCells) {
    }

    private record Continent(String code, boolean fromGeometry) {
    }
}
