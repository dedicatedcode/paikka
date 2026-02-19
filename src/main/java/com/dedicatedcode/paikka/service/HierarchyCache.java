package com.dedicatedcode.paikka.service;

import com.dedicatedcode.paikka.flatbuffers.Boundary;
import com.dedicatedcode.paikka.flatbuffers.Geometry;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.geom.prep.PreparedGeometryFactory;
import org.locationtech.jts.index.strtree.STRtree;
import org.locationtech.jts.io.WKBReader;
import org.rocksdb.RocksDB;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * Optimized Spatial Cache for OSM Administrative Hierarchies.
 * Uses a 'Candidate Count' guard to ensure sub-level boundaries (like Level 10/11)
 * are never missed when moving between POIs.
 */
public class HierarchyCache {

    private final STRtree globalIndex;
    private final RocksDB boundariesDb;
    private final S2Helper s2Helper;
    private final WKBReader wkbReader = new WKBReader();

    // Cache State
    private List<CachedBoundary> lastHierarchy = null;
    private Envelope lastEnvelope = null;
    private int lastCandidateCount = -1;

    public HierarchyCache(STRtree globalIndex, RocksDB boundariesDb, S2Helper s2Helper) {
        this.globalIndex = globalIndex;
        this.boundariesDb = boundariesDb;
        this.s2Helper = s2Helper;
    }

    public List<SimpleHierarchyItem> resolve(Point point) {
        // 1. FAST PATH: Envelope Check + Candidate Guard
        if (lastEnvelope != null && lastEnvelope.contains(point.getCoordinate())) {

            // CRITICAL FIX: Even if in envelope, check if the INDEX sees more items now
            // (e.g., we just walked into a small Level 10 boundary)
            if (getBoundaryCandidateCount(point) == lastCandidateCount) {
                if (matchesAll(lastHierarchy, point)) {
                    return extractInfo(lastHierarchy);
                }
            }
        }

        // 2. SLOW PATH: Full Re-calculation
        return refreshCache(point);
    }

    private List<SimpleHierarchyItem> refreshCache(Point point) {
        // 1. Get ALL overlapping candidates from the index (the bounding boxes)
        @SuppressWarnings("unchecked")
        List<Long> allCandidateIds = globalIndex.query(point.getEnvelopeInternal());

        // 2. Filter these candidates to find which ones ACTUALLY contain the point (the polygons)
        List<CachedBoundary> newHierarchy = new ArrayList<>();
        for (Long id : allCandidateIds) {
            CachedBoundary cb = fetchAndPrepare(id, point);
            if (cb != null) {
                newHierarchy.add(cb);
            }
        }

        if (!newHierarchy.isEmpty()) {
            this.lastHierarchy = newHierarchy;
            this.lastCandidateCount = allCandidateIds.size();

            // 3. THE FIX: The Safe Zone is the intersection of the boundaries we ARE in...
            Envelope safeEntry = calculateIntersectionEnvelope(newHierarchy);

            // ...MINUS the proximity of any boundary we ARE NOT in.
            // To keep it simple and fast, if we are in an area where
            // other boundaries are nearby, we must shrink the safe zone
            // to not touch the bounding boxes of those other boundaries.
            this.lastEnvelope = computeTrueSafeZone(safeEntry, allCandidateIds, point);
        } else {
            clear();
        }

        return extractInfo(newHierarchy);
    }

    private Envelope computeTrueSafeZone(Envelope intersection, List<Long> currentCandidates, Point point) {
        // We want to find the distance to the nearest 'neighbor' boundary
        // that we are NOT currently inside of.

        // Start with the intersection of our current hierarchy
        Envelope safe = new Envelope(intersection);

        // For every boundary that the index said was "near" but fetchAndPrepare said "not inside":
        // We should technically not trust the area near their borders.
        // Optimization: If the currentCandidates match our hierarchy size,
        // the current intersection is perfectly safe.
        if (currentCandidates.size() == lastHierarchy.size()) {
            return safe;
        }

        // If there are more candidates in the index than in our hierarchy,
        // we are in a "dangerous" zone. We reduce the safe envelope to a tiny
        // radius around the current point to force a refresh very soon.
        double tinyBuffer = 0.0001; // Approx 10 meters
        return new Envelope(
                point.getX() - tinyBuffer, point.getX() + tinyBuffer,
                point.getY() - tinyBuffer, point.getY() + tinyBuffer
        );
    }

    private int getBoundaryCandidateCount(Point p) {
        // This is extremely fast (O(log n) in the R-Tree)
        return globalIndex.query(p.getEnvelopeInternal()).size();
    }

    private List<CachedBoundary> findHierarchyFromIndex(Point poiPoint) {
        List<CachedBoundary> hierarchy = new ArrayList<>();
        // Query index for Bounding Box overlaps
        @SuppressWarnings("unchecked")
        List<Long> candidates = globalIndex.query(poiPoint.getEnvelopeInternal());

        for (Long boundaryId : candidates) {
            CachedBoundary cb = fetchAndPrepare(boundaryId, poiPoint);
            if (cb != null) {
                hierarchy.add(cb);
            }
        }

        // Sort: Country (2) -> State (4) -> City (8) -> Neighborhood (10/11)
        hierarchy.sort(Comparator.comparingInt(CachedBoundary::level));
        return hierarchy;
    }

    private CachedBoundary fetchAndPrepare(long id, Point p) {
        try {
            byte[] data = boundariesDb.get(s2Helper.longToByteArray(id));
            if (data == null) return null;

            Boundary b = Boundary.getRootAsBoundary(ByteBuffer.wrap(data));
            Geometry fbGeom = b.geometry();

            // Optimization: Quick BBox check before WKB parsing
            // (Assumes you stored the extent in the FlatBuffer for speed)

            byte[] wkb = new byte[fbGeom.dataLength()];
            fbGeom.dataAsByteBuffer().get(wkb);
            org.locationtech.jts.geom.Geometry jtsGeom = wkbReader.read(wkb);

            if (jtsGeom.contains(p)) {
                return new CachedBoundary(
                        b.level(),
                        b.name(),
                        b.osmId(),
                        PreparedGeometryFactory.prepare(jtsGeom)
                );
            }
        } catch (Exception ignored) {}
        return null;
    }

    private Envelope calculateIntersectionEnvelope(List<CachedBoundary> hierarchy) {
        Envelope res = new Envelope();
        for (CachedBoundary cb : hierarchy) {
            Envelope env = cb.geometry.getGeometry().getEnvelopeInternal();
            if (res.isNull()) res.init(env);
            else res.intersection(env);
        }
        return res;
    }

    private boolean matchesAll(List<CachedBoundary> hierarchy, Point p) {
        if (hierarchy == null) return false;
        for (CachedBoundary b : hierarchy) {
            if (!b.geometry.contains(p)) return false;
        }
        return true;
    }

    public void clear() {
        this.lastHierarchy = null;
        this.lastEnvelope = null;
        this.lastCandidateCount = -1;
    }

    private List<SimpleHierarchyItem> extractInfo(List<CachedBoundary> h) {
        return h.stream()
                .map(b -> new SimpleHierarchyItem(b.level, "administrative", b.name, b.osmId))
                .toList();
    }

    private record CachedBoundary(int level, String name, long osmId, PreparedGeometry geometry) {}
    public record SimpleHierarchyItem(int level, String type, String name, long osmId) {}
}