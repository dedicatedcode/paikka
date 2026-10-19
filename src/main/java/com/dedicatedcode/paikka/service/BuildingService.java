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
import com.dedicatedcode.paikka.flatbuffers.Building;
import com.dedicatedcode.paikka.flatbuffers.BuildingList;
import com.dedicatedcode.paikka.flatbuffers.Geometry;
import jakarta.annotation.PreDestroy;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.WKBReader;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.Cache;
import org.rocksdb.Filter;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Service for querying building information from the buildings database.
 */
@Service
@ConditionalOnProperty(name = "paikka.import-mode", havingValue = "false", matchIfMissing = true)
public class BuildingService {
    
    private static final Logger logger = LoggerFactory.getLogger(BuildingService.class);
    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
    
    private static final long MEGABYTE = 1024L * 1024L;
    private static final long BLOCK_SIZE = 64L * 1024L;

    private final PaikkaConfiguration config;
    private final S2Helper s2Helper;
    private RocksDB buildingsDb;
    private Options buildingsOptions;
    private Cache blockCache;
    private Filter filterPolicy;

    public BuildingService(PaikkaConfiguration config, S2Helper s2Helper) {
        this.config = config;
        this.s2Helper = s2Helper;
        initializeRocksDB();
    }
    
    private void initializeRocksDB() {
        if (buildingsDb != null) {
            return;
        }
        
        try {
            RocksDB.loadLibrary();
            Path buildingsDbPath = Paths.get(config.getDataDir(), "buildings_shards");
            
            // Check if the database directory exists
            if (!buildingsDbPath.toFile().exists()) {
                logger.warn("Buildings database not found at: {}", buildingsDbPath);
                return;
            }

            long cacheSize = config.getRocksDbConfiguration().getBuildingsBlockCacheSizeMb() * MEGABYTE;
            this.blockCache = new LRUCache(cacheSize);
            this.filterPolicy = new BloomFilter(10, false);
            BlockBasedTableConfig tableConfig = new BlockBasedTableConfig()
                    .setBlockCache(blockCache)
                    .setFilterPolicy(filterPolicy)
                    .setBlockSize(BLOCK_SIZE)
                    .setCacheIndexAndFilterBlocks(true)
                    .setPinL0FilterAndIndexBlocksInCache(true)
                    .setWholeKeyFiltering(true);
            this.buildingsOptions = new Options()
                    .setCreateIfMissing(false)
                    .setTableFormatConfig(tableConfig)
                    .setMaxOpenFiles(-1);
            this.buildingsDb = RocksDB.openReadOnly(buildingsOptions, buildingsDbPath.toString());
            logger.info("Successfully initialized RocksDB for buildings (block cache: {} MB)", cacheSize / MEGABYTE);
        } catch (Exception e) {
            logger.warn("Failed to initialize RocksDB for buildings: {}", e.getMessage());
            closeResources();
        }
    }
    
    public synchronized void reloadDatabase() {
        logger.info("Reloading buildings database...");
        closeResources();
        initializeRocksDB();
        
        if (buildingsDb != null) {
            logger.info("Buildings database reloaded successfully");
        } else {
            logger.warn("Buildings database reload completed but database is not available");
        }
    }

    @PreDestroy
    public synchronized void closeDatabase() {
        closeResources();
    }

    private void closeResources() {
        if (buildingsDb != null) {
            try {
                buildingsDb.close();
                logger.info("Closed existing buildings database connection");
            } catch (Exception e) {
                logger.warn("Error closing existing buildings database: {}", e.getMessage());
            }
            buildingsDb = null;
        }
        if (buildingsOptions != null) {
            try {
                buildingsOptions.close();
            } catch (Exception e) {
                logger.warn("Error closing buildings database options: {}", e.getMessage());
            }
            buildingsOptions = null;
        }
        if (filterPolicy != null) {
            try {
                filterPolicy.close();
            } catch (Exception e) {
                logger.warn("Error closing buildings filter policy: {}", e.getMessage());
            }
            filterPolicy = null;
        }
        if (blockCache != null) {
            try {
                blockCache.close();
            } catch (Exception e) {
                logger.warn("Error closing buildings block cache: {}", e.getMessage());
            }
            blockCache = null;
        }
    }
    
    /**
     * Creates a request-scoped lookup context. Reuse it for all results of one
     * reverse geocoding request so each building shard is read and indexed once.
     */
    public BuildingLookupContext newLookupContext() {
        return new BuildingLookupContext(
                config.getQueryConfiguration().getBuildingIndexMaxShards(),
                config.getQueryConfiguration().getBuildingIndexMaxBytes());
    }

    /**
     * Get building information for a point (lat, lon) by checking if it is contained in a building.
     * This searches in the given shard and surrounding shards if needed.
     */
    public BuildingInfo getBuildingInfo(double lat, double lon) {
        try (BuildingLookupContext context = newLookupContext()) {
            return getBuildingInfo(lat, lon, context);
        }
    }

    /**
     * Get building information using a shared request-scoped lookup context.
     */
    public BuildingInfo getBuildingInfo(double lat, double lon, BuildingLookupContext context) {
        if (buildingsDb == null) {
            logger.debug("Buildings database not initialized");
            return null;
        }

        try {
            Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(lon, lat));
            long centerShardId = s2Helper.getShardId(lat, lon);
            List<Long> shardsToSearch = new ArrayList<>();
            shardsToSearch.add(centerShardId);
            shardsToSearch.addAll(s2Helper.getNeighborShards(centerShardId));

            for (Long shardId : shardsToSearch) {
                BuildingShardIndex shard = context.get(shardId);
                if (shard == null) {
                    continue;
                }
                Building building = shard.findContaining(point);
                if (building != null) {
                    return decodeBuildingInfo(building);
                }
            }

            logger.debug("Building containing point ({}, {}) not found in buildings database", lon, lat);
            return null;

        } catch (RocksDBException e) {
            logger.error("RocksDB error while querying building info for point ({}, {})", lon, lat, e);
            return null;
        } catch (Exception e) {
            logger.error("Error while querying building info for point ({}, {})", lon, lat, e);
            return null;
        }
    }

    /**
     * Request-scoped set of parsed building shards.
     */
    public final class BuildingLookupContext implements AutoCloseable {

        private final int maxShards;
        private final long maxBytes;
        private final Map<Long, BuildingShardIndex> shards = new LinkedHashMap<>(16, 0.75f, true);
        private long indexedBytes;
        private int loadedShards;
        private int indexedBuildings;

        private BuildingLookupContext(int maxShards, long maxBytes) {
            this.maxShards = Math.max(1, maxShards);
            this.maxBytes = Math.max(1024L * 1024L, maxBytes);
        }

        private BuildingShardIndex get(long shardId) throws RocksDBException {
            BuildingShardIndex cached = shards.get(shardId);
            if (cached != null) {
                return cached;
            }

            byte[] data = buildingsDb.get(s2Helper.longToByteArray(shardId));
            if (data == null) {
                return null;
            }

            BuildingShardIndex index = BuildingShardIndex.build(data);
            loadedShards++;
            indexedBuildings += index.count();

            if (index.estimatedBytes() <= maxBytes) {
                while (!shards.isEmpty()
                        && (shards.size() >= maxShards || indexedBytes + index.estimatedBytes() > maxBytes)) {
                    Iterator<Map.Entry<Long, BuildingShardIndex>> iterator = shards.entrySet().iterator();
                    Map.Entry<Long, BuildingShardIndex> eldest = iterator.next();
                    iterator.remove();
                    indexedBytes -= eldest.getValue().estimatedBytes();
                }
                shards.put(shardId, index);
                indexedBytes += index.estimatedBytes();
            }

            return index;
        }

        public int getLoadedShards() {
            return loadedShards;
        }

        public int getIndexedBuildings() {
            return indexedBuildings;
        }

        @Override
        public void close() {
            shards.clear();
            indexedBytes = 0;
        }
    }

    private static final class BuildingShardIndex {

        private final byte[] data;
        private final BuildingList buildingList;
        private final int count;
        private final double[] envelopes;
        private final boolean[] unsupported;
        private final long estimatedBytes;

        private BuildingShardIndex(byte[] data,
                                   BuildingList buildingList,
                                   int count,
                                   double[] envelopes,
                                   boolean[] unsupported,
                                   long estimatedBytes) {
            this.data = data;
            this.buildingList = buildingList;
            this.count = count;
            this.envelopes = envelopes;
            this.unsupported = unsupported;
            this.estimatedBytes = estimatedBytes;
        }

        private static BuildingShardIndex build(byte[] data) {
            BuildingList buildingList = BuildingList.getRootAsBuildingList(ByteBuffer.wrap(data));
            int count = buildingList.buildingsLength();
            double[] envelopes = new double[count * 4];
            boolean[] unsupported = new boolean[count];
            Building reusableBuilding = new Building();
            Geometry reusableGeometry = new Geometry();

            for (int i = 0; i < count; i++) {
                Building building = buildingList.buildings(reusableBuilding, i);
                if (building == null) {
                    unsupported[i] = true;
                    continue;
                }
                Geometry geometry = building.geometry(reusableGeometry);
                if (geometry == null || geometry.dataLength() == 0) {
                    unsupported[i] = true;
                    continue;
                }
                ByteBuffer wkb = geometry.dataAsByteBuffer();
                if (wkb == null || !WkbEnvelope.read(wkb, envelopes, i * 4)) {
                    unsupported[i] = true;
                }
            }

            long estimatedBytes = data.length
                    + (long) count * 4L * Double.BYTES
                    + count
                    + 256L;
            return new BuildingShardIndex(data, buildingList, count, envelopes, unsupported, estimatedBytes);
        }

        private Building findContaining(Point point) {
            double x = point.getX();
            double y = point.getY();
            Building reusableBuilding = new Building();
            Geometry reusableGeometry = new Geometry();

            for (int i = 0; i < count; i++) {
                if (!unsupported[i]) {
                    int offset = i * 4;
                    if (x < envelopes[offset]
                            || x > envelopes[offset + 2]
                            || y < envelopes[offset + 1]
                            || y > envelopes[offset + 3]) {
                        continue;
                    }
                }

                Building building = buildingList.buildings(reusableBuilding, i);
                if (building == null) {
                    continue;
                }
                Geometry geometry = building.geometry(reusableGeometry);
                if (geometry == null || geometry.dataLength() == 0) {
                    continue;
                }

                try {
                    byte[] wkb = copyGeometry(geometry);
                    org.locationtech.jts.geom.Geometry jtsGeometry = new WKBReader().read(wkb);
                    if (jtsGeometry.intersects(point)) {
                        return building;
                    }
                } catch (Exception e) {
                    logger.warn("Failed to read geometry for building {}", building.id(), e);
                }
            }
            return null;
        }

        private int count() {
            return count;
        }

        private long estimatedBytes() {
            return estimatedBytes;
        }

        private static byte[] copyGeometry(Geometry geometry) {
            int length = geometry.dataLength();
            byte[] wkb = new byte[length];
            ByteBuffer buffer = geometry.dataAsByteBuffer();
            if (buffer != null) {
                buffer.get(wkb);
            } else {
                for (int i = 0; i < length; i++) {
                    wkb[i] = (byte) geometry.data(i);
                }
            }
            return wkb;
        }
    }
    
    private BuildingInfo decodeBuildingInfo(com.dedicatedcode.paikka.flatbuffers.Building building) {
        try {
            BuildingInfo info = new BuildingInfo();
            info.setOsmId(building.id());
            info.setLevel(100);
            info.setName(building.name());
            info.setCode(building.code());
            info.setType(building.name());

            // Decode geometry if present
            if (building.geometry() != null) {
                Geometry geometry = building.geometry();
                if (geometry.dataLength() > 0) {
                    byte[] wkbData = new byte[geometry.dataLength()];
                    ByteBuffer geometryBuffer = geometry.dataAsByteBuffer();
                    if (geometryBuffer != null) {
                        geometryBuffer.get(wkbData);
                    } else {
                        for (int i = 0; i < geometry.dataLength(); i++) {
                            wkbData[i] = (byte) geometry.data(i);
                        }
                    }

                    try {
                        WKBReader wkbReader = new WKBReader();
                        org.locationtech.jts.geom.Geometry jtsGeometry = wkbReader.read(wkbData);
                        info.setGeometry(jtsGeometry);
                        info.setBoundaryWkb(wkbData);
                    } catch (Exception e) {
                        logger.warn("Failed to decode geometry for building OSM ID {}: {}", building.id(), e.getMessage());
                    }
                }
            }

            return info;
        } catch (Exception e) {
            logger.error("Failed to decode building info for OSM ID {}", building.id(), e);
            return null;
        }
    }
    
    /**
     * Check if the buildings database is available.
     */
    public boolean isAvailable() {
        return buildingsDb != null;
    }
    
    /**
     * Represents building information retrieved from the buildings database.
     */
    public static class BuildingInfo {
        private long osmId;
        private int level;
        private String name;
        private String code;
        private String type;
        private org.locationtech.jts.geom.Geometry geometry;
        private byte[] boundaryWkb;
        
        public long getOsmId() {
            return osmId;
        }
        
        public void setOsmId(long osmId) {
            this.osmId = osmId;
        }
        
        public int getLevel() {
            return level;
        }
        
        public void setLevel(int level) {
            this.level = level;
        }
        
        public String getName() {
            return name;
        }
        
        public void setName(String name) {
            this.name = name;
        }
        
        public String getCode() {
            return code;
        }
        
        public void setCode(String code) {
            this.code = code;
        }
        
        public String getType() {
            return type;
        }
        
        public void setType(String type) {
            this.type = type;
        }
        
        public org.locationtech.jts.geom.Geometry getGeometry() {
            return geometry;
        }
        
        public void setGeometry(org.locationtech.jts.geom.Geometry geometry) {
            this.geometry = geometry;
        }
        
        public byte[] getBoundaryWkb() {
            return boundaryWkb;
        }
        
        public void setBoundaryWkb(byte[] boundaryWkb) {
            this.boundaryWkb = boundaryWkb;
        }
    }
}
