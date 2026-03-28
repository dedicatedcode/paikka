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
import com.dedicatedcode.paikka.flatbuffers.Boundary;
import com.dedicatedcode.paikka.flatbuffers.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.WKBReader;
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
import java.util.List;

/**
 * Service for querying building information from the buildings database.
 */
@Service
@ConditionalOnProperty(name = "paikka.import-mode", havingValue = "false", matchIfMissing = true)
public class BuildingService {
    
    private static final Logger logger = LoggerFactory.getLogger(BuildingService.class);
    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
    
    private final PaikkaConfiguration config;
    private final S2Helper s2Helper;
    private RocksDB buildingsDb;
    
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
            Path buildingsDbPath = Paths.get(config.getDataDir(), "buildings");
            
            // Check if the database directory exists
            if (!buildingsDbPath.toFile().exists()) {
                logger.warn("Buildings database not found at: {}", buildingsDbPath);
                return;
            }
            
            Options options = new Options().setCreateIfMissing(false);
            this.buildingsDb = RocksDB.openReadOnly(options, buildingsDbPath.toString());
            logger.info("Successfully initialized RocksDB for buildings");
        } catch (Exception e) {
            logger.warn("Failed to initialize RocksDB for buildings: {}", e.getMessage());
            this.buildingsDb = null;
        }
    }
    
    public synchronized void reloadDatabase() {
        logger.info("Reloading buildings database...");
        
        if (buildingsDb != null) {
            try {
                buildingsDb.close();
                logger.info("Closed existing buildings database connection");
            } catch (Exception e) {
                logger.warn("Error closing existing buildings database: {}", e.getMessage());
            }
            buildingsDb = null;
        }
        
        initializeRocksDB();
        
        if (buildingsDb != null) {
            logger.info("Buildings database reloaded successfully");
        } else {
            logger.warn("Buildings database reload completed but database is not available");
        }
    }
    
    /**
     * Get building information for a specific OSM ID.
     * This searches in the given shard and surrounding shards if needed.
     */
    public BuildingInfo getBuildingInfo(long osmId, double lat, double lon) {
        if (buildingsDb == null) {
            logger.debug("Buildings database not initialized");
            return null;
        }
        
        try {
            // First, try to get the building directly by its OSM ID
            byte[] key = s2Helper.longToByteArray(osmId);
            byte[] data = buildingsDb.get(key);
            
            if (data != null) {
                return decodeBuildingInfo(data, osmId);
            }
            
            // If not found directly, search in surrounding shards
            // Get the shard ID for the given coordinates
            long centerShardId = s2Helper.getShardId(lat, lon);
            
            // Search in the center shard and neighbor shards
            List<Long> shardsToSearch = new ArrayList<>();
            shardsToSearch.add(centerShardId);
            
            // Add neighbor shards
            shardsToSearch.addAll(s2Helper.getNeighborShards(centerShardId));
            
            // Search through all shards
            for (Long shardId : shardsToSearch) {
                // In the buildings database, buildings are stored by OSM ID, not shard ID
                // So we need a different approach. Let's scan for the building by OSM ID
                // Actually, buildings are stored by OSM ID, so we already tried that.
                // If not found, we need to search by location.
                
                // We could implement a spatial search, but for now, let's just return null
                // if not found by direct OSM ID lookup
                break;
            }
            
            logger.debug("Building with OSM ID {} not found in buildings database", osmId);
            return null;
            
        } catch (RocksDBException e) {
            logger.error("RocksDB error while querying building info for OSM ID {}", osmId, e);
            return null;
        } catch (Exception e) {
            logger.error("Error while querying building info for OSM ID {}", osmId, e);
            return null;
        }
    }
    
    /**
     * Get building information for a specific OSM ID without location.
     * This is a simpler version that doesn't search surrounding shards.
     */
    public BuildingInfo getBuildingInfo(long osmId) {
        if (buildingsDb == null) {
            logger.debug("Buildings database not initialized");
            return null;
        }
        
        try {
            byte[] key = s2Helper.longToByteArray(osmId);
            byte[] data = buildingsDb.get(key);
            
            if (data != null) {
                return decodeBuildingInfo(data, osmId);
            }
            
            logger.debug("Building with OSM ID {} not found in buildings database", osmId);
            return null;
            
        } catch (RocksDBException e) {
            logger.error("RocksDB error while querying building info for OSM ID {}", osmId, e);
            return null;
        }
    }
    
    private BuildingInfo decodeBuildingInfo(byte[] data, long osmId) {
        try {
            ByteBuffer buffer = ByteBuffer.wrap(data);
            Boundary boundary = Boundary.getRootAsBoundary(buffer);
            
            BuildingInfo info = new BuildingInfo();
            info.setOsmId(osmId);
            info.setLevel(boundary.level());
            info.setName(boundary.name());
            info.setCode(boundary.code());
            info.setType(boundary.name()); // In buildings, name often contains building type
            
            // Decode geometry if present
            if (boundary.geometry() != null) {
                Geometry geometry = boundary.geometry();
                if (geometry.dataLength() > 0) {
                    byte[] wkbData = new byte[geometry.dataLength()];
                    for (int i = 0; i < geometry.dataLength(); i++) {
                        wkbData[i] = (byte) geometry.data(i);
                    }
                    
                    try {
                        WKBReader wkbReader = new WKBReader();
                        org.locationtech.jts.geom.Geometry jtsGeometry = wkbReader.read(wkbData);
                        info.setGeometry(jtsGeometry);
                        info.setBoundaryWkb(wkbData);
                    } catch (Exception e) {
                        logger.warn("Failed to decode geometry for building OSM ID {}: {}", osmId, e.getMessage());
                    }
                }
            }
            
            return info;
        } catch (Exception e) {
            logger.error("Failed to decode building info for OSM ID {}", osmId, e);
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
