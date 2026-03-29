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
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
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
            Path buildingsDbPath = Paths.get(config.getDataDir(), "buildings_shards");
            
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
     * Get building information for a point (lat, lon) by checking if it is contained in a building.
     * This searches in the given shard and surrounding shards if needed.
     */
    public BuildingInfo getBuildingInfo(long osmId, double lat, double lon) {
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
                byte[] key = s2Helper.longToByteArray(shardId);
                byte[] data = buildingsDb.get(key);

                if (data != null) {
                    ByteBuffer buffer = ByteBuffer.wrap(data);
                    com.dedicatedcode.paikka.flatbuffers.BuildingList buildingList = com.dedicatedcode.paikka.flatbuffers.BuildingList.getRootAsBuildingList(buffer);
                    for (int i = 0; i < buildingList.buildingsLength(); i++) {
                        com.dedicatedcode.paikka.flatbuffers.Building building = buildingList.buildings(i);
                        if (building != null && building.geometry() != null) {
                            Geometry geometryFb = building.geometry();
                            if (geometryFb.dataLength() > 0) {
                                byte[] wkbData = new byte[geometryFb.dataLength()];
                                for (int j = 0; j < geometryFb.dataLength(); j++) {
                                    wkbData[j] = (byte) geometryFb.data(j);
                                }
                                try {
                                    WKBReader wkbReader = new WKBReader();
                                    org.locationtech.jts.geom.Geometry jtsGeometry = wkbReader.read(wkbData);
                                    if (jtsGeometry.contains(point)) {
                                        return decodeBuildingInfo(building);
                                    }
                                } catch (Exception e) {
                                    logger.warn("Failed to read geometry for building {}", building.id(), e);
                                }
                            }
                        }
                    }
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
    
    private BuildingInfo decodeBuildingInfo(com.dedicatedcode.paikka.flatbuffers.Building building) {
        try {
            BuildingInfo info = new BuildingInfo();
            info.setOsmId(building.id());
            info.setLevel(100); // Buildings don't have an admin_level, using a high value.
            info.setName(building.name());
            info.setCode(building.code());
            info.setType(building.name()); // In buildings, name often contains building type

            // Decode geometry if present
            if (building.geometry() != null) {
                Geometry geometry = building.geometry();
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
