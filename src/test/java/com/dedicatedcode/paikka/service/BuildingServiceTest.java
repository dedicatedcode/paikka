/*
 *  This file is part of paikka.
 *
 *  Paikka is free software: you can redistribute it and/or
 *  modify it under the terms of the GNU Affero General Public License
 *  as published by the Free Software Foundation, either version 3 of
 *  the License, or (at your option) any later version.
 *
 *  Paikka is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *  You should have received a copy of the GNU Affero General Public License
 *  along with Paikka. If not, see <https://www.gnu.org/licenses/>.
 */

package com.dedicatedcode.paikka.service;

import com.dedicatedcode.paikka.config.PaikkaConfiguration;
import com.dedicatedcode.paikka.flatbuffers.Building;
import com.dedicatedcode.paikka.flatbuffers.BuildingList;
import com.dedicatedcode.paikka.flatbuffers.Geometry;
import com.dedicatedcode.paikka.service.geometry.EncodedGeometry;
import com.dedicatedcode.paikka.service.geometry.GeometryCodec;
import com.google.flatbuffers.FlatBufferBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class BuildingServiceTest {

    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

    @TempDir
    Path dataDirectory;

    private BuildingService buildingService;

    @BeforeEach
    void setUp() throws Exception {
        writeBuildingShard();

        PaikkaConfiguration configuration = new PaikkaConfiguration();
        configuration.setDataDir(dataDirectory.toString());
        configuration.setQueryConfiguration(new PaikkaConfiguration.QueryConfiguration());
        PaikkaConfiguration.RocksDbConfiguration rocksDbConfiguration = new PaikkaConfiguration.RocksDbConfiguration();
        rocksDbConfiguration.setBuildingsBlockCacheSizeMb(8);
        configuration.setRocksDbConfiguration(rocksDbConfiguration);

        buildingService = new BuildingService(configuration, new S2Helper());
    }

    @AfterEach
    void tearDown() {
        if (buildingService != null) {
            buildingService.closeDatabase();
        }
    }

    @Test
    void findsBuildingWithEnvelopePrefilter() {
        BuildingService.BuildingInfo info = buildingService.getBuildingInfo(52.0005, 4.0005);

        assertThat(info).isNotNull();
        assertThat(info.getOsmId()).isEqualTo(1);
        assertThat(info.getName()).isEqualTo("first");
        assertThat(info.getGeometry()).isNotNull();
    }

    @Test
    void findsBuildingByIdAndKnownShard() {
        long shardId = new S2Helper().getShardId(52.0005, 4.0005);

        BuildingService.BuildingInfo info = buildingService.getBuildingById(1, shardId);

        assertThat(info).isNotNull();
        assertThat(info.getOsmId()).isEqualTo(1);
        assertThat(info.getGeometry()).isNotNull();
        assertThat(buildingService.getBuildingById(999, shardId)).isNull();
    }

    @Test
    void returnsNullOutsideEveryBuilding() {
        assertThat(buildingService.getBuildingInfo(52.01, 4.01)).isNull();
    }

    @Test
    void sharedContextReturnsTheSameResultAsIndividualLookups() {
        double[][] points = {
                {52.0005, 4.0005},
                {52.0025, 4.0025},
                {52.01, 4.01}
        };

        try (BuildingService.BuildingLookupContext context = buildingService.newLookupContext()) {
            for (double[] point : points) {
                BuildingService.BuildingInfo expected = buildingService.getBuildingInfo(point[0], point[1]);
                BuildingService.BuildingInfo actual = buildingService.getBuildingInfo(point[0], point[1], context);

                if (expected == null) {
                    assertThat(actual).isNull();
                } else {
                    assertThat(actual).isNotNull();
                    assertThat(actual.getOsmId()).isEqualTo(expected.getOsmId());
                    assertThat(actual.getName()).isEqualTo(expected.getName());
                }
            }
        }
    }

    private void writeBuildingShard() throws Exception {
        RocksDB.loadLibrary();
        S2Helper s2Helper = new S2Helper();
        double centerLat = 52.0005;
        double centerLon = 4.0005;
        long shardId = s2Helper.getShardId(centerLat, centerLon);

        byte[] buildingList = createBuildingList();
        Path shardDirectory = dataDirectory.resolve("buildings_shards");
        try (Options options = new Options().setCreateIfMissing(true);
             RocksDB db = RocksDB.open(options, shardDirectory.toString())) {
            db.put(s2Helper.longToByteArray(shardId), buildingList);
        }
    }

    private byte[] createBuildingList() {
        FlatBufferBuilder builder = new FlatBufferBuilder();
        int first = createBuilding(builder, 1, "first", polygon(4.000, 52.000, 4.001, 52.001));
        int second = createBuilding(builder, 2, "second", polygon(4.010, 52.010, 4.011, 52.011));
        int buildings = BuildingList.createBuildingsVector(builder, new int[]{first, second});
        int list = BuildingList.createBuildingList(builder, buildings);
        builder.finish(list);
        return builder.sizedByteArray();
    }

    private int createBuilding(FlatBufferBuilder builder, long id, String name, org.locationtech.jts.geom.Geometry geometry) {
        int nameOffset = builder.createString(name);
        int codeOffset = builder.createString("building");
        EncodedGeometry encoded = GeometryCodec.encode(geometry);
        int geometryData = Geometry.createDataVector(builder, encoded.data());
        int geometryOffset = Geometry.createGeometry(builder, geometryData, encoded.format());

        Building.startBuilding(builder);
        Building.addId(builder, id);
        Building.addName(builder, nameOffset);
        Building.addCode(builder, codeOffset);
        Building.addGeometry(builder, geometryOffset);
        return Building.endBuilding(builder);
    }

    private org.locationtech.jts.geom.Geometry polygon(double minLon, double minLat, double maxLon, double maxLat) {
        Coordinate[] coordinates = {
                new Coordinate(minLon, minLat),
                new Coordinate(maxLon, minLat),
                new Coordinate(maxLon, maxLat),
                new Coordinate(minLon, maxLat),
                new Coordinate(minLon, minLat)
        };
        return GEOMETRY_FACTORY.createPolygon(coordinates);
    }
}
