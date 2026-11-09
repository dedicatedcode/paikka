/*
 *  This file is part of paikka.
 *
 *  Paikka is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  Paikka is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with Paikka. If not, see <https://www.gnu.org/licenses/>.
 */

package com.dedicatedcode.paikka.service.importer;

import com.dedicatedcode.paikka.config.PaikkaConfiguration;
import com.dedicatedcode.paikka.service.S2Helper;
import com.slimjars.dist.gnu.trove.list.array.TLongArrayList;
import de.topobyte.osm4j.core.model.iface.OsmTag;
import de.topobyte.osm4j.core.model.impl.Node;
import de.topobyte.osm4j.core.model.impl.Tag;
import de.topobyte.osm4j.core.model.impl.Way;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ImportServicePoiFilterTest {

    private ImportService importService;

    @BeforeEach
    void setUp() {
        PaikkaConfiguration config = new PaikkaConfiguration();
        config.setSimplificationConfiguration(new PaikkaConfiguration.SimplificationConfiguration());
        importService = new ImportService(new S2Helper(),
                                         new GeometrySimplificationService(config),
                                         config,
                                         "test");
    }

    @Test
    void chargingStationNodeIsNotAPoi() {
        assertFalse(importService.isPoi(node(new Tag("amenity", "charging_station"),
                                             new Tag("operator", "Compleo"))));
    }

    @Test
    void chargingStationWayIsNotAPoi() {
        TLongArrayList nodes = new TLongArrayList();
        nodes.add(1L);
        nodes.add(2L);
        nodes.add(3L);
        nodes.add(1L);
        Way way = new Way(10L, nodes, List.of(new Tag("amenity", "charging_station")));

        assertFalse(importService.isPoi(way));
    }

    @Test
    void namedChargingStationIsStillNotAPoi() {
        assertFalse(importService.isPoi(node(new Tag("amenity", "charging_station"),
                                             new Tag("name", "Stadtwerke charging point"))));
    }

    @Test
    void otherAmenitiesAreStillImported() {
        assertTrue(importService.isPoi(node(new Tag("amenity", "parking"))));
        assertTrue(importService.isPoi(node(new Tag("amenity", "restaurant"))));
    }

    @Test
    void shopsAreStillImported() {
        assertTrue(importService.isPoi(node(new Tag("shop", "supermarket"))));
    }

    private Node node(OsmTag... tags) {
        return new Node(1L, 53.8676, 10.6958, List.of(tags));
    }
}
