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

import com.dedicatedcode.paikka.config.PaikkaConfiguration;
import de.topobyte.osm4j.core.model.impl.Relation;
import de.topobyte.osm4j.core.model.impl.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class StandaloneBoundaryImporterDisputedTest {

    private final StandaloneBoundaryImporter importer = newImporter();

    private static StandaloneBoundaryImporter newImporter() {
        try {
            PaikkaConfiguration config = new PaikkaConfiguration();

            PaikkaConfiguration.ImportConfiguration importCfg = new PaikkaConfiguration.ImportConfiguration();
            importCfg.setThreads(1);
            config.setImportConfiguration(importCfg);

            PaikkaConfiguration.SimplificationConfiguration simplCfg = new PaikkaConfiguration.SimplificationConfiguration();
            simplCfg.setContinentTolerance(0.005);
            simplCfg.setCountryTolerance(0.00045);
            simplCfg.setStateTolerance(0.00009);
            simplCfg.setPoiTolerance(0.000018);
            simplCfg.setDefaultTolerance(0.000045);
            config.setSimplificationConfiguration(simplCfg);

            return new StandaloneBoundaryImporter(new GeometrySimplificationService(config), config);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void acceptsDisputedCountryRelationWithoutAdminLevel() {
        Relation relation = new Relation(1703814L, List.of(),
                List.of(new Tag("boundary", "disputed"),
                        new Tag("type", "boundary"),
                        new Tag("ISO3166-1", "PS"),
                        new Tag("name", "State of Palestine")));

        assertTrue(importer.isAdministrativeBoundary(relation));

        StandaloneBoundaryImporter.RelationStub stub = importer.buildRelationStub(relation);
        assertEquals(2, stub.adminLevel());
        assertEquals("PS", stub.isoCode());
    }

    @Test
    void stillAcceptsAdministrativeBoundaryWithAdminLevel() {
        Relation relation = new Relation(51477L, List.of(),
                List.of(new Tag("boundary", "administrative"),
                        new Tag("admin_level", "2"),
                        new Tag("ISO3166-1", "DE")));

        assertTrue(importer.isAdministrativeBoundary(relation));

        StandaloneBoundaryImporter.RelationStub stub = importer.buildRelationStub(relation);
        assertEquals(2, stub.adminLevel());
        assertEquals("DE", stub.isoCode());
    }

    @Test
    void rejectsRelationWithoutBoundaryOrIsoCode() {
        Relation relation = new Relation(1L, List.of(),
                List.of(new Tag("name", "Something")));

        assertFalse(importer.isAdministrativeBoundary(relation));
    }
}
