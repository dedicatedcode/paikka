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
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with Paikka. If not, see <https://www.gnu.org/licenses/>.
 */

package com.dedicatedcode.paikka.service.importer;

import com.dedicatedcode.paikka.config.PaikkaConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Monaco is smaller than a single H3 cell at the country resolution (4). The importer must
 * fall back to a finer resolution instead of silently dropping the relation, otherwise every
 * Monégasque sub-region becomes a root of Europe in Reitti.
 */
class StandaloneBoundaryImporterMonacoTest {

    private static final long MONACO_COUNTRY = 1124039L;
    private static final long MONACO_MUNICIPALITY = 2220322L;

    private static Path tempOutputDir;
    private static Path tempPbfFile;

    @BeforeAll
    static void setUp() throws Exception {
        tempOutputDir = Files.createTempDirectory("paikka-monaco-test");
        tempPbfFile = Files.createTempFile("paikka-monaco-test", ".pbf");

        try (InputStream is = StandaloneBoundaryImporter.class.getClassLoader()
                .getResourceAsStream("monaco-filtered.pbf")) {
            assertNotNull(is, "monaco-filtered.pbf not found in test resources");
            Files.copy(is, tempPbfFile, StandardCopyOption.REPLACE_EXISTING);
        }

        PaikkaConfiguration config = new PaikkaConfiguration();

        PaikkaConfiguration.ImportConfiguration importCfg = new PaikkaConfiguration.ImportConfiguration();
        importCfg.setThreads(4);
        config.setImportConfiguration(importCfg);

        PaikkaConfiguration.SimplificationConfiguration simplCfg = new PaikkaConfiguration.SimplificationConfiguration();
        simplCfg.setContinentTolerance(0.005);
        simplCfg.setCountryTolerance(0.00045);
        simplCfg.setStateTolerance(0.00009);
        simplCfg.setPoiTolerance(0.000018);
        simplCfg.setDefaultTolerance(0.000045);
        config.setSimplificationConfiguration(simplCfg);

        GeometrySimplificationService simplService = new GeometrySimplificationService(config);
        StandaloneBoundaryImporter importer = new StandaloneBoundaryImporter(simplService, config);
        importer.importBoundaries(Collections.singletonList(tempPbfFile.toString()), tempOutputDir.toString());
    }

    @AfterAll
    static void tearDown() {
        if (tempOutputDir != null && Files.exists(tempOutputDir)) {
            deleteDirectory(tempOutputDir.toFile());
        }
        if (tempPbfFile != null && Files.exists(tempPbfFile)) {
            tempPbfFile.toFile().delete();
        }
    }

    @Test
    void subCellCountryIsKeptWithFinerResolution() throws IOException {
        Map<Long, String[]> rows = readHierarchy();

        String[] monaco = rows.get(MONACO_COUNTRY);
        assertNotNull(monaco, "Monaco (1124039) must not be dropped from the hierarchy");

        int resolution = Integer.parseInt(monaco[4]);
        long totalCells = Long.parseLong(monaco[5]);
        assertTrue(resolution > 4,
                "Monaco is smaller than a res-4 cell and must fall back to a finer resolution, got " + resolution);
        assertTrue(totalCells > 0, "Monaco must have at least one H3 cell");
    }

    @Test
    void subCellCountryKeepsItsChildren() throws IOException {
        Map<Long, String[]> rows = readHierarchy();

        String[] municipality = rows.get(MONACO_MUNICIPALITY);
        assertNotNull(municipality, "Monaco municipality (2220322) must be present");
        assertEquals(Long.toString(MONACO_COUNTRY), municipality[1],
                "Monaco municipality must be parented to the country relation");
    }

    private static Map<Long, String[]> readHierarchy() throws IOException {
        Map<Long, String[]> rows = new HashMap<>();
        for (String line : Files.readAllLines(tempOutputDir.resolve("region_hierarchy.tsv"))) {
            if (line.isBlank()) {
                continue;
            }
            String[] columns = line.split("\t", -1);
            rows.put(Long.parseLong(columns[0]), columns);
        }
        return rows;
    }

    private static void deleteDirectory(java.io.File dir) {
        java.io.File[] files = dir.listFiles();
        if (files != null) {
            for (java.io.File file : files) {
                deleteDirectory(file);
            }
        }
        dir.delete();
    }
}
