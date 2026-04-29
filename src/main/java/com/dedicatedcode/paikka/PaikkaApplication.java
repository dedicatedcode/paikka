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

package com.dedicatedcode.paikka;

import com.dedicatedcode.paikka.service.importer.ImportService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.*;

@SpringBootApplication
public class PaikkaApplication implements CommandLineRunner {
    
    private static final Logger logger = LoggerFactory.getLogger(PaikkaApplication.class);
    
    @Autowired
    private ImportService importService;

    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(PaikkaApplication.class);
        
        // Check if this is import mode
        boolean isImportMode = false;
        for (String arg : args) {
            if ("--import".equals(arg)) {
                isImportMode = true;
                break;
            }
        }
        
        if (isImportMode) {
            logger.info("Starting in import mode");
            app.setWebApplicationType(org.springframework.boot.WebApplicationType.NONE);
            System.setProperty("paikka.import-mode", "true");
            app.run(args);
        } else {
            logger.info("Starting in API server mode");
            System.setProperty("paikka.import-mode", "false");
            app.run(args);
        }
    }
    
    private  void printApiInfo() {
        logger.info("PAIKKA is now serving data under the following endpoints:");
        logger.info("  Health Check:     GET  /api/v1/health");
        logger.info("  Reverse Geocoding: GET  /api/v1/reverse?lat=60.1699&lon=24.9384");
        logger.info("  Reverse Geocoding (with language): GET  /api/v1/reverse?lat=60.1699&lon=24.9384&lang=fi");
        logger.info("  Reverse Geocoding (with limit): GET  /api/v1/reverse?lat=60.1699&lon=24.9384&lang=fi&limit=10");
        logger.info("  Geometry:         GET  /api/v1/geometry/12345");
        logger.info("");
        logger.info("Sample requests:");
        logger.info("  curl 'http://localhost:8080/api/v1/health'");
        logger.info("  curl 'http://localhost:8080/api/v1/reverse?lat=43.740382&lon=7.426712'");
        logger.info("  curl 'http://localhost:8080/api/v1/geometry/12345'");
        logger.info("");
    }

    @Override
    public void run(String... args) throws Exception {
        boolean isImportMode = false;
        List<String> pbfFiles = new ArrayList<>();
        String dataDir = "./data";
        Set<Integer> usedArgIndices = new HashSet<>();

        // Process flags and their values first
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if ("--import".equals(arg)) {
                isImportMode = true;
            } else if ("--pbf-file".equals(arg)) {
                if (i + 1 >= args.length) { logger.error("Missing --pbf-file value"); System.exit(1); }
                String value = args[ i + 1];
                usedArgIndices.add(i + 1);
                // Split comma-separated values, add non-empty trimmed paths
                Arrays.stream(value.split(",")).map(String::trim).filter(s -> !s.isEmpty()).forEach(pbfFiles::add);
                i++; // Skip flag value
            } else if ("--data-dir".equals(arg)) {
                if (i + 1 >= args.length) { logger.error("Missing --data-dir value"); System.exit(1); }
                dataDir = args[ i + 1];
                usedArgIndices.add(i + 1);
                i++; // Skip flag value
            }
        }

        // Collect trailing positional args (not flags/flag values) as PBF files
        for (int i = 0; i < args.length; i++) {
            if (usedArgIndices.contains(i)) continue;
            String arg = args[i];
            if (arg.startsWith("--")) continue; // Skip unrecognized flags
            if (isImportMode) pbfFiles.add(arg.trim());
        }

        if (isImportMode) {
            if (pbfFiles.isEmpty()) {
                logger.error("Import mode requires at least one PBF file (use --pbf-file or trailing positional args)");
                System.exit(1);
            }
            try {
                importService.importData(pbfFiles, dataDir);
                System.exit(0);
            } catch (Exception e) {
                logger.error("Import failed", e);
                System.exit(1);
            }
        } else {
            printApiInfo();
        }
    }
}
