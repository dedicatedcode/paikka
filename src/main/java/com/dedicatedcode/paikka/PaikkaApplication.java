package com.dedicatedcode.paikka;

import com.dedicatedcode.paikka.service.ImportService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

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
        // Only run import logic if --import flag is present
        boolean isImportMode = false;
        String pbfFile = null;
        String dataDir = "./data";
        
        for (int i = 0; i < args.length; i++) {
            if ("--import".equals(args[i])) {
                isImportMode = true;
            } else if ("--pbf-file".equals(args[i]) && i + 1 < args.length) {
                pbfFile = args[i + 1];
            } else if ("--data-dir".equals(args[i]) && i + 1 < args.length) {
                dataDir = args[i + 1];
            }
        }
        
        if (isImportMode) {
            if (pbfFile == null) {
                logger.error("Import mode requires --pbf-file argument");
                System.exit(1);
            }
            

            try {
                importService.importData(pbfFile, dataDir);
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
