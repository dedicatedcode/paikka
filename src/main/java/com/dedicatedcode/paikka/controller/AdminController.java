package com.dedicatedcode.paikka.controller;

import com.dedicatedcode.paikka.service.ReverseGeocodingService;
import com.dedicatedcode.paikka.service.BoundaryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.HashMap;
import java.util.Map;

@Controller
@RequestMapping("/admin")
public class AdminController {
    
    private static final Logger logger = LoggerFactory.getLogger(AdminController.class);
    
    private final ReverseGeocodingService reverseGeocodingService;
    private final BoundaryService boundaryService;

    public AdminController(ReverseGeocodingService reverseGeocodingService, BoundaryService boundaryService) {
        this.reverseGeocodingService = reverseGeocodingService;
        this.boundaryService = boundaryService;
    }

    /**
     * Refresh the RocksDB databases by reloading them from the data directory.
     * This is useful when a new data folder has been uploaded or updated.
     * 
     * Can be called via:
     * - Browser: POST to /admin/refresh-db (returns HTML response)
     * - API: POST to /admin/refresh-db with Accept: application/json (returns JSON)
     * - curl: curl -X POST http://localhost:8080/admin/refresh-db -u admin:password
     */
    @PostMapping("/refresh-db")
    @PreAuthorize("hasRole('ADMIN')")
    @ResponseBody
    public ResponseEntity<?> refreshDatabase() {
        logger.info("Database refresh requested");
        
        try {
            // Reload the search service (POI shards database)
            logger.info("Reloading POI shards database...");
            reverseGeocodingService.reloadDatabase();
            
            // Reload the boundary service (boundaries database)
            logger.info("Reloading boundaries database...");
            boundaryService.reloadDatabase();
            
            logger.info("Database refresh completed successfully");
            
            // Return JSON response for API calls
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "Database refreshed successfully");
            response.put("timestamp", System.currentTimeMillis());
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            logger.error("Failed to refresh database", e);
            
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", "Failed to refresh database: " + e.getMessage());
            errorResponse.put("timestamp", System.currentTimeMillis());
            
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
        }
    }
}
