package com.dedicatedcode.paikka.controller;

import com.dedicatedcode.paikka.dto.GeoJsonGeometry;
import com.dedicatedcode.paikka.exception.POINotFoundException;
import com.dedicatedcode.paikka.service.BoundaryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

/**
 * REST controller for geometry operations.
 */
@RestController
@RequestMapping("/api/v1")
@ConditionalOnProperty(name = "paikka.import-mode", havingValue = "false", matchIfMissing = true)
public class GeometryController {
    
    private static final Logger logger = LoggerFactory.getLogger(GeometryController.class);
    
    private final BoundaryService boundaryService;
    
    public GeometryController(BoundaryService boundaryService) {
        this.boundaryService = boundaryService;
    }
    
    /**
     * Get boundary geometry by OSM ID.
     * 
     * @param osmId OSM relation ID
     * @return Boundary geometry in GeoJSON format
     */
    @GetMapping("/geometry/{osmId}")
    public ResponseEntity<GeoJsonGeometry> getGeometry(@PathVariable long osmId) {
        logger.debug("Geometry request for OSM ID: {}", osmId);
        
        try {
            GeoJsonGeometry geometry = boundaryService.getBoundaryGeometry(osmId);
            return ResponseEntity.ok()
                .header("X-Result-Count", "1")
                .body(geometry);
        } catch (POINotFoundException e) {
            throw e; // Let Spring handle the 404 response
        } catch (Exception e) {
            logger.error("Error retrieving geometry for OSM ID {}: {}", osmId, e.getMessage());
            throw new RuntimeException("Internal error retrieving geometry", e);
        }
    }
}
