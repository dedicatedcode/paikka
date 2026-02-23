package com.dedicatedcode.paikka.controller;

import com.dedicatedcode.paikka.config.PaikkaConfiguration;
import com.dedicatedcode.paikka.dto.POIResponse;
import com.dedicatedcode.paikka.service.MetadataService;
import com.dedicatedcode.paikka.service.ReverseGeocodingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * REST controller for reverse geocoding operations.
 */
@RestController
@RequestMapping("/api/v1")
@ConditionalOnProperty(name = "paikka.import-mode", havingValue = "false", matchIfMissing = true)
public class GeocodingController {
    
    private static final Logger logger = LoggerFactory.getLogger(GeocodingController.class);
    
    private final ReverseGeocodingService reverseGeocodingService;
    private final PaikkaConfiguration config;
    private final MetadataService metadataService; // Inject MetadataService
    
    public GeocodingController(ReverseGeocodingService reverseGeocodingService, PaikkaConfiguration config, MetadataService metadataService) {
        this.reverseGeocodingService = reverseGeocodingService;
        this.config = config;
        this.metadataService = metadataService; // Inject MetadataService
    }
    
    /**
     * Reverse geocode a coordinate to find nearby POIs.
     * 
     * @param lat Latitude in degrees
     * @param lon Longitude in degrees
     * @param lang Language code for localized names (optional)
     * @param limit Maximum number of results to return (optional, defaults to configured max)
     * @return POI information with hierarchy
     */
    @GetMapping("/reverse")
    public ResponseEntity<Map<String, Object>> reverse(
            @RequestParam double lat,
            @RequestParam double lon,
            @RequestParam(defaultValue = "en") String lang,
            @RequestParam(required = false) Integer limit) {
        
        // Determine effective limit
        int effectiveLimit = (limit != null) ? Math.min(limit, config.getMaxResults()) : config.getDefaultResults();
        
        logger.debug("Reverse geocoding request: lat={}, lon={}, lang={}, limit={}", lat, lon, lang, effectiveLimit);
        
        // Validate coordinates
        if (lat < -90 || lat > 90) {
            Map<String, Object> error = new HashMap<>();
            error.put("error", "Invalid latitude. Must be between -90 and 90.");
            return ResponseEntity.badRequest().body(error);
        }
        
        if (lon < -180 || lon > 180) {
            Map<String, Object> error = new HashMap<>();
            error.put("error", "Invalid longitude. Must be between -180 and 180.");
            return ResponseEntity.badRequest().body(error);
        }
        
        // Validate limit parameter
        if (limit != null && limit <= 0) {
            Map<String, Object> error = new HashMap<>();
            error.put("error", "Invalid limit. Must be a positive integer.");
            return ResponseEntity.badRequest().body(error);
        }
        
        List<POIResponse> results = reverseGeocodingService.findNearbyPOIs(lat, lon, lang, effectiveLimit);
        
        Map<String, Object> response = new HashMap<>();
        response.put("results", results);
        response.put("count", results.size());
        response.put("query", Map.of(
            "lat", lat,
            "lon", lon,
            "lang", lang,
            "limit", effectiveLimit
        ));
        
        return ResponseEntity.ok()
            .header("X-Result-Count", String.valueOf(results.size()))
            .header("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate, max-age=0") // No caching
            .body(response);
    }
    
    /**
     * Health check endpoint.
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> health() {
        Map<String, Object> response = new HashMap<>();
        response.put("status", "ok");
        response.put("service", "paikka");
        response.put("metadata", metadataService.getMetadata()); // Include metadata
        return ResponseEntity.ok()
            .header("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate, max-age=0") // No caching
            .body(response);
    }
}
