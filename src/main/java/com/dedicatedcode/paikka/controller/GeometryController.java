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

package com.dedicatedcode.paikka.controller;

import com.dedicatedcode.paikka.dto.GeoJsonGeometry;
import com.dedicatedcode.paikka.exception.POINotFoundException;
import com.dedicatedcode.paikka.service.BoundaryService;
import com.dedicatedcode.paikka.service.MetadataService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
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
    private final MetadataService metadataService;
    
    public GeometryController(BoundaryService boundaryService, MetadataService metadataService) {
        this.boundaryService = boundaryService;
        this.metadataService = metadataService;
    }

    /**
     * Redirects to the current versioned geometry endpoint.
     * This endpoint can be cached for a short duration.
     *
     * @param osmId OSM relation ID
     * @return Redirect to the versioned geometry
     */
    @GetMapping("/geometry/latest/{osmId}")
    public ResponseEntity<Void> getLatestGeometry(@PathVariable long osmId) {
        String currentDataVersion = metadataService.getDataVersion();
        String redirectUrl = String.format("/api/v1/geometry/%s/%d", currentDataVersion, osmId);
        logger.debug("Redirecting /geometry/latest/{} to {}", osmId, redirectUrl);
        return ResponseEntity.status(HttpStatus.FOUND) // 302 Found
                .location(URI.create(redirectUrl))
                .header("Cache-Control", "public, max-age=86400") // Cache redirect for 1 day
                .build();
    }
    
    /**
     * Get boundary geometry by OSM ID for a specific data version.
     * 
     * @param dataVersion The data version identifier (for cache busting)
     * @param osmId OSM relation ID
     * @return Boundary geometry in GeoJSON format
     */
    @GetMapping("/geometry/{dataVersion}/{osmId}")
    public ResponseEntity<GeoJsonGeometry> getGeometry(@PathVariable String dataVersion, @PathVariable long osmId) {
        logger.debug("Geometry request for OSM ID: {} (version: {})", osmId, dataVersion);

        // Optional: Validate dataVersion. If the requested version is not the current one,
        // it might indicate a stale client or a request for old data.
        // Returning 410 Gone indicates the resource is no longer available at that version.
        String currentDataVersion = metadataService.getDataVersion();
        if (!"unknown".equals(currentDataVersion) && !currentDataVersion.equals(dataVersion)) {
            logger.warn("Requested geometry for old data version {}. Current version is {}. Returning 410 Gone.", dataVersion, currentDataVersion);
            return ResponseEntity.status(HttpStatus.GONE).build(); // 410 Gone
        }
        
        try {
            GeoJsonGeometry geometry = boundaryService.getBoundaryGeometry(osmId);
            return ResponseEntity.ok()
                .header("X-Result-Count", "1")
                .header("Cache-Control", "public, max-age=31536000, immutable") // Long-term caching for immutable data
                .body(geometry);
        } catch (POINotFoundException e) {
            throw e; // Let Spring handle the 404 response
        } catch (Exception e) {
            logger.error("Error retrieving geometry for OSM ID {} (version {}): {}", osmId, dataVersion, e.getMessage());
            throw new RuntimeException("Internal error retrieving geometry", e);
        }
    }
}
