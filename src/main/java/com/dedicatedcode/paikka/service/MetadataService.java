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

package com.dedicatedcode.paikka.service;

import com.dedicatedcode.paikka.config.PaikkaConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Service
@ConditionalOnProperty(name = "paikka.import-mode", havingValue = "false", matchIfMissing = true)
public class MetadataService {

    private static final Logger logger = LoggerFactory.getLogger(MetadataService.class);
    private static final String METADATA_FILE_NAME = "paikka_metadata.json";

    private final PaikkaConfiguration config;
    private final ObjectMapper objectMapper;

    private volatile PaikkaMetadata metadata; // Change type to PaikkaMetadata

    public MetadataService(PaikkaConfiguration config, ObjectMapper objectMapper) {
        this.config = config;
        this.objectMapper = objectMapper;
        this.metadata = null; // Initialize with null, will be loaded in @PostConstruct
    }

    @PostConstruct
    public void init() {
        loadMetadata();
    }

    /**
     * Loads the metadata from the paikka_metadata.json file.
     * This method is synchronized to prevent race conditions during reload.
     */
    public synchronized void loadMetadata() {
        Path metadataPath = Paths.get(config.getDataDir(), METADATA_FILE_NAME);

        if (!Files.exists(metadataPath)) {
            logger.warn("Metadata file not found at {}. Running without metadata.", metadataPath);
            this.metadata = null; // Set to null if file not found
            return;
        }

        try {
            this.metadata = objectMapper.readValue(metadataPath.toFile(), PaikkaMetadata.class); // Deserialize to PaikkaMetadata
            logger.info("Metadata loaded successfully from {}", metadataPath);
        } catch (IOException e) {
            logger.error("Failed to load metadata from {}: {}", metadataPath, e.getMessage());
            this.metadata = null; // Set to null on error
        }
    }

    /**
     * Reloads the metadata. Useful after a new import.
     */
    public void reload() {
        logger.info("Reloading metadata...");
        loadMetadata();
    }

    /**
     * Returns the current data version.
     * If metadata is not available, returns a default "unknown" version.
     */
    public String getDataVersion() {
        return Optional.ofNullable(metadata)
                .map(PaikkaMetadata::dataVersion)
                .orElse("unknown");
    }

    /**
     * Returns the full metadata map.
     * Converts the PaikkaMetadata record to a Map for compatibility with existing consumers.
     */
    public Map<String, Object> getMetadata() {
        if (metadata == null) {
            return Collections.emptyMap();
        }
        // Convert PaikkaMetadata record to a Map
        Map<String, Object> metadataMap = new HashMap<>();
        metadataMap.put("importTimestamp", metadata.importTimestamp());
        metadataMap.put("dataVersion", metadata.dataVersion());
        metadataMap.put("file", metadata.file());
        metadataMap.put("gridLevel", metadata.gridLevel());
        metadataMap.put("paikkaVersion", metadata.paikkaVersion());
        return Collections.unmodifiableMap(metadataMap);
    }

    /**
     * Returns the import timestamp as an Instant.
     * If metadata is not available or timestamp is invalid, returns Optional.empty().
     */
    public Optional<Instant> getImportTimestamp() {
        return Optional.ofNullable(metadata)
                .map(PaikkaMetadata::importTimestamp)
                .flatMap(timestampStr -> {
                    try {
                        return Optional.of(Instant.parse(timestampStr));
                    } catch (DateTimeParseException e) {
                        logger.warn("Invalid importTimestamp format in metadata: {}", timestampStr);
                        return Optional.empty();
                    }
                });
    }

    /**
     * Returns the PAIKKA application version that generated the data.
     */
    public String getPaikkaVersion() {
        return Optional.ofNullable(metadata)
                .map(PaikkaMetadata::paikkaVersion)
                .orElse("unknown");
    }
}
