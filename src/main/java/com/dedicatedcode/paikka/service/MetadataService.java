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

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Service
@ConditionalOnProperty(name = "paikka.import-mode", havingValue = "false", matchIfMissing = true)
public class MetadataService {

    private static final Logger logger = LoggerFactory.getLogger(MetadataService.class);

    private final ObjectMapper objectMapper;
    private final MetaDataProvider provider;

    private volatile PaikkaMetadata metadata; // Change type to PaikkaMetadata

    public MetadataService(MetaDataProvider provider, ObjectMapper objectMapper) {
        this.provider = provider;
        this.objectMapper = objectMapper;
        this.metadata = null;
    }

    @PostConstruct
    public void init() {
        loadMetadata();
    }

    /**
     * Loads the metadata from the paikka_metadata.json file.
     * This method is synchronized to prevent race conditions during reload.
     */
    private synchronized void loadMetadata() {
        if (!provider.exists()) {
            logger.warn("Metadata file not!. Running without metadata.");
            this.metadata = null; // Set to null if file not found
            return;
        }

        try (InputStream is = provider.get()) {
            this.metadata = objectMapper.readValue(is, PaikkaMetadata.class); // Deserialize to PaikkaMetadata
            logger.info("Metadata loaded successfully");
        } catch (IOException e) {
            logger.error("Failed to load metadata:", e);
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
        metadataMap.put("files", metadata.files());
        metadataMap.put("gridLevel", metadata.gridLevel());
        metadataMap.put("paikkaVersion", metadata.paikkaVersion());
        return Collections.unmodifiableMap(metadataMap);
    }
}
