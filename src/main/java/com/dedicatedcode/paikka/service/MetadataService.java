package com.dedicatedcode.paikka.service;

import com.dedicatedcode.paikka.config.PaikkaConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
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

    private volatile Map<String, Object> metadata;

    public MetadataService(PaikkaConfiguration config, ObjectMapper objectMapper) {
        this.config = config;
        this.objectMapper = objectMapper;
        this.metadata = Collections.emptyMap(); // Initialize with empty map
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
            this.metadata = Collections.emptyMap();
            return;
        }

        try {
            this.metadata = objectMapper.readValue(metadataPath.toFile(), Map.class);
            logger.info("Metadata loaded successfully from {}", metadataPath);
        } catch (IOException e) {
            logger.error("Failed to load metadata from {}: {}", metadataPath, e.getMessage());
            this.metadata = Collections.emptyMap();
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
        return Optional.ofNullable(metadata.get("dataVersion"))
                .map(Object::toString)
                .orElse("unknown");
    }

    /**
     * Returns the full metadata map.
     */
    public Map<String, Object> getMetadata() {
        return Collections.unmodifiableMap(metadata);
    }

    /**
     * Returns the import timestamp as an Instant.
     * If metadata is not available or timestamp is invalid, returns Optional.empty().
     */
    public Optional<Instant> getImportTimestamp() {
        return Optional.ofNullable(metadata.get("importTimestamp"))
                .map(Object::toString)
                .flatMap(timestampStr -> {
                    try {
                        return Optional.of(Instant.parse(timestampStr));
                    } catch (java.time.format.DateTimeParseException e) {
                        logger.warn("Invalid importTimestamp format in metadata: {}", timestampStr);
                        return Optional.empty();
                    }
                });
    }

    /**
     * Returns the PAIKKA application version that generated the data.
     */
    public String getPaikkaVersion() {
        return Optional.ofNullable(metadata.get("paikkaVersion"))
                .map(Object::toString)
                .orElse("unknown");
    }
}
