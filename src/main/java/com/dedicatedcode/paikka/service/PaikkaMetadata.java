package com.dedicatedcode.paikka.service;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

record PaikkaMetadata(
        @JsonProperty("importTimestamp") String importTimestamp,
        @JsonProperty("dataVersion") String dataVersion,
        @JsonProperty("file") String file,
        @JsonProperty("gridLevel") Integer gridLevel, // Use Integer for nullable
        @JsonProperty("paikkaVersion") String paikkaVersion // Renamed from appVersion to match usage
) {
    // Default constructor for Jackson deserialization if needed, though records often handle this implicitly
    // You can add custom methods here if you need to parse the timestamp into an Instant directly within the record
    // For now, we'll keep the parsing logic in MetadataService for consistency with existing Optional<Instant> return.
}
