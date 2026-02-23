package com.dedicatedcode.paikka.service;

import com.fasterxml.jackson.annotation.JsonProperty;

record PaikkaMetadata(
        @JsonProperty("importTimestamp") String importTimestamp,
        @JsonProperty("dataVersion") String dataVersion,
        @JsonProperty("file") String file,
        @JsonProperty("gridLevel") Integer gridLevel,
        @JsonProperty("paikkaVersion") String paikkaVersion
) {
}
