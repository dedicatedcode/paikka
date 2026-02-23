package com.dedicatedcode.paikka.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.user;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@AutoConfigureMockMvc
class GeocodingControllerIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    // ObjectMapper is no longer needed for parsing responses in tests, but kept for potential future use or other tests
    // @Autowired
    // private ObjectMapper objectMapper; 

    @Value("${paikka.data-dir}")
    private Path dataDirectory;

    @BeforeEach
    void setupDataAndRefresh() throws Exception {
        // Clear the dataDirectory before we copy
        if (Files.exists(dataDirectory)) {
            try (Stream<Path> walk = Files.walk(dataDirectory)) {
                walk.sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            throw new RuntimeException("Failed to delete " + path, e);
                        }
                    });
            }
        }
        Files.createDirectories(dataDirectory);

        Path zipPath = Paths.get("src/test/resources/data-monaco.zip");
        if (!Files.exists(zipPath)) {
            throw new IllegalStateException("Test resource data-monaco.zip not found at " + zipPath.toAbsolutePath());
        }
        extractZip(zipPath, dataDirectory);
        System.out.println("Extracted data-monaco.zip to: " + dataDirectory.toAbsolutePath());

        // Perform admin refresh using MockMvc
        mockMvc.perform(post("/admin/refresh-db")
                        .with(user("admin"))
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andReturn();

    }

    private static void extractZip(Path zipFilePath, Path destinationDir) throws IOException {
        try (InputStream fi = Files.newInputStream(zipFilePath);
             BufferedInputStream bi = new BufferedInputStream(fi);
             ZipInputStream zip = new ZipInputStream(bi)) {

            ZipEntry entry;
            while ((entry = zip.getNextEntry()) != null) {
                Path file = destinationDir.resolve(entry.getName()).normalize();
                if (!file.startsWith(destinationDir)) {
                    // Security check: prevent path traversal
                    throw new IOException("Bad entry: " + entry.getName());
                }
                if (entry.isDirectory()) {
                    Files.createDirectories(file);
                } else {
                    Files.createDirectories(file.getParent());
                    Files.copy(zip, file);
                }
                zip.closeEntry();
            }
        }
    }

    @Test
    void contextLoads() {
        assertThat(mockMvc).isNotNull();
    }

    @Test
    void testHealthEndpoint() throws Exception {
        mockMvc.perform(get("/api/v1/health"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("ok"))
                .andExpect(jsonPath("$.metadata").exists())
                .andExpect(jsonPath("$.metadata.dataVersion").exists());
    }

    @Test
    void testReverseGeocodingKnownLocationMonaco() throws Exception {
        // Coordinates for Monaco
        double lat = 43.7384;
        double lon = 7.4246;

        mockMvc.perform(get("/api/v1/reverse")
                        .param("lat", String.valueOf(lat))
                        .param("lon", String.valueOf(lon))
                        .param("lang", "en"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.results").isArray())
                .andExpect(jsonPath("$.results").isNotEmpty())
                // Assert that results contain something related to Monaco
                // Using jsonPath to check for any name containing "Monaco" or "Monte Carlo"
                .andExpect(jsonPath("$.results[*].name").value(org.hamcrest.Matchers.anyOf(
                        org.hamcrest.Matchers.hasItem(org.hamcrest.Matchers.containsString("Monaco")),
                        org.hamcrest.Matchers.hasItem(org.hamcrest.Matchers.containsString("Monte Carlo"))
                )));
    }

    @Test
    void testReverseGeocodingInvalidCoordinates() throws Exception {
        // Invalid latitude
        mockMvc.perform(get("/api/v1/reverse")
                        .param("lat", String.valueOf(91.0))
                        .param("lon", String.valueOf(13.0)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("Invalid latitude. Must be between -90 and 90."));

        // Invalid longitude
        mockMvc.perform(get("/api/v1/reverse")
                        .param("lat", String.valueOf(52.0))
                        .param("lon", String.valueOf(181.0)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("Invalid longitude. Must be between -180 and 180."));
    }

    @Test
    void testReverseGeocodingWithLimit() throws Exception {
        double lat = 43.7384; // Monaco
        double lon = 7.4246; // Monaco
        int limit = 2;

        mockMvc.perform(get("/api/v1/reverse")
                        .param("lat", String.valueOf(lat))
                        .param("lon", String.valueOf(lon))
                        .param("limit", String.valueOf(limit)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.results").isArray())
                .andExpect(jsonPath("$.results.length()").value(limit))
                .andExpect(header().string("X-Result-Count", String.valueOf(limit)));
    }
}
