package com.dedicatedcode.paikka.controller;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry; // This import will be removed as it's no longer needed
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream; // This import will be removed as it's no longer needed
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream; // This import will be removed as it's no longer needed
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class GeocodingControllerIntegrationTest {

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;

    private static Path dataDirectory;
    private static boolean setupDone = false;

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) throws IOException {
        // Create a temporary directory using Java's Files API
        Path hostTempDir = Files.createTempDirectory("paikka-test-data");
        dataDirectory = hostTempDir; // Store it for later use in BeforeAll

        // Point Spring Boot's data-dir to the temporary directory
        registry.add("paikka.data-dir", hostTempDir::toString);
        registry.add("paikka.import-mode", () -> "false");
        registry.add("paikka.admin.password", () -> "testpassword"); // Set a password for admin endpoint
    }

    @BeforeAll
    static void setupDataAndRefresh(@Autowired TestRestTemplate staticRestTemplate, @LocalServerPort int staticPort) throws Exception {
        if (!setupDone) {
            // Ensure the data directory exists (it should, as created in DynamicPropertySource)
            Files.createDirectories(dataDirectory);

            // Extract data-monaco.zip to the temporary data directory
            Path zipPath = Paths.get("src/test/resources/data-monaco.zip");
            if (!Files.exists(zipPath)) {
                throw new IllegalStateException("Test resource data-monaco.zip not found at " + zipPath.toAbsolutePath());
            }
            extractZip(zipPath, dataDirectory);
            System.out.println("Extracted data-monaco.zip to: " + dataDirectory.toAbsolutePath());

            // Perform admin refresh using TestRestTemplate
            String adminRefreshUrl = "http://localhost:" + staticPort + "/admin/refresh-db";
            HttpHeaders headers = new HttpHeaders();
            String auth = "admin:testpassword";
            String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes());
            headers.add("Authorization", "Basic " + encodedAuth);
            headers.setContentType(MediaType.APPLICATION_JSON); // Request JSON response

            HttpEntity<String> request = new HttpEntity<>(headers);
            ResponseEntity<Map> adminResponse = staticRestTemplate.exchange(adminRefreshUrl, HttpMethod.POST, request, Map.class);

            assertThat(adminResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
            assertThat(adminResponse.getBody()).isNotNull();
            assertThat(adminResponse.getBody().get("success")).isEqualTo(true);
            System.out.println("Admin refresh response: " + adminResponse.getBody());

            // Mark setup as done
            setupDone = true;
        }
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
        assertThat(restTemplate).isNotNull();
    }

    @Test
    void testHealthEndpoint() {
        String url = "http://localhost:" + port + "/api/v1/health";
        ResponseEntity<Map> response = restTemplate.getForEntity(url, Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody()).containsKey("status");
        assertThat(response.getBody().get("status")).isEqualTo("ok");
        assertThat(response.getBody()).containsKey("metadata");
        assertThat((Map<String, Object>) response.getBody().get("metadata")).containsKey("dataVersion");
    }

    @Test
    void testReverseGeocodingKnownLocationMonaco() {
        // Coordinates for Monaco
        double lat = 43.7384;
        double lon = 7.4246;
        String url = String.format("http://localhost:%d/api/v1/reverse?lat=%f&lon=%f&lang=en", port, lat, lon);

        ResponseEntity<Map> response = restTemplate.getForEntity(url, Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody()).containsKey("results");
        assertThat(response.getBody().get("results")).isInstanceOf(List.class);

        List<Map<String, Object>> results = (List<Map<String, Object>>) response.getBody().get("results");
        assertThat(results).isNotEmpty();

        // Assert that results contain something related to Monaco
        boolean foundMonaco = results.stream()
                .anyMatch(poi -> {
                    String name = (String) poi.get("name");
                    // Assuming the Monaco dataset will return "Monaco" or a place within Monaco
                    return name != null && (name.contains("Monaco") || name.contains("Monte Carlo"));
                });
        assertThat(foundMonaco).isTrue();
    }

    @Test
    void testReverseGeocodingInvalidCoordinates() {
        String url = String.format("http://localhost:%d/api/v1/reverse?lat=%f&lon=%f", port, 91.0, 13.0); // Invalid latitude
        ResponseEntity<Map> response = restTemplate.getForEntity(url, Map.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody()).containsKey("error");
        assertThat(response.getBody().get("error")).isEqualTo("Invalid latitude. Must be between -90 and 90.");

        url = String.format("http://localhost:%d/api/v1/reverse?lat=%f&lon=%f", port, 52.0, 181.0); // Invalid longitude
        response = restTemplate.getForEntity(url, Map.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody()).containsKey("error");
        assertThat(response.getBody().get("error")).isEqualTo("Invalid longitude. Must be between -180 and 180.");
    }

    @Test
    void testReverseGeocodingWithLimit() {
        double lat = 43.7384; // Monaco
        double lon = 7.4246; // Monaco
        int limit = 2;
        String url = String.format("http://localhost:%d/api/v1/reverse?lat=%f&lon=%f&limit=%d", port, lat, lon, limit);

        ResponseEntity<Map> response = restTemplate.getForEntity(url, Map.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody()).containsKey("results");
        List<Map<String, Object>> results = (List<Map<String, Object>>) response.getBody().get("results");
        assertThat(results.size()).isLessThanOrEqualTo(limit); // Should be exactly 'limit' if enough results, or less
        assertThat(response.getHeaders().get("X-Result-Count")).containsExactly(String.valueOf(results.size()));
    }
}
