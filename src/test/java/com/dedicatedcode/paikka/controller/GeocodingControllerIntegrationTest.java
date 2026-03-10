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

import com.dedicatedcode.paikka.IntegrationTest;
import com.dedicatedcode.paikka.TestHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.user;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@IntegrationTest
class GeocodingControllerIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Value("${paikka.data-dir}")
    private Path dataDirectory;

    @BeforeEach
    void setupDataAndRefresh() throws Exception {
        TestHelper.unpack(dataDirectory, "data-monaco.zip");

        // Perform admin refresh using MockMvc
        mockMvc.perform(post("/admin/refresh-db").header("X-Admin-Token", "test")
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andReturn();

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
        double lat = 43.728410;
        double lon = 7.417274;

        mockMvc.perform(get("/api/v1/reverse")
                        .param("lat", String.valueOf(lat))
                        .param("lon", String.valueOf(lon))
                        .param("lang", "en"))
                .andExpectAll(
                        status().isOk(),
                        jsonPath("$.results").isArray(),
                        jsonPath("$.results").isNotEmpty(),
                        jsonPath("$.results[0].display_name").value("Promethee sculpture"),
                        jsonPath("$.results[0].hierarchy").isNotEmpty()
                );
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
