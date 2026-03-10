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

import java.nio.file.Path;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@IntegrationTest
class GeometryControllerTest {

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
    void shouldBeAbleToFetchGeometry() throws Exception {
        mockMvc.perform(get("/api/v1/geometry/20260310-051652/2220322")
                                .contentType(MediaType.APPLICATION_JSON))
                .andExpectAll(
                        status().isOk(),
                        jsonPath("$.type").value("MultiPolygon"),
                        jsonPath("$.coordinates").isNotEmpty()
                );
    }

    @Test
    void shouldRedirectToLatestDataset() throws Exception {
        mockMvc.perform(get("/api/v1/geometry/latest/2220322")
                                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().is3xxRedirection())
                .andExpect(header().exists("Location"))
                .andExpect(header().string("Location", "/api/v1/geometry/20260310-051652/2220322"));
    }
}