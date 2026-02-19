package com.dedicatedcode.paikka.controller;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(GeometryController.class)
class GeometryControllerTest {
    
    @Autowired
    private MockMvc mockMvc;
    
    @Test
    void testGeometryEndpoint() throws Exception {
        mockMvc.perform(get("/api/v1/geometry/12345"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.osm_id").value(12345))
                .andExpect(jsonPath("$.message").exists());
    }
}
