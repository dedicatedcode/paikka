package com.dedicatedcode.paikka.controller;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(GeocodingController.class)
class GeocodingControllerTest {
    
    @Autowired
    private MockMvc mockMvc;
    
    @Test
    void testReverseEndpoint() throws Exception {
        mockMvc.perform(get("/api/v1/reverse")
                .param("lat", "52.5200")
                .param("lon", "13.4050"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.lat").value(52.5200))
                .andExpect(jsonPath("$.lon").value(13.4050))
                .andExpect(jsonPath("$.lang").value("en"));
    }
    
    @Test
    void testReverseEndpointWithLanguage() throws Exception {
        mockMvc.perform(get("/api/v1/reverse")
                .param("lat", "52.5200")
                .param("lon", "13.4050")
                .param("lang", "fi"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.lang").value("fi"));
    }
    
    @Test
    void testHealthEndpoint() throws Exception {
        mockMvc.perform(get("/api/v1/health"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("ok"))
                .andExpect(jsonPath("$.service").value("paikka"));
    }
    
    @Test
    void testReverseEndpointMissingParameters() throws Exception {
        mockMvc.perform(get("/api/v1/reverse"))
                .andExpect(status().isBadRequest());
    }
}
