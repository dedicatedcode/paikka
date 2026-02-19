package com.dedicatedcode.paikka.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class GeoJsonGeometry {
    
    @JsonProperty("type")
    private String type;
    
    @JsonProperty("coordinates")
    private Object coordinates;
    
    public GeoJsonGeometry() {}
    
    public GeoJsonGeometry(String type, Object coordinates) {
        this.type = type;
        this.coordinates = coordinates;
    }
    
    public String getType() {
        return type;
    }
    
    public void setType(String type) {
        this.type = type;
    }
    
    public Object getCoordinates() {
        return coordinates;
    }
    
    public void setCoordinates(Object coordinates) {
        this.coordinates = coordinates;
    }
}
