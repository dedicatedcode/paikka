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
