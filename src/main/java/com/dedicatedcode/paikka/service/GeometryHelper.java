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

package com.dedicatedcode.paikka.service;

import com.dedicatedcode.paikka.dto.GeoJsonGeometry;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class GeometryHelper {
    private static final Logger logger = LoggerFactory.getLogger(GeometryHelper.class);

    public static GeoJsonGeometry convertJtsToGeoJson(Geometry geometry) {
        String geometryType = geometry.getGeometryType();
        Object coordinates = null;

        switch (geometryType) {
            case "Point":
                coordinates = new double[]{geometry.getCoordinate().x, geometry.getCoordinate().y};
                break;
            case "Polygon":
                // For polygons, we need to extract the exterior ring coordinates
                // This is a simplified implementation
                coordinates = extractPolygonCoordinates(geometry);
                break;
            case "MultiPolygon":
                // For multipolygons, extract all polygon coordinates
                coordinates = extractMultiPolygonCoordinates(geometry);
                break;
            default:
                // For other geometry types, just indicate the type
                logger.debug("Unsupported geometry type for GeoJSON conversion: {}", geometryType);
                return new GeoJsonGeometry("Unknown", null);
        }

        return new GeoJsonGeometry(geometryType, coordinates);
    }


    private static Object extractPolygonCoordinates(Geometry polygon) {
        try {
            // Get exterior ring coordinates
            org.locationtech.jts.geom.Coordinate[] coords = polygon.getCoordinates();
            double[][] ring = new double[coords.length][2];

            for (int i = 0; i < coords.length; i++) {
                ring[i][0] = coords[i].x; // longitude
                ring[i][1] = coords[i].y; // latitude
            }

            // GeoJSON polygon format: [[[x,y],[x,y],...]]
            return new double[][][]{ring};
        } catch (Exception e) {
            logger.warn("Failed to extract polygon coordinates: {}", e.getMessage());
            return null;
        }
    }

    private static Object extractMultiPolygonCoordinates(Geometry multiPolygon) {
        try {
            List<double[][][]> polygons = new ArrayList<>();

            for (int i = 0; i < multiPolygon.getNumGeometries(); i++) {
                Geometry polygon = multiPolygon.getGeometryN(i);
                Object polyCoords = extractPolygonCoordinates(polygon);
                if (polyCoords instanceof double[][][]) {
                    polygons.add((double[][][]) polyCoords);
                }
            }

            return polygons.toArray(new double[0][][][]);
        } catch (Exception e) {
            logger.warn("Failed to extract multipolygon coordinates: {}", e.getMessage());
            return null;
        }
    }
}
