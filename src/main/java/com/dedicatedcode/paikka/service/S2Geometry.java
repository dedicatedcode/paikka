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

import com.google.common.geometry.*;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.LinearRing;
import java.util.ArrayList;
import java.util.List;

public class S2Geometry {

    /**
     * Converts a JTS Polygon into an S2Polygon.
     */
    public static S2Polygon toS2Polygon(org.locationtech.jts.geom.Geometry geom) {
        if (!(geom instanceof Polygon jtsPoly)) {
            // If it's a MultiPolygon, you'd iterate through them. 
            // For now, let's handle the standard Polygon.
            return new S2Polygon();
        }

        List<S2Loop> loops = new ArrayList<>();
        
        // 1. Handle the exterior shell
        loops.add(createLoop(jtsPoly.getExteriorRing()));

        // 2. Handle holes (interior rings)
        for (int i = 0; i < jtsPoly.getNumInteriorRing(); i++) {
            loops.add(createLoop(jtsPoly.getInteriorRingN(i)));
        }

        S2Polygon s2Polygon = new S2Polygon(loops);
        // Important: S2Polygons need to be initialized/fixed for orientation
        return s2Polygon;
    }

    private static S2Loop createLoop(LinearRing ring) {
        List<S2Point> points = new ArrayList<>();
        Coordinate[] coords = ring.getCoordinates();
        
        // JTS repeats the last point (closed loop). S2 does NOT.
        // We take N-1 points.
        for (int i = 0; i < coords.length - 1; i++) {
            points.add(S2LatLng.fromDegrees(coords[i].y, coords[i].x).toPoint());
        }
        
        S2Loop loop = new S2Loop(points);
        loop.normalize(); // Ensures the loop covers the smaller area of the sphere
        return loop;
    }
}