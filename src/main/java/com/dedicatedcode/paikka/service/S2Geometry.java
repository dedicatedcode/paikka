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
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class S2Geometry {

    public static List<S2Polygon> toS2Polygons(Geometry geom) {
        if (geom == null || geom.isEmpty()) {
            return Collections.emptyList();
        }
        List<S2Polygon> result = new ArrayList<>();
        collectPolygons(geom, result);
        return result;
    }

    private static void collectPolygons(Geometry geom, List<S2Polygon> result) {
        if (geom instanceof Polygon poly) {
            List<S2Loop> loops = new ArrayList<>();
            loops.add(createLoop(poly.getExteriorRing()));
            for (int i = 0; i < poly.getNumInteriorRing(); i++) {
                loops.add(createLoop(poly.getInteriorRingN(i)));
            }
            S2Polygon sp = new S2Polygon();
            sp.initOriented(loops);
            result.add(sp);
        } else if (geom instanceof GeometryCollection) {
            for (int i = 0; i < geom.getNumGeometries(); i++) {
                collectPolygons(geom.getGeometryN(i), result);
            }
        }
    }

    private static S2Loop createLoop(LinearRing ring) {
        List<S2Point> points = new ArrayList<>();
        Coordinate[] coords = ring.getCoordinates();

        for (int i = 0; i < coords.length - 1; i++) {
            points.add(S2LatLng.fromDegrees(coords[i].y, coords[i].x).toPoint());
        }

        S2Loop loop = new S2Loop(points);
        loop.normalize();
        return loop;
    }
}