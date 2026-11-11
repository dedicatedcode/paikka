/*
 *  This file is part of paikka.
 *
 *  Paikka is free software: you can redistribute it and/or
 *  modify it under the terms of the GNU Affero General Public License
 *  as published by the Free Software Foundation, either version 3 of the
 *  License, or any later version.
 *
 *  Paikka is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *  See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License
 * along with Paikka. If not, see <https://www.gnu.org/licenses/>.
 */

package com.dedicatedcode.paikka.service.importer;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.WKBReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Static continent reference data used for hierarchy tagging.
 * <p>
 * Continent outlines derive from Natural Earth 110m country polygons, dissolved per continent
 * and simplified (see {@code scripts/generate-continent-polygons.py}). The ISO mapping is
 * generated from the same source. The continent assignment rule is: the country tag decides
 * membership; the polygon is only used when no country ancestor is available (filtered packs).
 */
public final class ContinentReference {

    public static final String UNKNOWN = "XX";

    private static final Logger log = LoggerFactory.getLogger(ContinentReference.class);
    private static final List<String> CONTINENT_CODES = List.of("EU", "AS", "AF", "NA", "SA", "OC", "AN");
    private static final WKBReader WKB_READER = new WKBReader();

    private static final Map<String, String> ISO_TO_CONTINENT = loadIsoMapping();
    private static final Map<String, Geometry> CONTINENT_GEOMETRIES = new ConcurrentHashMap<>();

    private ContinentReference() {
    }

    public static String continentForIso(String iso) {
        if (iso == null || iso.isBlank()) {
            return null;
        }
        return ISO_TO_CONTINENT.get(iso.trim().toUpperCase(Locale.ROOT));
    }

    public static String continentForPoint(Point point) {
        if (point == null || point.isEmpty()) {
            return UNKNOWN;
        }
        for (String code : CONTINENT_CODES) {
            Geometry geometry = geometry(code);
            if (geometry != null && geometry.covers(point)) {
                return code;
            }
        }
        return UNKNOWN;
    }

    private static Geometry geometry(String code) {
        return CONTINENT_GEOMETRIES.computeIfAbsent(code, ContinentReference::loadGeometry);
    }

    private static Geometry loadGeometry(String code) {
        String resource = "/coverage/continents/" + code + ".wkb";
        try (InputStream stream = ContinentReference.class.getResourceAsStream(resource)) {
            if (stream == null) {
                return null;
            }
            return WKB_READER.read(stream.readAllBytes());
        } catch (Exception e) {
            log.warn("Could not load continent geometry {}: {}", resource, e.getMessage());
            return null;
        }
    }

    private static Map<String, String> loadIsoMapping() {
        Map<String, String> mapping = new HashMap<>();
        String resource = "/coverage/iso-continents.csv";
        try (InputStream stream = ContinentReference.class.getResourceAsStream(resource)) {
            if (stream == null) {
                log.warn("Could not find {}", resource);
                return Map.of();
            }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split(",", 2);
                    if (parts.length == 2 && !parts[0].isBlank() && !parts[1].isBlank()) {
                        mapping.put(parts[0].trim().toUpperCase(Locale.ROOT), parts[1].trim().toUpperCase(Locale.ROOT));
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Could not load {}: {}", resource, e.getMessage());
        }
        return Map.copyOf(mapping);
    }
}
