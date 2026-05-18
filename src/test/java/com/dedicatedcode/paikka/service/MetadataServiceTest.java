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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class MetadataServiceTest {
    @SuppressWarnings("unchecked")
    @Test
    void shouldLoadMetadata() {
        MetadataService candidate = new MetadataService(new MetaDataProvider() {
            @Override
            public boolean exists() {
                return true;
            }

            @Override
            public InputStream get() throws FileNotFoundException {
                return getClass().getResourceAsStream("/metadata.json");
            }
        }, new ObjectMapper());
        candidate.reload();

        Map<String, Object> loaded = candidate.getMetadata();
        assertNotNull(loaded);
        assertEquals("2026-05-17T07:47:51.028675223Z", loaded.get("importTimestamp"));
        assertEquals("20260517-074751", loaded.get("dataVersion"));
        assertEquals(12, loaded.get("gridLevel"));
        assertEquals("1.0.0", loaded.get("paikkaVersion"));
        List<String> files = (List<String>) loaded.get("files");
        assertNotNull(files);
        assertEquals(2, files.size());
        assertEquals("planet-filtered.pbf", files.get(0));
        assertEquals("test.pbf", files.get(1));
    }
}