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

package com.dedicatedcode.paikka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class TestHelper {
    private static final Logger log = LoggerFactory.getLogger(TestHelper.class);
    public static void unpack(Path dataDirectory, String bundle) throws IOException {
        // Clear the dataDirectory before we copy
        if (Files.exists(dataDirectory)) {
            try (Stream<Path> walk = Files.walk(dataDirectory)) {
                walk.sorted(Comparator.reverseOrder())
                        .forEach(path -> {
                            try {
                                Files.delete(path);
                            } catch (IOException e) {
                                throw new RuntimeException("Failed to delete " + path, e);
                            }
                        });
            }
        }
        Files.createDirectories(dataDirectory);

        Path zipPath = Paths.get("src/test/resources/" + bundle);
        if (!Files.exists(zipPath)) {
            throw new IllegalStateException("Test resource " + bundle + " not found at " + zipPath.toAbsolutePath());
        }
        try (InputStream fi = Files.newInputStream(zipPath);
             BufferedInputStream bi = new BufferedInputStream(fi);
             ZipInputStream zip = new ZipInputStream(bi)) {

            ZipEntry entry;
            while ((entry = zip.getNextEntry()) != null) {
                Path file = dataDirectory.resolve(entry.getName()).normalize();
                if (!file.startsWith(dataDirectory)) {
                    // Security check: prevent path traversal
                    throw new IOException("Bad entry: " + entry.getName());
                }
                if (entry.isDirectory()) {
                    Files.createDirectories(file);
                } else {
                    Files.createDirectories(file.getParent());
                    Files.copy(zip, file);
                }
                zip.closeEntry();
            }
        }

        log.info("Unpacked {} to {}", zipPath, dataDirectory);

    }
}
