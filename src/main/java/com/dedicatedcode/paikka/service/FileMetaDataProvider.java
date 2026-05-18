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

import com.dedicatedcode.paikka.config.PaikkaConfiguration;
import org.springframework.stereotype.Service;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;

@Service
public class FileMetaDataProvider implements MetaDataProvider {
    private static final String METADATA_FILE_NAME = "paikka_metadata.json";

    private final PaikkaConfiguration configuration;
    private final Path metadataPath;

    public FileMetaDataProvider(PaikkaConfiguration configuration) {
        this.configuration = configuration;
        this.metadataPath = Paths.get(configuration.getDataDir(), METADATA_FILE_NAME);
    }

    @Override
    public boolean exists() {
        return false;
    }

    @Override
    public InputStream get() throws FileNotFoundException {
        return new FileInputStream(metadataPath.toFile());
    }
}
