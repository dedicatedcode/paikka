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

import com.fasterxml.jackson.annotation.JsonProperty;

record PaikkaMetadata(
        @JsonProperty("importTimestamp") String importTimestamp,
        @JsonProperty("dataVersion") String dataVersion,
        @JsonProperty("file") String file,
        @JsonProperty("gridLevel") Integer gridLevel,
        @JsonProperty("paikkaVersion") String paikkaVersion
) {
}
