/*
 *  This file is part of paikka.
 *
 *  Paikka is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  Paikka is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with Paikka. If not, see <https://www.gnu.org/licenses/>.
 */

package com.dedicatedcode.paikka.service.importer;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Country assignment used to run inheritance before applying country hints. A region whose own
 * country only came from a hint (for example a federal subject with ISO3166-2 but no resolvable
 * parent) therefore could not pass that country on to its children, and the whole subtree was
 * dropped. These tests pin down the fixed behaviour.
 */
class RegionHierarchyCountryAssignmentTest {

    private static RegionHierarchyWriter.RegionInfo region(long osmId, int adminLevel) {
        return new RegionHierarchyWriter.RegionInfo(osmId, adminLevel, 9, 1);
    }

    @Test
    void childrenInheritCountryFromParentThatOnlyHasAHint() {
        List<RegionHierarchyWriter.RegionInfo> regions = List.of(
                region(1, 2), region(2, 4), region(3, 6), region(4, 8));
        Map<Long, Long> parents = Map.of(2L, 1L, 3L, 2L, 4L, 3L);
        Map<Long, String> countries = new HashMap<>();
        countries.put(1L, "RU");

        RegionHierarchyWriter.inheritCountries(regions, parents, countries, Map.of(2L, "RU"));

        assertEquals("RU", countries.get(3L));
        assertEquals("RU", countries.get(4L));
    }

    @Test
    void hintOnlyParentWithoutTreeParentStillPassesCountryDown() {
        // Kabardino-Balkaria (RU-KB) has 13 subareas but its own parent could not be resolved
        // from the clipped Europe extract. Its hint must still reach the children.
        List<RegionHierarchyWriter.RegionInfo> regions = List.of(
                region(10, 2), region(20, 4), region(30, 6));
        Map<Long, Long> parents = Map.of(30L, 20L);
        Map<Long, String> countries = new HashMap<>();
        countries.put(10L, "RU");

        RegionHierarchyWriter.inheritCountries(regions, parents, countries, Map.of(20L, "RU"));

        assertEquals("RU", countries.get(30L));
    }

    @Test
    void inheritedCountryWinsOverOwnHint() {
        List<RegionHierarchyWriter.RegionInfo> regions = List.of(region(1, 2), region(2, 6));
        Map<Long, String> countries = new HashMap<>();
        countries.put(1L, "AT");

        RegionHierarchyWriter.inheritCountries(regions, Map.of(2L, 1L), countries, Map.of(2L, "DE"));

        assertEquals("AT", countries.get(2L));
    }

    @Test
    void regionWithoutCountryAncestorStaysUnassigned() {
        List<RegionHierarchyWriter.RegionInfo> regions = List.of(region(1, 2), region(2, 6));
        Map<Long, String> countries = new HashMap<>();
        countries.put(1L, "AT");

        RegionHierarchyWriter.inheritCountries(regions, Map.of(), countries, Map.of());

        assertFalse(countries.containsKey(2L));
    }
}
