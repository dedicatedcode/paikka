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

package com.dedicatedcode.paikka.service;

import com.dedicatedcode.paikka.service.ReverseGeocodingService.AddressData;
import com.dedicatedcode.paikka.service.ReverseGeocodingService.HierarchyData;
import com.dedicatedcode.paikka.service.ReverseGeocodingService.POIData;
import com.dedicatedcode.paikka.service.ReverseGeocodingService.POIWithDistance;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ReverseGeocodingSelectionTest {

    @Test
    void prefersNamedAndAddressedPoisOverAnonymousCluster() {
        List<POIWithDistance> candidates = new ArrayList<>();
        candidates.add(candidate(anonymous(1), 0.000));
        candidates.add(candidate(anonymous(2), 0.005));
        candidates.add(candidate(anonymous(3), 0.010));
        candidates.add(candidate(anonymous(4), 0.015));
        candidates.add(candidate(anonymous(5), 0.020));
        candidates.add(candidate(addressed(20, "Kanalstraße", "84", "23552", "Lübeck"), 0.030));
        candidates.add(candidate(named(30, "Praxis am Kanal"), 0.040));
        candidates.add(candidate(named(40, "Kanalstraße / Hundestraße"), 0.050));
        candidates.add(candidate(named(10, "P4"), 0.060));
        candidates.add(candidate(named(50, "Landwege"), 0.070));

        List<POIData> selected = ReverseGeocodingService.selectPOIs(candidates, 5);

        assertThat(selected).hasSize(5);
        assertThat(selected).extracting(POIData::id).containsExactly(20L, 30L, 40L, 10L, 50L);
        assertThat(selected).allMatch(ReverseGeocodingService::isMeaningful);
        assertThat(selected).noneMatch(poi -> poi.id() <= 5L);
    }

    @Test
    void fillsRemainingSlotsWithAnonymousPoisWhenNeeded() {
        List<POIWithDistance> candidates = List.of(
                candidate(anonymous(1), 0.001),
                candidate(anonymous(2), 0.002),
                candidate(named(20, "P4"), 0.030),
                candidate(anonymous(3), 0.003),
                candidate(anonymous(4), 0.004));

        List<POIData> selected = ReverseGeocodingService.selectPOIs(candidates, 5);

        assertThat(selected).hasSize(5);
        assertThat(selected).extracting(POIData::id).containsExactly(20L, 1L, 2L, 3L, 4L);
    }

    @Test
    void formatsAddressAsDisplayName() {
        AddressData address = new AddressData("Kanalstraße", "84", "23552", "Lübeck", "DE");

        assertThat(ReverseGeocodingService.formatAddress(address))
                .isEqualTo("Kanalstraße 84, 23552 Lübeck");
        assertThat(ReverseGeocodingService.formatAddress(
                new AddressData("Kanalstraße", null, "23552", "Lübeck", null)))
                .isEqualTo("Kanalstraße, 23552 Lübeck");
        assertThat(ReverseGeocodingService.formatAddress(
                new AddressData(null, "84", "23552", "Lübeck", null)))
                .isEqualTo("84, 23552 Lübeck");
        assertThat(ReverseGeocodingService.formatAddress(
                new AddressData(null, null, null, "Lübeck", null)))
                .isEqualTo("Lübeck");
        assertThat(ReverseGeocodingService.formatAddress(null)).isNull();
    }

    @Test
    void treatsAddressOnlyPoisAsMeaningful() {
        assertThat(ReverseGeocodingService.isMeaningful(anonymous(1))).isFalse();
        assertThat(ReverseGeocodingService.isMeaningful(named(2, "P4"))).isTrue();
        assertThat(ReverseGeocodingService.isMeaningful(
                addressed(3, "Kanalstraße", "84", "23552", "Lübeck"))).isTrue();
    }

    private static POIWithDistance candidate(POIData poi, double distance) {
        return new POIWithDistance(poi, distance);
    }

    private static POIData anonymous(long id) {
        return poi(id, Map.of(), null);
    }

    private static POIData named(long id, String name) {
        return poi(id, Map.of("default", name), null);
    }

    private static POIData addressed(long id, String street, String houseNumber, String postcode, String city) {
        return poi(id, Map.of(), new AddressData(street, houseNumber, postcode, city, "DE"));
    }

    private static POIData poi(long id, Map<String, String> names, AddressData address) {
        return new POIData(id, 53.8676f, 10.6958f, "amenity", "charging_station",
                           names, address, List.<HierarchyData>of(), null);
    }
}
