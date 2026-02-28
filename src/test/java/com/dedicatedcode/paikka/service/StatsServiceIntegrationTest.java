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

import com.dedicatedcode.paikka.IntegrationTest;
import com.dedicatedcode.paikka.dto.StatsAggregationResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@IntegrationTest
class StatsServiceIntegrationTest {

    @Autowired
    private StatsService statsService;

    @BeforeEach
    void setUp() {
        statsService.clearDatabase();
    }
    @Test
    void testRecordQueryAndRetrieveDailyStats() {
        // Record some queries
        statsService.recordQuery("/api/v1/search", Map.of("q", "Berlin"), 150L, 10, 200);
        statsService.recordQuery("/api/v1/search", Map.of("q", "Munich"), 200L, 5, 200);
        statsService.recordQuery("/api/v1/reverse", Map.of("lat", "52.5", "lon", "13.4"), 100L, 3, 200);

        // Wait for async processing and flush
        await().atMost(15, TimeUnit.SECONDS)
            .pollInterval(1, TimeUnit.SECONDS)
            .until(() -> {
                statsService.flushPendingStats();
                List<StatsAggregationResponse> stats = statsService.getDailyStats(
                    LocalDate.now().minusDays(1), LocalDate.now(), null);
                return stats.stream().mapToLong(StatsAggregationResponse::getQueryCount).sum() >= 3;
            });

        // Verify daily stats
        List<StatsAggregationResponse> dailyStats = statsService.getDailyStats(
            LocalDate.now().minusDays(1), LocalDate.now(), null);

        assertThat(dailyStats).isNotEmpty();
        StatsAggregationResponse todayStats = dailyStats.stream()
            .filter(s -> s.getDate().equals(LocalDate.now()))
            .findFirst()
            .orElse(null);
        assertThat(todayStats).isNotNull();
        assertThat(todayStats.getQueryCount()).isEqualTo(3);
        assertThat(todayStats.getAvgResponseTime()).isBetween(100.0, 200.0);
    }

    @Test
    void testRecordQueryWithErrorStatus() {
        statsService.recordQuery("/api/v1/search", Map.of("q", "test"), 50L, 0, 404);

        await().atMost(15, TimeUnit.SECONDS)
            .pollInterval(1, TimeUnit.SECONDS)
            .until(() -> {
                statsService.flushPendingStats();
                List<StatsAggregationResponse> stats = statsService.getDailyStats(
                    LocalDate.now().minusDays(1), LocalDate.now(), null);
                return stats.stream().anyMatch(s -> s.getErrorCount() > 0);
            });

        List<StatsAggregationResponse> dailyStats = statsService.getDailyStats(
            LocalDate.now().minusDays(1), LocalDate.now(), null);

        assertThat(dailyStats).isNotEmpty();
        assertThat(dailyStats.get(0).getErrorCount()).isGreaterThan(0);
    }

    @Test
    void testGetHourlyStats() {
        statsService.recordQuery("/api/v1/search", Map.of("q", "test"), 100L, 5, 200);

        await().atMost(15, TimeUnit.SECONDS)
            .pollInterval(1, TimeUnit.SECONDS)
            .until(() -> {
                statsService.flushPendingStats();
                List<StatsAggregationResponse> stats = statsService.getHourlyStats(
                    LocalDate.now().minusDays(1), LocalDate.now(), null);
                return !stats.isEmpty();
            });

        List<StatsAggregationResponse> hourlyStats = statsService.getHourlyStats(
            LocalDate.now().minusDays(1), LocalDate.now(), null);

        assertThat(hourlyStats).isNotEmpty();
        assertThat(hourlyStats.get(0).getHour()).isBetween(0, 23);
    }

    @Test
    void testGetAvailableEndpoints() {
        statsService.recordQuery("/api/v1/search", Map.of("q", "test"), 100L, 5, 200);
        statsService.recordQuery("/api/v1/reverse", Map.of("lat", "0", "lon", "0"), 50L, 2, 200);

        await().atMost(15, TimeUnit.SECONDS)
            .pollInterval(1, TimeUnit.SECONDS)
            .until(() -> {
                statsService.flushPendingStats();
                return !statsService.getAvailableEndpoints().isEmpty();
            });

        List<String> endpoints = statsService.getAvailableEndpoints();

        assertThat(endpoints).contains("/api/v1/search", "/api/v1/reverse");
    }

    @Test
    void testLocationStatsRecording() {
        statsService.recordQuery("/api/v1/reverse", Map.of("lat", "52.5", "lon", "13.4"), 100L, 3, 200);
        statsService.recordQuery("/api/v1/reverse", Map.of("lat", "52.5", "lon", "13.4"), 150L, 5, 200);
        statsService.recordQuery("/api/v1/reverse", Map.of("lat", "52.5", "lon", "13.4"), 150L, 5, 200);
        statsService.recordQuery("/api/v1/reverse", Map.of("lat", "52.5", "lon", "13.4"), 150L, 5, 200);
        statsService.recordQuery("/api/v1/reverse", Map.of("lat", "52.5", "lon", "13.4"), 150L, 5, 200);
        statsService.recordQuery("/api/v1/reverse", Map.of("lat", "52.5", "lon", "13.4"), 150L, 5, 200);
        statsService.recordQuery("/api/v1/reverse", Map.of("lat", "48.5", "lon", "9.5"), 80L, 2, 200);
        statsService.recordQuery("/api/v1/reverse", Map.of("lat", "48.5", "lon", "9.5"), 80L, 2, 200);
        statsService.recordQuery("/api/v1/reverse", Map.of("lat", "48.5", "lon", "9.5"), 80L, 2, 200);
        statsService.recordQuery("/api/v1/reverse", Map.of("lat", "48.5", "lon", "9.5"), 80L, 2, 200);
        statsService.recordQuery("/api/v1/reverse", Map.of("lat", "48.5", "lon", "9.5"), 80L, 2, 200);
        statsService.recordQuery("/api/v1/reverse", Map.of("lat", "48.5", "lon", "9.5"), 80L, 2, 200);

        await().atMost(15, TimeUnit.SECONDS).until(() -> {
            statsService.flushPendingStats();
            return !statsService.getLocationStats().isEmpty();
        });
        List<StatsService.LocationStatsResponse> locationStats = statsService.getLocationStats();

        assertThat(locationStats).isNotEmpty();
        // The coordinates are rounded to 0.5 degrees
        StatsService.LocationStatsResponse location52 = locationStats.stream()
            .filter(l -> Math.abs(l.lat() - 52.5) < 0.25)
            .findFirst()
            .orElse(null);
        assertThat(location52).isNotNull();

        assertThat(location52.queryCount()).isEqualTo(6);
    }

    @Test
    void testStatsAggregationWithEndpointFilter() {
        statsService.recordQuery("/api/v1/search", Map.of("q", "test1"), 100L, 5, 200);
        statsService.recordQuery("/api/v1/search", Map.of("q", "test2"), 150L, 10, 200);
        statsService.recordQuery("/api/v1/reverse", Map.of("lat", "0", "lon", "0"), 50L, 2, 200);

        await().atMost(15, TimeUnit.SECONDS)
            .pollInterval(1, TimeUnit.SECONDS)
            .until(() -> {
                statsService.flushPendingStats();
                List<StatsAggregationResponse> stats = statsService.getDailyStats(
                    LocalDate.now().minusDays(1), LocalDate.now(), "/api/v1/search");
                return stats.stream().mapToLong(StatsAggregationResponse::getQueryCount).sum() >= 2;
            });

        List<StatsAggregationResponse> searchStats = statsService.getDailyStats(
            LocalDate.now().minusDays(1), LocalDate.now(), "/api/v1/search");

        assertThat(searchStats).isNotEmpty();
        assertThat(searchStats.get(0).getQueryCount()).isEqualTo(2);

        List<StatsAggregationResponse> reverseStats = statsService.getDailyStats(
            LocalDate.now().minusDays(1), LocalDate.now(), "/api/v1/reverse");

        assertThat(reverseStats).isNotEmpty();
        assertThat(reverseStats.get(0).getQueryCount()).isEqualTo(1);
    }
}