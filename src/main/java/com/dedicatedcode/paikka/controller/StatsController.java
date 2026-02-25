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

package com.dedicatedcode.paikka.controller;

import com.dedicatedcode.paikka.dto.StatsAggregationResponse;
import com.dedicatedcode.paikka.service.StatsService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.time.LocalDate;
import java.util.List;

@Controller
@RequestMapping("/admin")
@ConditionalOnProperty(name = "paikka.import-mode", havingValue = "false", matchIfMissing = true)
public class StatsController {
    
    private final StatsService statsService;
    
    public StatsController(StatsService statsService) {
        this.statsService = statsService;
    }
    
    @GetMapping("/stats")
    public String stats(
            @RequestParam(defaultValue = "") String startDate,
            @RequestParam(defaultValue = "") String endDate,
            @RequestParam(defaultValue = "") String endpoint,
            @RequestParam(defaultValue = "daily") String view,
            Model model) {
        
        // Set default dates if not provided
        LocalDate start = startDate.isEmpty() ? LocalDate.now().minusDays(30) : LocalDate.parse(startDate);
        LocalDate end = endDate.isEmpty() ? LocalDate.now() : LocalDate.parse(endDate);
        
        // Get available endpoints
        List<String> availableEndpoints = statsService.getAvailableEndpoints();
        
        List<StatsAggregationResponse> stats;
        if ("hourly".equals(view)) {
            // For hourly view, use the date range
            stats = statsService.getHourlyStats(start, end, endpoint.isEmpty() ? null : endpoint);
        } else {
            // Daily view
            stats = statsService.getDailyStats(start, end, endpoint.isEmpty() ? null : endpoint);
        }
        
        model.addAttribute("stats", stats);
        model.addAttribute("availableEndpoints", availableEndpoints);
        model.addAttribute("startDate", start.toString());
        model.addAttribute("endDate", end.toString());
        model.addAttribute("selectedEndpoint", endpoint);
        model.addAttribute("view", view);
        
        return "stats";
    }
    
    @GetMapping("/map")
    public String map() {
        return "map";
    }
    
    @GetMapping("/api/location-stats")
    @ResponseBody
    public ResponseEntity<List<StatsService.LocationStatsResponse>> getLocationStats() {
        List<StatsService.LocationStatsResponse> locationStats = statsService.getLocationStats();
        return ResponseEntity.ok(locationStats);
    }
}
