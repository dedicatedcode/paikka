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

package com.dedicatedcode.paikka.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDate;

public class StatsAggregationResponse {
    
    @JsonProperty("date")
    private LocalDate date;
    
    @JsonProperty("hour")
    private Integer hour;
    
    @JsonProperty("queryCount")
    private Long queryCount;
    
    @JsonProperty("avgResponseTime")
    private Double avgResponseTime;
    
    @JsonProperty("avgResultCount")
    private Double avgResultCount;
    
    @JsonProperty("maxResponseTime")
    private Long maxResponseTime;
    
    @JsonProperty("minResponseTime")
    private Long minResponseTime;
    
    @JsonProperty("successCount")
    private Long successCount;
    
    @JsonProperty("errorCount")
    private Long errorCount;
    
    public StatsAggregationResponse() {}
    
    public StatsAggregationResponse(LocalDate date, Integer hour, Long queryCount, 
                                   Double avgResponseTime, Double avgResultCount,
                                   Long maxResponseTime, Long minResponseTime,
                                   Long successCount, Long errorCount) {
        this.date = date;
        this.hour = hour;
        this.queryCount = queryCount;
        this.avgResponseTime = avgResponseTime;
        this.avgResultCount = avgResultCount;
        this.maxResponseTime = maxResponseTime;
        this.minResponseTime = minResponseTime;
        this.successCount = successCount;
        this.errorCount = errorCount;
    }
    
    // Getters and setters
    public LocalDate getDate() { return date; }
    public void setDate(LocalDate date) { this.date = date; }
    
    public Integer getHour() { return hour; }
    public void setHour(Integer hour) { this.hour = hour; }
    
    public Long getQueryCount() { return queryCount; }
    public void setQueryCount(Long queryCount) { this.queryCount = queryCount; }
    
    public Double getAvgResponseTime() { return avgResponseTime; }
    public void setAvgResponseTime(Double avgResponseTime) { this.avgResponseTime = avgResponseTime; }
    
    public Double getAvgResultCount() { return avgResultCount; }
    public void setAvgResultCount(Double avgResultCount) { this.avgResultCount = avgResultCount; }
    
    public Long getMaxResponseTime() { return maxResponseTime; }
    public void setMaxResponseTime(Long maxResponseTime) { this.maxResponseTime = maxResponseTime; }
    
    public Long getMinResponseTime() { return minResponseTime; }
    public void setMinResponseTime(Long minResponseTime) { this.minResponseTime = minResponseTime; }
    
    public Long getSuccessCount() { return successCount; }
    public void setSuccessCount(Long successCount) { this.successCount = successCount; }
    
    public Long getErrorCount() { return errorCount; }
    public void setErrorCount(Long errorCount) { this.errorCount = errorCount; }
}
