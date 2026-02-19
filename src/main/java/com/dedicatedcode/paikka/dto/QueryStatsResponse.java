package com.dedicatedcode.paikka.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Map;

public class QueryStatsResponse {
    
    @JsonProperty("id")
    private Long id;
    
    @JsonProperty("timestamp")
    private LocalDateTime timestamp;
    
    @JsonProperty("endpoint")
    private String endpoint;
    
    @JsonProperty("parameters")
    private Map<String, String> parameters;
    
    @JsonProperty("responseTimeMs")
    private Long responseTimeMs;
    
    @JsonProperty("resultCount")
    private Integer resultCount;
    
    @JsonProperty("clientIp")
    private String clientIp;
    
    @JsonProperty("dateOnly")
    private LocalDate dateOnly;
    
    @JsonProperty("hourBucket")
    private Integer hourBucket;
    
    public QueryStatsResponse() {}
    
    public QueryStatsResponse(Long id, LocalDateTime timestamp, String endpoint, 
                             Map<String, String> parameters, Long responseTimeMs, 
                             Integer resultCount, String clientIp, LocalDate dateOnly, 
                             Integer hourBucket) {
        this.id = id;
        this.timestamp = timestamp;
        this.endpoint = endpoint;
        this.parameters = parameters;
        this.responseTimeMs = responseTimeMs;
        this.resultCount = resultCount;
        this.clientIp = clientIp;
        this.dateOnly = dateOnly;
        this.hourBucket = hourBucket;
    }
    
    // Getters and setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    
    public LocalDateTime getTimestamp() { return timestamp; }
    public void setTimestamp(LocalDateTime timestamp) { this.timestamp = timestamp; }
    
    public String getEndpoint() { return endpoint; }
    public void setEndpoint(String endpoint) { this.endpoint = endpoint; }
    
    public Map<String, String> getParameters() { return parameters; }
    public void setParameters(Map<String, String> parameters) { this.parameters = parameters; }
    
    public Long getResponseTimeMs() { return responseTimeMs; }
    public void setResponseTimeMs(Long responseTimeMs) { this.responseTimeMs = responseTimeMs; }
    
    public Integer getResultCount() { return resultCount; }
    public void setResultCount(Integer resultCount) { this.resultCount = resultCount; }
    
    public String getClientIp() { return clientIp; }
    public void setClientIp(String clientIp) { this.clientIp = clientIp; }
    
    public LocalDate getDateOnly() { return dateOnly; }
    public void setDateOnly(LocalDate dateOnly) { this.dateOnly = dateOnly; }
    
    public Integer getHourBucket() { return hourBucket; }
    public void setHourBucket(Integer hourBucket) { this.hourBucket = hourBucket; }
}
