package com.dedicatedcode.paikka.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration properties for the Paikka application.
 */
@Configuration
@ConfigurationProperties(prefix = "paikka")
public class PaikkaConfiguration {
    
    /**
     * Data directory where RocksDB databases are stored.
     */
    private String dataDir = "./data";
    
    /**
     * Maximum number of nodes to cache in Chronicle Map.
     */
    private long maxNodes = 50_000_000L;
    
    /**
     * S2 cell level for POI sharding.
     */
    private int s2Level = 14;
    
    /**
     * Maximum number of threads to use for parallel processing during import.
     * Defaults to half of available processors to prevent system overload.
     */
    private int maxImportThreads = Math.max(1, Runtime.getRuntime().availableProcessors() / 2);
    
    /**
     * Maximum number of results to return from reverse geocoding queries at most.
     */
    private int maxResults = 10;
    /**
     * Default number of results to return from reverse geocoding queries when now limit is given.
     */
    private int defaultResults = 10;
    
    /**
     * Base URL for the application, used for generating absolute URLs in responses.
     */
    private String baseUrl = "http://localhost:8080";
    
    /**
     * Path to the SQLite database file for storing API statistics.
     */
    private String statsDbPath = "./data/stats.db";

    public String getDataDir() {
        return dataDir;
    }
    
    public void setDataDir(String dataDir) {
        this.dataDir = dataDir;
    }
    
    public long getMaxNodes() {
        return maxNodes;
    }
    
    public void setMaxNodes(long maxNodes) {
        this.maxNodes = maxNodes;
    }
    
    public int getS2Level() {
        return s2Level;
    }
    
    public void setS2Level(int s2Level) {
        this.s2Level = s2Level;
    }
    
    public int getMaxImportThreads() {
        return maxImportThreads;
    }
    
    public void setMaxImportThreads(int maxImportThreads) {
        this.maxImportThreads = Math.max(1, maxImportThreads);
    }
    
    public int getMaxResults() {
        return maxResults;
    }
    
    public void setMaxResults(int maxResults) {
        this.maxResults = Math.max(1, maxResults);
    }

    public int getDefaultResults() {
        return defaultResults;
    }

    public void setDefaultResults(int defaultResults) {
        this.defaultResults = Math.max(1, defaultResults);
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public void setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }
    
    public String getStatsDbPath() {
        return statsDbPath;
    }
    
    public void setStatsDbPath(String statsDbPath) {
        this.statsDbPath = statsDbPath;
    }
}
