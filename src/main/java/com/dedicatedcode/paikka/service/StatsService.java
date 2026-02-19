package com.dedicatedcode.paikka.service;

import com.dedicatedcode.paikka.config.PaikkaConfiguration;
import com.dedicatedcode.paikka.dto.StatsAggregationResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.File;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

@Service
@ConditionalOnProperty(name = "paikka.import-mode", havingValue = "false", matchIfMissing = true)
public class StatsService {
    
    private static final Logger logger = LoggerFactory.getLogger(StatsService.class);
    
    private final PaikkaConfiguration config;
    private final ObjectMapper objectMapper;
    private final Queue<StatsRecord> pendingStats = new ConcurrentLinkedQueue<>();
    
    private Connection connection;
    
    public StatsService(PaikkaConfiguration config) {
        this.config = config;
        this.objectMapper = new ObjectMapper();
    }
    
    @PostConstruct
    public void initialize() {
        try {
            initializeDatabase();
            logger.info("Stats service initialized with database at: {}", config.getStatsDbPath());
        } catch (Exception e) {
            logger.error("Failed to initialize stats service", e);
        }
    }
    
    @PreDestroy
    public void cleanup() {
        try {
            flushPendingStats();
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            logger.error("Error closing stats database connection", e);
        }
    }
    
    private void initializeDatabase() throws SQLException {
        // Ensure directory exists
        File dbFile = new File(config.getStatsDbPath());
        dbFile.getParentFile().mkdirs();
        
        // Connect to SQLite database
        String url = "jdbc:sqlite:" + config.getStatsDbPath();
        connection = DriverManager.getConnection(url);
        
        // Create table if not exists
        String createTableSQL = """
            CREATE TABLE IF NOT EXISTS query_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                endpoint VARCHAR(100),
                parameters TEXT,
                response_time_ms INTEGER,
                result_count INTEGER,
                client_ip VARCHAR(45),
                date_only DATE,
                hour_bucket INTEGER,
                status_code INTEGER
            )
            """;
        
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(createTableSQL);
            
            // Add status_code column if it doesn't exist (for existing databases)
            try {
                stmt.execute("ALTER TABLE query_stats ADD COLUMN status_code INTEGER DEFAULT 200");
            } catch (SQLException e) {
                // Column already exists, ignore
                logger.debug("status_code column already exists or could not be added: {}", e.getMessage());
            }
            
            // Create indexes
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_stats_date ON query_stats(date_only)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_stats_endpoint_date ON query_stats(endpoint, date_only)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_stats_hour ON query_stats(date_only, hour_bucket)");
        }
        
        // Create location stats table
        String createLocationTableSQL = """
            CREATE TABLE IF NOT EXISTS location_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rounded_lat REAL NOT NULL,
                rounded_lon REAL NOT NULL,
                query_count INTEGER DEFAULT 1,
                last_queried DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(rounded_lat, rounded_lon)
            )
            """;
        
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(createLocationTableSQL);
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_location_coords ON location_stats(rounded_lat, rounded_lon)");
        }
    }
    
    @Async
    public void recordQuery(String endpoint, Map<String, String> sortedParams, 
                           long responseTimeMs, int resultCount, String clientIp, int statusCode) {
        try {
            String parametersJson = objectMapper.writeValueAsString(sortedParams);
            LocalDateTime now = LocalDateTime.now();
            
            StatsRecord record = new StatsRecord(
                endpoint,
                parametersJson,
                responseTimeMs,
                resultCount,
                clientIp,
                now.toLocalDate(),
                now.getHour(),
                statusCode
            );
            
            pendingStats.offer(record);
            
            // Record location if this is a reverse geocoding query
            if ("/api/v1/reverse".equals(endpoint) && sortedParams.containsKey("lat") && sortedParams.containsKey("lon")) {
                try {
                    double lat = Double.parseDouble(sortedParams.get("lat"));
                    double lon = Double.parseDouble(sortedParams.get("lon"));
                    recordLocation(lat, lon);
                } catch (NumberFormatException e) {
                    logger.debug("Invalid lat/lon parameters for location tracking: {}", e.getMessage());
                }
            }
            
        } catch (JsonProcessingException e) {
            logger.error("Failed to serialize parameters for stats", e);
        } catch (Exception e) {
            logger.error("Failed to record query stats", e);
        }
    }
    
    private void recordLocation(double lat, double lon) {
        // Round coordinates to ~50km precision (approximately 0.5 degrees)
        double roundedLat = Math.round(lat * 2.0) / 2.0;
        double roundedLon = Math.round(lon * 2.0) / 2.0;
        
        String upsertSQL = """
            INSERT INTO location_stats (rounded_lat, rounded_lon, query_count, last_queried) 
            VALUES (?, ?, 1, CURRENT_TIMESTAMP)
            ON CONFLICT(rounded_lat, rounded_lon) 
            DO UPDATE SET 
                query_count = query_count + 1,
                last_queried = CURRENT_TIMESTAMP
            """;
        
        try (PreparedStatement pstmt = connection.prepareStatement(upsertSQL)) {
            pstmt.setDouble(1, roundedLat);
            pstmt.setDouble(2, roundedLon);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            logger.error("Failed to record location stats", e);
        }
    }
    
    @Scheduled(fixedDelay = 10000) // Every 10 seconds
    public void flushPendingStats() {
        if (pendingStats.isEmpty() || connection == null) {
            return;
        }
        
        List<StatsRecord> batch = new ArrayList<>();
        StatsRecord record;
        while ((record = pendingStats.poll()) != null && batch.size() < 1000) {
            batch.add(record);
        }
        
        if (batch.isEmpty()) {
            return;
        }
        
        String insertSQL = """
            INSERT INTO query_stats (endpoint, parameters, response_time_ms, result_count, 
                                   client_ip, date_only, hour_bucket, status_code) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """;
        
        try (PreparedStatement pstmt = connection.prepareStatement(insertSQL)) {
            for (StatsRecord statsRecord : batch) {
                pstmt.setString(1, statsRecord.endpoint);
                pstmt.setString(2, statsRecord.parametersJson);
                pstmt.setLong(3, statsRecord.responseTimeMs);
                pstmt.setInt(4, statsRecord.resultCount);
                pstmt.setString(5, statsRecord.clientIp);
                pstmt.setString(6, statsRecord.dateOnly.toString());
                pstmt.setInt(7, statsRecord.hourBucket);
                pstmt.setInt(8, statsRecord.statusCode);
                pstmt.addBatch();
            }
            pstmt.executeBatch();
            logger.debug("Flushed {} stats records to database", batch.size());
        } catch (SQLException e) {
            logger.error("Failed to flush stats to database", e);
            // Re-add failed records to queue
            batch.forEach(pendingStats::offer);
        }
    }
    
    public List<StatsAggregationResponse> getDailyStats(LocalDate startDate, LocalDate endDate, String endpoint) {
        String sql = """
            SELECT date_only, COUNT(*) as query_count, 
                   AVG(response_time_ms) as avg_response_time,
                   AVG(result_count) as avg_result_count,
                   MAX(response_time_ms) as max_response_time,
                   MIN(response_time_ms) as min_response_time,
                   SUM(CASE WHEN status_code >= 200 AND status_code < 300 THEN 1 ELSE 0 END) as success_count,
                   SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) as error_count
            FROM query_stats 
            WHERE date_only BETWEEN ? AND ?
            """ + (endpoint != null && !endpoint.isEmpty() ? " AND endpoint = ?" : "") + """
            GROUP BY date_only
            ORDER BY date_only
            """;
        
        List<StatsAggregationResponse> results = new ArrayList<>();
        
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, startDate.toString());
            pstmt.setString(2, endDate.toString());
            if (endpoint != null && !endpoint.isEmpty()) {
                pstmt.setString(3, endpoint);
            }
            
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    results.add(new StatsAggregationResponse(
                        LocalDate.parse(rs.getString("date_only")),
                        null,
                        rs.getLong("query_count"),
                        rs.getDouble("avg_response_time"),
                        rs.getDouble("avg_result_count"),
                        rs.getLong("max_response_time"),
                        rs.getLong("min_response_time"),
                        rs.getLong("success_count"),
                        rs.getLong("error_count")
                    ));
                }
            }
        } catch (SQLException e) {
            logger.error("Failed to retrieve daily stats", e);
        }
        
        return results;
    }
    
    public List<StatsAggregationResponse> getHourlyStats(LocalDate startDate, LocalDate endDate, String endpoint) {
        String sql = """
            SELECT date_only, hour_bucket, COUNT(*) as query_count, 
                   AVG(response_time_ms) as avg_response_time,
                   AVG(result_count) as avg_result_count,
                   MAX(response_time_ms) as max_response_time,
                   MIN(response_time_ms) as min_response_time,
                   SUM(CASE WHEN status_code >= 200 AND status_code < 300 THEN 1 ELSE 0 END) as success_count,
                   SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) as error_count
            FROM query_stats 
            WHERE date_only BETWEEN ? AND ?
            """ + (endpoint != null && !endpoint.isEmpty() ? " AND endpoint = ?" : "") + """
            GROUP BY date_only, hour_bucket
            ORDER BY date_only, hour_bucket
            """;
        
        List<StatsAggregationResponse> results = new ArrayList<>();
        
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, startDate.toString());
            pstmt.setString(2, endDate.toString());
            if (endpoint != null && !endpoint.isEmpty()) {
                pstmt.setString(3, endpoint);
            }
            
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    results.add(new StatsAggregationResponse(
                        LocalDate.parse(rs.getString("date_only")),
                        rs.getInt("hour_bucket"),
                        rs.getLong("query_count"),
                        rs.getDouble("avg_response_time"),
                        rs.getDouble("avg_result_count"),
                        rs.getLong("max_response_time"),
                        rs.getLong("min_response_time"),
                        rs.getLong("success_count"),
                        rs.getLong("error_count")
                    ));
                }
            }
        } catch (SQLException e) {
            logger.error("Failed to retrieve hourly stats", e);
        }
        
        return results;
    }
    
    public List<String> getAvailableEndpoints() {
        String sql = "SELECT DISTINCT endpoint FROM query_stats ORDER BY endpoint";
        List<String> endpoints = new ArrayList<>();
        
        try (PreparedStatement pstmt = connection.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {
            
            while (rs.next()) {
                endpoints.add(rs.getString("endpoint"));
            }
        } catch (SQLException e) {
            logger.error("Failed to retrieve available endpoints", e);
        }
        
        return endpoints;
    }
    
    public List<LocationStatsResponse> getLocationStats() {
        String sql = """
            SELECT rounded_lat, rounded_lon, query_count, last_queried 
            FROM location_stats 
            WHERE query_count >= 5
            ORDER BY query_count DESC 
            LIMIT 1000
            """;
        
        List<LocationStatsResponse> results = new ArrayList<>();
        
        try (PreparedStatement pstmt = connection.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {
            
            while (rs.next()) {
                results.add(new LocationStatsResponse(
                    rs.getDouble("rounded_lat"),
                    rs.getDouble("rounded_lon"),
                    rs.getInt("query_count"),
                    rs.getString("last_queried")
                ));
            }
        } catch (SQLException e) {
            logger.error("Failed to retrieve location stats", e);
        }
        
        return results;
    }
    
    @Scheduled(cron = "0 0 2 * * ?") // Daily at 2 AM
    public void cleanupOldStats() {
        LocalDate cutoffDate = LocalDate.now().minusYears(1);
        String sql = "DELETE FROM query_stats WHERE date_only < ?";
        
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, cutoffDate.toString());
            int deleted = pstmt.executeUpdate();
            if (deleted > 0) {
                logger.info("Cleaned up {} old stats records older than {}", deleted, cutoffDate);
            }
        } catch (SQLException e) {
            logger.error("Failed to cleanup old stats", e);
        }
        
        // Also cleanup old location stats with low query counts
        String locationCleanupSQL = "DELETE FROM location_stats WHERE query_count < 3 AND last_queried < datetime('now', '-30 days')";
        try (PreparedStatement pstmt = connection.prepareStatement(locationCleanupSQL)) {
            int deleted = pstmt.executeUpdate();
            if (deleted > 0) {
                logger.info("Cleaned up {} old location stats records", deleted);
            }
        } catch (SQLException e) {
            logger.error("Failed to cleanup old location stats", e);
        }
    }
    
    public static class LocationStatsResponse {
        private final double lat;
        private final double lon;
        private final int queryCount;
        private final String lastQueried;
        
        public LocationStatsResponse(double lat, double lon, int queryCount, String lastQueried) {
            this.lat = lat;
            this.lon = lon;
            this.queryCount = queryCount;
            this.lastQueried = lastQueried;
        }
        
        public double getLat() { return lat; }
        public double getLon() { return lon; }
        public int getQueryCount() { return queryCount; }
        public String getLastQueried() { return lastQueried; }
    }
    
    private static class StatsRecord {
        final String endpoint;
        final String parametersJson;
        final long responseTimeMs;
        final int resultCount;
        final String clientIp;
        final LocalDate dateOnly;
        final int hourBucket;
        final int statusCode;
        
        StatsRecord(String endpoint, String parametersJson, long responseTimeMs, 
                   int resultCount, String clientIp, LocalDate dateOnly, int hourBucket, int statusCode) {
            this.endpoint = endpoint;
            this.parametersJson = parametersJson;
            this.responseTimeMs = responseTimeMs;
            this.resultCount = resultCount;
            this.clientIp = clientIp;
            this.dateOnly = dateOnly;
            this.hourBucket = hourBucket;
            this.statusCode = statusCode;
        }
    }
}
