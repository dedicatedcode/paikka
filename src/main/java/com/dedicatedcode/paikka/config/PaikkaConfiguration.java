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

package com.dedicatedcode.paikka.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.boot.context.properties.bind.Name;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "paikka")
public class PaikkaConfiguration {
    
    private String dataDir = "./data";

    private String statsDbPath = "./data/stats.db";
    @Name("import")
    private ImportConfiguration importConfiguration;
    @Name("query")
    private QueryConfiguration queryConfiguration;

    public ImportConfiguration getImportConfiguration() {
        return importConfiguration;
    }

    public void setImportConfiguration(ImportConfiguration importConfiguration) {
        this.importConfiguration = importConfiguration;
    }

    public QueryConfiguration getQueryConfiguration() {
        return queryConfiguration;
    }

    public void setQueryConfiguration(QueryConfiguration queryConfiguration) {
        this.queryConfiguration = queryConfiguration;
    }

    public String getDataDir() {
        return dataDir;
    }
    
    public void setDataDir(String dataDir) {
        this.dataDir = dataDir;
    }

    public String getStatsDbPath() {
        return statsDbPath;
    }
    
    public void setStatsDbPath(String statsDbPath) {
        this.statsDbPath = statsDbPath;
    }

    public static class ImportConfiguration {

        private int threads = Math.max(1, Runtime.getRuntime().availableProcessors() / 2);
        private int chunkSize = 500_000;

        public int getThreads() {
            return threads;
        }

        public void setThreads(int threads) {
            this.threads = threads;
        }

        public int getChunkSize() {
            return chunkSize;
        }

        public void setChunkSize(int chunkSize) {
            this.chunkSize = chunkSize;
        }

    }

    public static class QueryConfiguration {

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

    }
}
