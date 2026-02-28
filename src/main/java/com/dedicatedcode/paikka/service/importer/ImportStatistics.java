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

package com.dedicatedcode.paikka.service.importer;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

class ImportStatistics {
    private final AtomicLong entitiesRead = new AtomicLong(0);
    private final AtomicLong nodesCached = new AtomicLong(0);
    private final AtomicLong nodesFound = new AtomicLong(0);
    private final AtomicLong waysProcessed = new AtomicLong(0);
    private final AtomicLong relationsFound = new AtomicLong(0);
    private final AtomicLong boundariesProcessed = new AtomicLong(0);
    private final AtomicLong poisProcessed = new AtomicLong(0);
    private final AtomicLong poiIndexRecRead = new AtomicLong(0);
    private final AtomicBoolean poiIndexRecReadDone = new AtomicBoolean(false);
    private final AtomicLong rocksDbWrites = new AtomicLong(0);
    private final AtomicLong queueSize = new AtomicLong(0);
    private final AtomicLong activeThreads = new AtomicLong(0);
    private volatile String currentPhase = "Initializing";
    private volatile boolean running = true;
    private final long startTime = System.currentTimeMillis();
    private volatile long phaseStartTime = System.currentTimeMillis();
    private long totalTime;
    private final AtomicLong shardsCompacted = new AtomicLong(0);
    private volatile long compactionStartTime = 0;

    private volatile long datasetBytes;
    private volatile long shardsBytes;
    private volatile long boundariesBytes;
    private volatile long tmpGridBytes;
    private volatile long tmpNodeBytes;
    private volatile long tmpWayBytes;
    private volatile long tmpNeededBytes;
    private volatile long tmpRelBytes;
    private volatile long tmpPoiBytes;
    private volatile long tmpTotalBytes;
    private volatile long tmpAppendBytes;

    private final int TOTAL_STEPS = 5;
    private int currentStep = 0;

    public long getEntitiesRead() {
        return entitiesRead.get();
    }

    public void incrementEntitiesRead() {
        entitiesRead.incrementAndGet();
    }

    public long getNodesCached() {
        return nodesCached.get();
    }

    public void incrementNodesCached() {
        nodesCached.incrementAndGet();
    }

    public long getNodesFound() {
        return nodesFound.get();
    }

    public void incrementNodesFound() {
        nodesFound.incrementAndGet();
    }

    public long getWaysProcessed() {
        return waysProcessed.get();
    }

    public void incrementWaysProcessed() {
        waysProcessed.incrementAndGet();
    }

    public long getRelationsFound() {
        return relationsFound.get();
    }

    public void incrementRelationsFound() {
        relationsFound.incrementAndGet();
    }

    public long getBoundariesProcessed() {
        return boundariesProcessed.get();
    }

    public void incrementBoundariesProcessed() {
        boundariesProcessed.incrementAndGet();
    }

    public long getPoisProcessed() {
        return poisProcessed.get();
    }

    public void incrementPoisProcessed(int count) {
        poisProcessed.addAndGet(count);
    }

    public long getPoiIndexRecRead() {
        return poiIndexRecRead.get();
    }

    public void incrementPoiIndexRecRead() {
        poiIndexRecRead.incrementAndGet();
    }

    public boolean isPoiIndexRecReadDone() {
        return poiIndexRecReadDone.get();
    }

    public void setPoiIndexRecReadDone() {
        poiIndexRecReadDone.set(true);
    }

    public long getRocksDbWrites() {
        return rocksDbWrites.get();
    }

    public void incrementRocksDbWrites() {
        rocksDbWrites.incrementAndGet();
    }

    public int getQueueSize() {
        return (int) queueSize.get();
    }

    public void setQueueSize(int size) {
        queueSize.set(size);
    }

    public int getActiveThreads() {
        return (int) activeThreads.get();
    }

    public void incrementActiveThreads() {
        activeThreads.incrementAndGet();
    }

    public void decrementActiveThreads() {
        activeThreads.decrementAndGet();
    }

    public String getCurrentPhase() {
        return currentPhase;
    }

    public void setCurrentPhase(int step, String phase) {
        this.currentPhase = phase;
        this.phaseStartTime = System.currentTimeMillis();
        this.currentStep = step;
    }

    public long getPhaseStartTime() {
        return phaseStartTime;
    }

    public boolean isRunning() {
        return running;
    }

    public void stop() {
        this.running = false;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getTotalTime() {
        return totalTime;
    }

    public void setTotalTime(long t) {
        this.totalTime = t;
    }

    public long getShardsCompacted() {
        return shardsCompacted.get();
    }

    public void incrementShardsCompacted() {
        shardsCompacted.incrementAndGet();
    }

    public void setCompactionStartTime(long t) {
        this.compactionStartTime = t;
    }

    public long getCompactionStartTime() {
        return compactionStartTime;
    }

    public long getDatasetBytes() {
        return datasetBytes;
    }

    public void setDatasetBytes(long v) {
        this.datasetBytes = v;
    }

    public long getShardsBytes() {
        return shardsBytes;
    }

    public void setShardsBytes(long v) {
        this.shardsBytes = v;
    }

    public long getBoundariesBytes() {
        return boundariesBytes;
    }

    public void setBoundariesBytes(long v) {
        this.boundariesBytes = v;
    }

    public long getTmpGridBytes() {
        return tmpGridBytes;
    }

    public void setTmpGridBytes(long v) {
        this.tmpGridBytes = v;
    }

    public long getTmpNodeBytes() {
        return tmpNodeBytes;
    }

    public void setTmpNodeBytes(long v) {
        this.tmpNodeBytes = v;
    }

    public long getTmpWayBytes() {
        return tmpWayBytes;
    }

    public void setTmpWayBytes(long v) {
        this.tmpWayBytes = v;
    }

    public long getTmpNeededBytes() {
        return tmpNeededBytes;
    }

    public void setTmpNeededBytes(long v) {
        this.tmpNeededBytes = v;
    }

    public long getTmpRelBytes() {
        return tmpRelBytes;
    }

    public void setTmpRelBytes(long v) {
        this.tmpRelBytes = v;
    }

    public long getTmpPoiBytes() {
        return tmpPoiBytes;
    }

    public void setTmpPoiBytes(long v) {
        this.tmpPoiBytes = v;
    }

    public long getTmpAppendBytes() {
        return tmpAppendBytes;
    }

    public void setTmpAppendBytes(long v) {
        this.tmpAppendBytes = v;
    }

    public long getTmpTotalBytes() {
        return tmpTotalBytes;
    }

    public void setTmpTotalBytes(long v) {
        this.tmpTotalBytes = v;
    }

    public String getMemoryStats() {
        Runtime r = Runtime.getRuntime();
        long used = (r.totalMemory() - r.freeMemory()) / 1024 / 1024 / 1024;
        long max = r.maxMemory() / 1024 / 1024 / 1024;
        return String.format("%dGB/%dGB", used, max);
    }

    public void startProgressReporter() {
        boolean isTty = System.console() != null;

        Thread.ofPlatform().daemon().start(() -> {
            while (isRunning()) {
                long elapsed = System.currentTimeMillis() - getStartTime();
                long phaseElapsed = System.currentTimeMillis() - getPhaseStartTime();
                double phaseSeconds = phaseElapsed / 1000.0;

                String phase = getCurrentPhase();
                StringBuilder sb = new StringBuilder();

                if (isTty) {
                    sb.append("\r\033[K");
                }

                // Add step indicator
                sb.append(String.format("\033[1;90m[%d/%d]\033[0m ", currentStep, TOTAL_STEPS));

                if (phase.contains("1.1.1")) {
                    long pbfPerSec = phaseSeconds > 0 ? (long)(getEntitiesRead() / phaseSeconds) : 0;
                    sb.append(String.format("\033[1;36m[%s]\033[0m \033[1mScanning PBF Structure\033[0m", formatTime(elapsed)));
                    sb.append(String.format(" │ \033[32mPBF Entities:\033[0m %s \033[33m(%s/s)\033[0m",
                                            formatCompactNumber(getEntitiesRead()), formatCompactRate(pbfPerSec)));
                    sb.append(String.format(" │ \033[37mNodes Found:\033[0m %s", formatCompactNumber(getNodesFound())));
                    sb.append(String.format(" │ \033[34mWays Found:\033[0m %s", formatCompactNumber(getWaysProcessed())));
                    sb.append(String.format(" │ \033[35mRelations:\033[0m %s", formatCompactNumber(getRelationsFound())));

                } else if (phase.contains("1.1.2")) {
                    long nodesPerSec = phaseSeconds > 0 ? (long)(getNodesCached() / phaseSeconds) : 0;
                    sb.append(String.format("\033[1;36m[%s]\033[0m \033[1mCaching Node Coordinates\033[0m", formatTime(elapsed)));
                    sb.append(String.format(" │ \033[32mNodes Cached:\033[0m %s \033[33m(%s/s)\033[0m",
                                            formatCompactNumber(getNodesCached()), formatCompactRate(nodesPerSec)));
                    sb.append(String.format(" │ \033[36mQueue:\033[0m %s", formatCompactNumber(getQueueSize())));
                    sb.append(String.format(" │ \033[37mThreads:\033[0m %d", getActiveThreads()));

                } else if (phase.contains("1.2")) {
                    long boundsPerSec = phaseSeconds > 0 ? (long)(getBoundariesProcessed() / phaseSeconds) : 0;
                    sb.append(String.format("\033[1;36m[%s]\033[0m \033[1mProcessing Admin Boundaries\033[0m", formatTime(elapsed)));
                    sb.append(String.format(" │ \033[32mBoundaries:\033[0m %s \033[33m(%s/s)\033[0m",
                                            formatCompactNumber(getBoundariesProcessed()), formatCompactRate(boundsPerSec)));
                    sb.append(String.format(" │ \033[37mThreads:\033[0m %d", getActiveThreads()));

                } else if (phase.contains("2.1")) {
                    long poisPerSec = phaseSeconds > 0 ? (long)(getPoisProcessed() / phaseSeconds) : 0;
                    long poisReadSec = phaseSeconds > 0 ? (long)(getPoiIndexRecRead() / phaseSeconds) : 0;
                    sb.append(String.format("\033[1;36m[%s]\033[0m \033[1mProcessing POIs & Sharding\033[0m", formatTime(elapsed)));
                    sb.append(String.format(" │ \033[32mPOI Index Rec Read:\033[0m %s \033[33m%s\033[0m", formatCompactNumber(getPoiIndexRecRead()), isPoiIndexRecReadDone() ? "(done)" : String.format("(%s/s)",formatCompactRate(poisReadSec))));
                    sb.append(String.format(" │ \033[32mPOIs Processed:\033[0m %s \033[33m(%s/s)\033[0m", formatCompactNumber(getPoisProcessed()), formatCompactRate(poisPerSec)));
                    sb.append(String.format(" │ \033[36mQueue:\033[0m %s", formatCompactNumber(getQueueSize())));
                    sb.append(String.format(" │ \033[37mThreads:\033[0m %d", getActiveThreads()));

                } else if (phase.contains("2.2")) {
                    long compactionElapsed = System.currentTimeMillis() - getCompactionStartTime();
                    double compactionPhaseSeconds = compactionElapsed / 1000.0;
                    long shardsCompacted = getShardsCompacted();
                    long shardsPerSec = compactionPhaseSeconds > 0 ? (long)(shardsCompacted / compactionPhaseSeconds) : 0;
                    sb.append(String.format("\033[1;36m[%s]\033[0m \033[1mCompacting POIs\033[0m", formatTime(elapsed)));
                    sb.append(String.format(" │ \033[32mShards Compacted:\033[0m %s \033[33m(%s/s)\033[0m",
                                            formatCompactNumber(shardsCompacted), formatCompactRate(shardsPerSec)));

                } else {
                    sb.append(String.format("\033[1;36m[%s]\033[0m %s", formatTime(elapsed), phase));
                }

                sb.append(String.format(" │ \033[31mHeap:\033[0m %s", getMemoryStats()));

                if (isTty) {
                    System.out.print(sb);
                    System.out.flush();
                } else {
                    System.out.println(sb);
                }

                try {
                    Thread.sleep(isTty ? 500 : 5000);
                } catch (InterruptedException e) {
                    break;
                }
            }
            if (isTty) System.out.println();
        });
    }

    public void printFinalStatistics() {
        System.out.println("\n\033[1;36m" + "═".repeat(80) + "\n" + centerText("🎯 FINAL IMPORT STATISTICS") + "\n" + "═".repeat(80) + "\033[0m");

        long totalTime = Math.max(1, getTotalTime());
        double totalSeconds = totalTime / 1000.0;

        System.out.printf("\n\033[1;37m⏱️  Total Import Time:\033[0m \033[1;33m%s\033[0m%n%n", formatTime(getTotalTime()));

        System.out.println("\033[1;37m📊 Processing Summary:\033[0m");
        System.out.println("┌─────────────────┬─────────────────┬─────────────────┐");
        System.out.println("│ \033[1mEntity Type\033[0m     │ \033[1mTotal Count\033[0m     │ \033[1mAvg Speed\033[0m       │");
        System.out.println("├─────────────────┼─────────────────┼─────────────────┤");
        System.out.printf("│ \033[32mPBF Entities\033[0m    │ %15s │ %13s/s │%n",
                          formatCompactNumber(getEntitiesRead()),
                          formatCompactNumber((long)(getEntitiesRead() / totalSeconds)));
        System.out.printf("│ \033[37mNodes Found\033[0m     │ %15s │ %13s/s │%n",
                          formatCompactNumber(getNodesFound()),
                          formatCompactNumber((long)(getNodesFound() / totalSeconds)));
        System.out.printf("│ \033[34mNodes Cached\033[0m    │ %15s │ %13s/s │%n",
                          formatCompactNumber(getNodesCached()),
                          formatCompactNumber((long)(getNodesCached() / totalSeconds)));
        System.out.printf("│ \033[35mWays Processed\033[0m  │ %15s │ %13s/s │%n",
                          formatCompactNumber(getWaysProcessed()),
                          formatCompactNumber((long)(getWaysProcessed() / totalSeconds)));
        System.out.printf("│ \033[36mBoundaries\033[0m      │ %15s │ %13s/s │%n",
                          formatCompactNumber(getBoundariesProcessed()),
                          formatCompactNumber((long)(getBoundariesProcessed() / totalSeconds)));
        System.out.printf("│ \033[33mPOIs Created\033[0m    │ %15s │ %13s/s │%n",
                          formatCompactNumber(getPoisProcessed()),
                          formatCompactNumber((long)(getPoisProcessed() / totalSeconds)));
        System.out.println("└─────────────────┴─────────────────┴─────────────────┘");

        long totalObjects = getNodesFound() + getNodesCached() + getWaysProcessed() + getBoundariesProcessed() + getPoisProcessed();
        System.out.printf("%n\033[1;37m🚀 Overall Throughput:\033[0m \033[1;32m%s objects\033[0m processed at \033[1;33m%s objects/sec\033[0m%n",
                          formatCompactNumber(totalObjects),
                          formatCompactNumber((long)(totalObjects / totalSeconds)));

        System.out.printf("\033[1;37m💾 Database Operations:\033[0m \033[1;36m%s writes\033[0m%n",
                          formatCompactNumber(getRocksDbWrites()));

        System.out.println("\n\033[1;37m📦 Dataset Size:\033[0m " + formatSize(getDatasetBytes()));
        System.out.println("  • poi_shards:  " + formatSize(getShardsBytes()));
        System.out.println("  • boundaries:  " + formatSize(getBoundariesBytes()));

        System.out.println("\n\033[1;37m🧹 Temporary DBs:\033[0m " + formatSize(getTmpTotalBytes()));
        System.out.println("  • grid_index:  " + formatSize(getTmpGridBytes()));
        System.out.println("  • node_cache:  " + formatSize(getTmpNodeBytes()));
        System.out.println("  • way_index:   " + formatSize(getTmpWayBytes()));
        System.out.println("  • needed_nodes:" + formatSize(getTmpNeededBytes()));
        System.out.println("  • rel_index:   " + formatSize(getTmpRelBytes()));
        System.out.println("  • poi_index:   " + formatSize(getTmpPoiBytes()));
        System.out.println("  • append_index:   " + formatSize(getTmpAppendBytes()));
        System.out.println();
    }


    private String formatTime(long ms) {
        long s = ms / 1000;
        return String.format("%d:%02d:%02d", s / 3600, (s % 3600) / 60, s % 60);
    }

    private String formatCompactNumber(long n) {
        if (n < 1000) return String.valueOf(n);
        if (n < 1_000_000) return String.format("%.2fk", n / 1000.0);
        return String.format("%.3fM", n / 1_000_000.0);
    }
    
    private String formatCompactRate(long n) {
        if (n < 1000) return String.valueOf(n);
        if (n < 1_000_000) return String.format("%.1fk", n / 1000.0);
        return String.format("%.1fM", n / 1_000_000.0);
    }

    private String formatSize(long bytes) {
        if (bytes < 1024) return bytes + " B";
        double kb = bytes / 1024.0;
        if (kb < 1024) return String.format("%.1f KB", kb);
        double mb = kb / 1024.0;
        if (mb < 1024) return String.format("%.1f MB", mb);
        double gb = mb / 1024.0;
        if (gb < 1024) return String.format("%.2f GB", gb);
        double tb = gb / 1024.0;
        return String.format("%.2f TB", tb);
    }

    private String centerText(String text) {
        int pad = (80 - text.length()) / 2;
        return " ".repeat(Math.max(0, pad)) + text;
    }


    public void printPhaseHeader(String phase) {
        System.out.println("\n\033[1;36m" + "─".repeat(80) + "\n" + phase + "\n" + "─".repeat(80) + "\033[0m");
    }

    public void printSuccess() {
        System.out.println("\n\033[1;32m" + "=".repeat(80) + "\n" + centerText("IMPORT COMPLETED SUCCESSFULLY") + "\n" + "=".repeat(80) + "\033[0m");
    }

    public void printError(String message) {
        System.out.println("\n\033[1;31m" + "=".repeat(80) + "\n" + centerText(message) + "\n" + "=".repeat(80) + "\033[0m");
    }

    public void printPhaseSummary(String phaseName, long phaseStartTime) {
        long phaseTime = System.currentTimeMillis() - phaseStartTime;
        System.out.printf("\n\u001B[1;32m✓ %s COMPLETED\u001B[0m \u001B[2m(%s)\u001B[0m%n", phaseName, formatTime(phaseTime));
    }
}
