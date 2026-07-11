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

import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class BoundaryImportStatistics {

    public enum Stage {
        CACHING_NODES_WAYS("Caching Nodes & Ways"),
        PROCESSING_RELATIONS("Processing Relations & H3"),
        OVERALL("Overall");

        private final String shortName;

        Stage(String shortName) {
            this.shortName = shortName;
        }

        @Override
        public String toString() {
            return this.shortName;
        }
    }

    public enum Kind {
        READ("Read/IO"),
        DECODE("Decode"),
        GEOMETRY("Geometry"),
        STORE("Store/Write"),
        OVERALL("Overall");

        private final String shortName;

        Kind(String shortName) {
            this.shortName = shortName;
        }

        @Override
        public String toString() {
            return shortName;
        }
    }

    private static final double DEGRADED_WARN_RATE = 1e-4; // 0.01% = 1 in 10,000
    private static final int ERROR_SAMPLE_LIMIT = 50;

    private final AtomicLong errorsTotal = new AtomicLong(0);
    private final ConcurrentHashMap<String, AtomicLong> errorBuckets = new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<String> errorSamples = new ConcurrentLinkedQueue<>();

    private final AtomicLong nodesCached = new AtomicLong(0);
    private final AtomicLong waysCached = new AtomicLong(0);
    private final AtomicLong relationsFound = new AtomicLong(0);
    private final AtomicLong relationsProcessed = new AtomicLong(0);
    private final AtomicLong h3CellsGenerated = new AtomicLong(0);

    private volatile String currentPhase = "Initializing";
    private volatile boolean running = true;
    private final long startTime = System.currentTimeMillis();
    private volatile long phaseStartTime = System.currentTimeMillis();
    private long totalTime;

    private final int TOTAL_STEPS = 2;
    private int currentStep = 0;

    public long getNodesCached() {
        return nodesCached.get();
    }

    public void incrementNodesCached() {
        nodesCached.incrementAndGet();
    }

    public long getWaysCached() {
        return waysCached.get();
    }

    public void incrementWaysCached() {
        waysCached.incrementAndGet();
    }

    public long getRelationsFound() {
        return relationsFound.get();
    }

    public void incrementRelationsFound() {
        relationsFound.incrementAndGet();
    }

    public long getRelationsProcessed() {
        return relationsProcessed.get();
    }

    public void incrementRelationsProcessed() {
        relationsProcessed.incrementAndGet();
    }

    public long getH3CellsGenerated() {
        return h3CellsGenerated.get();
    }

    public void addH3CellsGenerated(long count) {
        h3CellsGenerated.addAndGet(count);
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

    public void recordError(Stage stage, Kind kind, Long osmId, String operation, Exception e) {
        errorsTotal.incrementAndGet();

        String safePhase = stage.toString();
        String safeKind = kind.toString();
        String safeOp = operation != null ? operation : "-";
        String ex = (e != null) ? e.getClass().getSimpleName() : "Exception";
        String bucketKey = safePhase + "|" + safeKind + "|" + safeOp + "|" + ex;

        errorBuckets.computeIfAbsent(bucketKey, k -> new AtomicLong(0)).incrementAndGet();

        if (errorSamples.size() < ERROR_SAMPLE_LIMIT) {
            String msg = (e != null ? e.getMessage() : null);
            errorSamples.add(
                    "phase=" + safePhase
                            + " kind=" + safeKind
                            + " id=" + (osmId != null ? osmId : "-")
                            + " op=" + safeOp
                            + " ex=" + (e != null ? e.getClass().getName() : "java.lang.Exception")
                            + (msg != null ? " msg=" + msg : "")
            );
        }
    }

    public long getErrorsTotal() {
        return errorsTotal.get();
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

                sb.append(String.format("\033[1;90m[%d/%d]\033[0m ", currentStep, TOTAL_STEPS));

                if (phase.contains("1.1")) {
                    long nodesPerSec = phaseSeconds > 0 ? (long) (getNodesCached() / phaseSeconds) : 0;
                    sb.append(String.format("\033[1;36m[%s]\033[0m \033[1mCaching Nodes & Ways\033[0m", formatTime(elapsed)));
                    sb.append(String.format(" │ \033[32mNodes:\033[0m %s \033[33m(%s/s)\033[0m",
                                            formatCompactNumber(getNodesCached()), formatCompactRate(nodesPerSec)));
                    sb.append(String.format(" │ \033[34mWays:\033[0m %s", formatCompactNumber(getWaysCached())));
                } else if (phase.contains("2.1")) {
                    long relsPerSec = phaseSeconds > 0 ? (long) (getRelationsProcessed() / phaseSeconds) : 0;
                    double percentage = getRelationsFound() > 0 ? (double) getRelationsProcessed() / getRelationsFound() * 100.0 : 0.0;
                    sb.append(String.format("\033[1;36m[%s]\033[0m \033[1mProcessing Relations & H3\033[0m", formatTime(elapsed)));
                    sb.append(String.format(" │ \033[32mRelations:\033[0m %s/%s \033[33m(%s/s)\033[0m",
                                            formatCompactNumber(getRelationsProcessed()), formatCompactNumber(getRelationsFound()), formatCompactRate(relsPerSec)));
                    sb.append(String.format(" │ \033[35mProgress:\033[0m %.2f%%", percentage));
                    sb.append(String.format(" │ \033[36mH3 Cells:\033[0m %s", formatCompactNumber(getH3CellsGenerated())));
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
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    break;
                }
            }
            if (isTty) System.out.println();
        });
    }

    public void printFinalStatistics() {
        System.out.println("\n\033[1;36m" + "═".repeat(80) + "\n" + centerText("🎯 BOUNDARY IMPORT STATISTICS") + "\n" + "═".repeat(80) + "\033[0m");

        long totalTime = Math.max(1, getTotalTime());
        double totalSeconds = totalTime / 1000.0;

        System.out.printf("\n\033[1;37m⏱️  Total Import Time:\033[0m \033[1;33m%s\033[0m%n%n", formatTime(getTotalTime()));

        System.out.println("\033[1;37m📊 Processing Summary:\033[0m");
        System.out.println("┌────────────────────┬─────────────────┬─────────────────┐");
        System.out.println("│ \033[1mEntity Type\033[0m        │ \033[1mTotal Count\033[0m     │ \033[1mAvg Speed\033[0m       │");
        System.out.println("├────────────────────┼─────────────────┼─────────────────┤");
        System.out.printf("│ \033[32mNodes Cached\033[0m       │ %15s │ %13s/s │%n",
                          formatCompactNumber(getNodesCached()),
                          formatCompactNumber((long) (getNodesCached() / totalSeconds)));
        System.out.printf("│ \033[34mWays Cached\033[0m        │ %15s │ %13s/s │%n",
                          formatCompactNumber(getWaysCached()),
                          formatCompactNumber((long) (getWaysCached() / totalSeconds)));
        System.out.printf("│ \033[35mRelations Found\033[0m    │ %15s │ %13s/s │%n",
                          formatCompactNumber(getRelationsFound()),
                          formatCompactNumber((long) (getRelationsFound() / totalSeconds)));
        System.out.printf("│ \033[36mRelations Processed\033[0m│ %15s │ %13s/s │%n",
                          formatCompactNumber(getRelationsProcessed()),
                          formatCompactNumber((long) (getRelationsProcessed() / totalSeconds)));
        System.out.printf("│ \033[33mH3 Cells Generated\033[0m │ %15s │ %13s/s │%n",
                          formatCompactNumber(getH3CellsGenerated()),
                          formatCompactNumber((long) (getH3CellsGenerated() / totalSeconds)));
        System.out.println("└────────────────────┴─────────────────┴─────────────────┘");

        System.out.println();
    }

    public void printOutcomeAndErrors() {
        long err = getErrorsTotal();
        long denominator = Math.max(1L, getRelationsFound());
        double rate = (double) err / (double) denominator;

        String outcome = (rate >= DEGRADED_WARN_RATE) ? "DEGRADED" : "OK";
        System.out.println("\n\033[1;36mIMPORT OUTCOME: " + outcome
                                   + " | errors=" + err
                                   + " | relationsFound=" + denominator
                                   + " | errorRate=" + String.format(Locale.ROOT, "%.6f%%", rate * 100.0)
                                   + "\033[0m");

        if (err == 0) {
            return;
        }

        System.err.println("\n=== Boundary import errors summary (best-effort) ===");
        System.err.println("totalErrors=" + err);
        System.err.println("topBuckets=" + Math.min(10, errorBuckets.size()) + "/" + errorBuckets.size());

        errorBuckets.entrySet().stream()
                .sorted((a, b) -> Long.compare(b.getValue().get(), a.getValue().get()))
                .limit(10)
                .forEach(e -> System.err.println("  " + e.getValue().get() + "x " + e.getKey()));

        if (!errorSamples.isEmpty()) {
            System.err.println("\nSamples (first " + errorSamples.size() + "):");
            for (String s : errorSamples) {
                System.err.println("  " + s);
            }
        }

        System.err.println("=== End boundary import errors summary ===\n");
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

    private String centerText(String text) {
        int pad = (80 - text.length()) / 2;
        return " ".repeat(Math.max(0, pad)) + text;
    }

    public void printPhaseHeader(String phase) {
        System.out.println("\n\033[1;36m" + "─".repeat(80) + "\n" + phase + "\n" + "─".repeat(80) + "\033[0m");
    }

    public void printSuccess() {
        System.out.println("\n\033[1;32m" + "=".repeat(80) + "\n" + centerText("BOUNDARY IMPORT COMPLETED SUCCESSFULLY") + "\n" + "=".repeat(80) + "\033[0m");
    }

    public void printError(String message) {
        System.out.println("\n\033[1;31m" + "=".repeat(80) + "\n" + centerText(message) + "\n" + "=".repeat(80) + "\033[0m");
    }

    public void printPhaseSummary(String phaseName, long phaseStartTime) {
        long phaseTime = System.currentTimeMillis() - phaseStartTime;
        System.out.printf("\n\u001B[1;32m✓ %s COMPLETED\u001B[0m \u001B[2m(%s)\u001B[0m%n", phaseName, formatTime(phaseTime));
    }
}
