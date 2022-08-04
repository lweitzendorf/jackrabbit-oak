package org.apache.jackrabbit.oak.segment.file;

import org.apache.jackrabbit.oak.spi.gc.GCMonitor;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkState;

/**
 * Monitors the exploration process required for parallel compaction to give
 * an estimated progress log before compaction itself can commence.
 * Similar in structure and function to {@link GCNodeWriteMonitor}.
 */
public class GCNodeExplorationMonitor {
    private final long gcProgressLog;
    private final GCMonitor gcMonitor;
    private long start;
    private long estimated = -1;
    private final AtomicLong nodes = new AtomicLong();
    private volatile boolean running = false;

    public GCNodeExplorationMonitor(long gcProgressLog, @NotNull GCMonitor gcMonitor) {
        this.gcProgressLog = gcProgressLog;
        this.gcMonitor = gcMonitor;
    }

    public void init(long estimatedSize) {
        checkState(!running);
        nodes.set(0);
        estimated = estimatedSize;
        start = System.currentTimeMillis();
        running = true;
    }

    public void onNode() {
        long exploredNodes = nodes.incrementAndGet();
        if (gcProgressLog > 0 && exploredNodes % gcProgressLog == 0) {
            gcMonitor.info("explored {} nodes in {} ms. {}",
                    nodes, System.currentTimeMillis() - start, getPercentageDone());
        }
    }

    public void finished() {
        running = false;
    }

    @NotNull
    private String getPercentageDone() {
        int percentage = getEstimatedPercentage();
        return (percentage >= 0) ? percentage + "% complete." : "";
    }

    public int getEstimatedPercentage() {
        if (!running) {
            return 100;
        }
        long numNodes = estimated;
        return (numNodes <= 0) ? -1 : Math.min((int) (100 * ((double) nodes.get() / numNodes)), 99);
    }
}
