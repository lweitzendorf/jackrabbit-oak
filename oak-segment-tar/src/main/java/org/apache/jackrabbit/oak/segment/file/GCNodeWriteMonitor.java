/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.segment.file;

import static com.google.common.base.Preconditions.checkState;

import org.apache.jackrabbit.oak.spi.gc.GCMonitor;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Monitors the compaction cycle and keeps a compacted nodes counter, in order
 * to provide a best-effort progress log based on extrapolating the previous
 * size and node count and current size to deduce current node count.
 */
public class GCNodeWriteMonitor {
    public static final GCNodeWriteMonitor EMPTY = new GCNodeWriteMonitor(
            -1, GCMonitor.EMPTY);

    /**
     * Number of nodes the monitor will log a message, -1 to disable
     */
    private final long gcProgressLog;

    private final GCMonitor gcMonitor;

    /**
     * Start timestamp of compaction (reset at each {@code init()} call).
     */
    private long start = 0;

    /**
     * Estimated nodes to compact per cycle (reset at each {@code init()} call).
     */
    private volatile long estimated = -1;

    /**
     * Number of compacted nodes
     */
    private final AtomicLong nodes = new AtomicLong();

    /**
     * Number of compacted properties
     */
    private final AtomicLong properties = new AtomicLong();

    /**
     * Number of compacted binaries
     */
    private final AtomicLong binaries = new AtomicLong();

    private volatile boolean running = false;

    public GCNodeWriteMonitor(long gcProgressLog, @NotNull GCMonitor gcMonitor) {
        this.gcProgressLog = gcProgressLog;
        this.gcMonitor = gcMonitor;
    }

    /**
     * @param prevSize
     *            size from latest successful compaction
     * @param prevCompactedNodes
     *            number of nodes compacted during latest compaction operation
     * @param currentSize
     *            current repository size
     */
    public void init(long prevSize, long prevCompactedNodes, long currentSize) {
        checkState(!running);
        if (prevCompactedNodes > 0) {
            estimated = (long) (((double) currentSize / prevSize) * prevCompactedNodes);
            gcMonitor.info(
                "estimated number of nodes to compact is {}, based on {} nodes compacted to {} bytes "
                    + "on disk in previous compaction and current size of {} bytes on disk.",
                estimated, prevCompactedNodes, prevSize, currentSize);
        } else {
            gcMonitor.info("unable to estimate number of nodes for compaction, missing gc history.");
        }
        nodes.set(0);
        properties.set(0);
        binaries.set(0);
        start = System.currentTimeMillis();
        running = true;
    }

    public void updateEstimate(long newEstimate) {
        estimated = newEstimate;
    }

    public void onNode() {
        long writtenNodes = nodes.incrementAndGet();
        if (gcProgressLog > 0 && writtenNodes % gcProgressLog == 0) {
            gcMonitor.info("compacted {} nodes, {} properties, {} binaries in {} ms. {}",
                nodes, properties, binaries, System.currentTimeMillis() - start, getPercentageDone());
        }
    }

    public void onProperty() {
        properties.incrementAndGet();
    }

    public void onBinary() {
        binaries.incrementAndGet();
    }

    public void finished() {
        running = false;
    }

    /**
     * Compacted nodes in current cycle
     */
    public long getCompactedNodes() {
        return nodes.get();
    }

    /**
     * Estimated nodes to compact in current cycle. Can be {@code -1} if the
     * estimation could not be performed.
     */
    public long getEstimatedTotal() {
        return estimated;
    }

    @NotNull
    private String getPercentageDone() {
        int percentage = getEstimatedPercentage();
        return (percentage >= 0) ? percentage + "% complete." : "";
    }

    /**
     * Estimated completion percentage. Can be {@code -1} if the estimation
     * could not be performed.
     */
    public int getEstimatedPercentage() {
        if (!running) {
            return 100;
        }
        long numNodes = estimated;
        return (numNodes <= 0) ? -1 : Math.min((int) (100 * ((double) nodes.get() / numNodes)), 99);
    }

    public boolean isCompactionRunning() {
        return running;
    }

    public long getGcProgressLog() {
        return gcProgressLog;
    }
}
