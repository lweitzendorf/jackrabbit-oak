package org.apache.jackrabbit.oak.segment;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.segment.file.GCNodeExplorationMonitor;
import org.apache.jackrabbit.oak.segment.file.GCNodeWriteMonitor;
import org.apache.jackrabbit.oak.segment.file.cancel.Canceller;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.gc.GCMonitor;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.segment.ClassicCompactor.getStableIdBytes;

/**
 * This compactor implementation leverages the tree structure of the repository for concurrent compaction.
 * It uses the estimated compaction size to set a maximum subtree size and traverses the repository to find
 * suitable entry points for asynchronous compaction. After the exploration phase, the main thread will collect
 * these compaction results and write their parents' node states to disk. If there are too few nodes to process
 * in parallel efficiently, the implementation will fall back to the behavior of {@link CheckpointCompactor}.
 */
public class ParallelCompactor extends CheckpointCompactor {
    /**
     * Number of nodes below which compaction falls back to sequential execution.
     */
    private static final long CONCURRENT_EFFICIENCY_THRESHOLD = 1_000_000;

    private final int numWorkers;

    /**
     * Manages workers for asynchronous compaction.
     */
    @Nullable
    private final ExecutorService executorService;

    /**
     * Monitors progress of explored nodes in the initial exploration phase.
     */
    @NotNull
    private final GCNodeExplorationMonitor explorationMonitor;

    public ParallelCompactor(
            @NotNull GCMonitor gcListener,
            @NotNull SegmentReader reader,
            @NotNull SegmentWriter writer,
            @Nullable BlobStore blobStore,
            @NotNull GCNodeWriteMonitor compactionMonitor) {
        this(gcListener, reader, writer, blobStore, compactionMonitor, -1);
    }

    /**
     * Create a new instance based on the passed arguments.
     * @param gcListener listener receiving notifications about the garbage collection process
     * @param reader     segment reader used to read from the segments
     * @param writer     segment writer used to serialise to segments
     * @param blobStore  the blob store or {@code null} if none
     * @param compactionMonitor   notification call back for each compacted nodes, properties, and binaries
     * @param nThreads   number of threads to use for parallel compaction,
     *                   negative numbers are interpreted relative to the number of available processors
     */
    public ParallelCompactor(
            @NotNull GCMonitor gcListener,
            @NotNull SegmentReader reader,
            @NotNull SegmentWriter writer,
            @Nullable BlobStore blobStore,
            @NotNull GCNodeWriteMonitor compactionMonitor,
            int nThreads) {
        super(gcListener, reader, writer, blobStore, compactionMonitor);
        explorationMonitor = new GCNodeExplorationMonitor(compactionMonitor.getGcProgressLog(), gcListener);

        int availableProcessors = Runtime.getRuntime().availableProcessors();
        if (nThreads < 0) {
            nThreads += availableProcessors + 1;
        }

        if (nThreads <= 1 || getEstimatedSize() < CONCURRENT_EFFICIENCY_THRESHOLD) {
            gcListener.info("using sequential compaction.");
            numWorkers = 0;
            executorService = null;
        } else {
            numWorkers = Math.min(nThreads, availableProcessors) - 1;
            executorService = Executors.newFixedThreadPool(this.numWorkers);
        }
    }

    /**
     * Get estimated number of nodes to compact nodes from {@code compactionMonitor}.
     */
    private long getEstimatedSize() {
        return compactor.compactionMonitor.getEstimatedTotal();
    }

    /**
     * Calculates the maximum subtree size for asynchronous compaction entry points.
     */
    private long getMaxSequentialCompactionSize() {
        return getEstimatedSize() / numWorkers;
    }

    /**
     * Represents structure of repository changes. Tree is built by exploration process and subsequently
     * used to collect and merge asynchronous compaction results.
     */
    private class CompactionTree {
        /**
         * Recursive number of nodes in this subtree.
         */
        private long size;

        /**
         * Stores result of asynchronous compaction.
         */
        @Nullable
        private Future<SegmentNodeState> compactionFuture;

        @NotNull
        private final NodeState before;

        @NotNull
        private final NodeState after;

        @NotNull
        private final NodeState onto;

        @NotNull
        private final HashMap<String, CompactionTree> modifiedChildren = new HashMap<>();

        @NotNull
        private final List<Property> modifiedProperties = new ArrayList<>();

        @NotNull
        private final List<String> removedChildNames = new ArrayList<>();

        @NotNull
        private final List<String> removedPropertyNames = new ArrayList<>();

        CompactionTree(@NotNull NodeState before, @NotNull NodeState after, @NotNull NodeState onto) {
            this.size = 1;
            this.before = checkNotNull(before);
            this.after = checkNotNull(after);
            this.onto = checkNotNull(onto).exists() ? onto : EMPTY_NODE;
        }

        private class Property {
            @NotNull
            private final PropertyState state;

            Property(@NotNull PropertyState state) {
                this.state = state;
            }

            @NotNull
            PropertyState compact() {
                return compactor.compact(state);
            }
        }

        void registerModifiedChild(String name, @NotNull CompactionTree child) {
            size += child.size;
            modifiedChildren.put(name, child);
        }

        void registerModifiedProperty(@NotNull PropertyState state) {
            modifiedProperties.add(new Property(state));
        }

        void registerRemovedChild(@NotNull String name) {
            removedChildNames.add(name);
        }

        void registerRemovedProperty(@NotNull String name) {
            removedPropertyNames.add(name);
        }

        /**
         * Cuts off tree, removing references to every child node and property.
         */
        void prune() {
            modifiedChildren.clear();
            modifiedProperties.clear();
            removedChildNames.clear();
            removedPropertyNames.clear();
        }

        /**
         * Start asynchronous compaction.
         */
        boolean compactAsync(Canceller canceller) {
            if (compactionFuture != null) {
                return false;
            }
            checkNotNull(executorService);
            compactionFuture = executorService.submit(() -> compactor.compact(before, after, onto, canceller));
            return true;
        }

        /**
         * Start synchronous compaction on tree or collect result of asynchronous compaction if it has been started.
         */
        @Nullable
        SegmentNodeState compact() throws IOException {
            if (compactionFuture != null) {
                try {
                    return compactionFuture.get();
                } catch (InterruptedException e) {
                    return null;
                } catch (ExecutionException e) {
                    throw new IOException(e);
                }
            }

            MemoryNodeBuilder builder = new MemoryNodeBuilder(onto);

            for (Map.Entry<String, CompactionTree> entry : modifiedChildren.entrySet()) {
                SegmentNodeState compactedState = entry.getValue().compact();
                if (compactedState == null) {
                    // compaction failed, cancel other compaction threads
                    checkNotNull(executorService);
                    executorService.shutdownNow();
                    return null;
                }
                builder.setChildNode(entry.getKey(), compactedState);
            }
            for (String childName : removedChildNames) {
                builder.getChildNode(childName).remove();
            }
            for (Property property : modifiedProperties) {
                builder.setProperty(property.compact());
            }
            for (String propertyName : removedPropertyNames) {
                builder.removeProperty(propertyName);
            }

            return compactor.writeNodeState(builder.getNodeState(), getStableIdBytes(after));
        }
    }

    /**
     * Implementation of {@link NodeStateDiff} to build {@link CompactionTree} and start asynchronous compaction on
     * suitable entry points. Performs what is referred to as the exploration phase in other comments.
     */
    private class ExplorationCompactDiff implements NodeStateDiff {
        @NotNull
        private final NodeState base;

        @NotNull
        private final Canceller canceller;

        @Nullable
        private IOException exception;

        /**
         * Stores the node being compared to enable access in {@link NodeStateDiff} callbacks.
         */
        private CompactionTree currentNode;

        ExplorationCompactDiff(@NotNull NodeState base, @NotNull Canceller canceller) {
            this.canceller = canceller;
            this.base = base;
        }

        @Nullable
        SegmentNodeState diff(@NotNull NodeState before, @NotNull NodeState after) throws IOException {
            gcListener.info("exploring content tree to find subtrees for parallel compaction.");
            gcListener.info("maximum subtree node count is {}, based on an estimate of {} total nodes "
                    + "and {} available workers.", getMaxSequentialCompactionSize(), getEstimatedSize(), numWorkers);

            explorationMonitor.init(getEstimatedSize());
            CompactionTree compactionTree = diff(before, after, base);
            explorationMonitor.finished();

            if (compactionTree == null) {
                return null;
            }

            gcListener.info("finished exploration, updating current estimated node count {} to {}.",
                    getEstimatedSize(), compactionTree.size);

            long oldSubtreeMax = getMaxSequentialCompactionSize();
            compactor.compactionMonitor.updateEstimate(compactionTree.size);

            // if this happens, no entry point will have been found
            if (compactionTree.size <= oldSubtreeMax) {
                gcListener.info("content tree much smaller than expected.");
                if (compactionTree.size < CONCURRENT_EFFICIENCY_THRESHOLD) {
                    gcListener.info("compaction size too small, using sequential compaction.");
                    return compactor.compact(before, after, base, canceller);
                } else {
                    gcListener.info("rerunning exploration with maximum subtree node count {}.",
                            getMaxSequentialCompactionSize());
                    explorationMonitor.init(getEstimatedSize());
                    compactionTree = diff(before, after, base);
                    explorationMonitor.finished();
                    if (compactionTree == null) {
                        return null;
                    }
                }
            }

            return compactionTree.compact();
        }

        @Nullable
        private CompactionTree diff(@NotNull NodeState before, @NotNull NodeState after, @NotNull NodeState onto) throws IOException {
            currentNode = new CompactionTree(before, after, onto);
            boolean success = after.compareAgainstBaseState(before,
                    new CancelableDiff(this, () -> canceller.check().isCancelled()));

            if (exception != null) {
                throw new IOException(exception);
            } else if (success) {
                long maxSequentialCompactionSize = getMaxSequentialCompactionSize();
                if (currentNode.size > maxSequentialCompactionSize) {
                    // this should be true for very few nodes, therefore iteration is not an efficiency concern
                    for (CompactionTree child : currentNode.modifiedChildren.values()) {
                        if (child.size <= maxSequentialCompactionSize) {
                            // new entry point for async compaction found
                            checkState(child.compactAsync(canceller));
                        }
                    }
                } else {
                    // tree already smaller than {@code maxSequentialCompactionSize}
                    // -> no child will be an entry point
                    // cutting off tree here to free up memory
                    currentNode.prune();
                }
                explorationMonitor.onNode();
                return currentNode;
            } else {
                return null;
            }
        }

        private boolean onNodeChange(String name, NodeState before, NodeState after) {
            try {
                CompactionTree parent = currentNode;
                CompactionTree child = diff(before, after, parent.onto.getChildNode(name));
                if (child == null) {
                    return false;
                }
                parent.registerModifiedChild(name, child);
                currentNode = parent;
                return true;
            } catch (IOException e) {
                exception = e;
                return false;
            }
        }

        @Override
        public boolean propertyAdded(PropertyState after) {
            currentNode.registerModifiedProperty(after);
            return true;
        }

        @Override
        public boolean propertyChanged(PropertyState before, PropertyState after) {
            currentNode.registerModifiedProperty(after);
            return true;
        }

        @Override
        public boolean propertyDeleted(PropertyState before) {
            currentNode.registerRemovedProperty(before.getName());
            return true;
        }

        @Override
        public boolean childNodeAdded(String name, NodeState after) {
            return onNodeChange(name, EMPTY_NODE, after);
        }

        @Override
        public boolean childNodeChanged(String name, NodeState before, NodeState after) {
            return onNodeChange(name, before, after);
        }

        @Override
        public boolean childNodeDeleted(String name, NodeState before) {
            currentNode.registerRemovedChild(name);
            return true;
        }
    }

    @Nullable
    @Override
    protected SegmentNodeState compactWithDelegate(
            @NotNull NodeState before,
            @NotNull NodeState after,
            @NotNull NodeState onto,
            Canceller canceller
    ) throws IOException {
        if (executorService == null) {
            return compactor.compact(before, after, onto, canceller);
        }
        return new ExplorationCompactDiff(onto, canceller).diff(before, after);
    }
}
