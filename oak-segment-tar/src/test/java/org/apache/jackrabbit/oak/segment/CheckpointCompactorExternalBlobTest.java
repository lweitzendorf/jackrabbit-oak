package org.apache.jackrabbit.oak.segment;

import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.GCNodeWriteMonitor;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.apache.jackrabbit.oak.spi.gc.GCMonitor;
import org.jetbrains.annotations.NotNull;

import static org.apache.jackrabbit.oak.segment.DefaultSegmentWriterBuilder.defaultSegmentWriterBuilder;

public class CheckpointCompactorExternalBlobTest extends CompactorExternalBlobTest {
    @Override
    protected CheckpointCompactor createCompactor(@NotNull FileStore fileStore, @NotNull GCGeneration generation) {
        SegmentWriter writer = defaultSegmentWriterBuilder("c")
                .withGeneration(generation)
                .build(fileStore);

        return new CheckpointCompactor(
                GCMonitor.EMPTY,
                fileStore.getReader(),
                writer,
                fileStore.getBlobStore(),
                GCNodeWriteMonitor.EMPTY);
    }
}
