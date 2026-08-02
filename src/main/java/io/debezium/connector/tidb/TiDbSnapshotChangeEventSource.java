/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.tidb;

import java.util.List;
import java.util.Map;

import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.actions.snapshotting.SnapshotConfiguration;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.SnapshotResult;

/**
 * Snapshot source of the TiDB connector.
 * <p>
 * Initial data snapshots are not implemented yet: a TiCDC changefeed created with a
 * {@code start-ts} in the past can backfill history, and TiDB's MySQL-compatible SQL endpoint
 * will be used for managed initial/incremental snapshots in a follow-up iteration. Until then
 * every snapshot is skipped and the connector goes straight to streaming.
 *
 * @author Aviral Srivastava
 */
public class TiDbSnapshotChangeEventSource extends AbstractSnapshotChangeEventSource<TiDbPartition, TiDbOffsetContext> {

    public TiDbSnapshotChangeEventSource(TiDbConnectorConfig connectorConfig,
                                         SnapshotProgressListener<TiDbPartition> snapshotProgressListener,
                                         NotificationService<TiDbPartition, TiDbOffsetContext> notificationService) {
        super(connectorConfig, snapshotProgressListener, notificationService);
    }

    @Override
    public SnapshottingTask getSnapshottingTask(TiDbPartition partition, TiDbOffsetContext previousOffset) {
        // Neither part of a snapshot applies yet: table structure arrives inline with every
        // TiCDC message and data snapshots require the SQL endpoint support of a later
        // iteration, so the snapshot is always skipped
        return new SnapshottingTask(false, false, List.of(), Map.of(), false);
    }

    @Override
    public SnapshottingTask getBlockingSnapshottingTask(TiDbPartition partition, TiDbOffsetContext previousOffset,
                                                        SnapshotConfiguration snapshotConfiguration) {
        return new SnapshottingTask(false, false, List.of(), Map.of(), true);
    }

    @Override
    protected SnapshotResult<TiDbOffsetContext> doExecute(ChangeEventSourceContext context, TiDbOffsetContext previousOffset,
                                                          SnapshotContext<TiDbPartition, TiDbOffsetContext> snapshotContext,
                                                          SnapshottingTask snapshottingTask) {
        // Not reachable while getSnapshottingTask() skips both snapshot parts
        return SnapshotResult.skipped(previousOffset);
    }

    @Override
    protected SnapshotContext<TiDbPartition, TiDbOffsetContext> prepare(TiDbPartition partition, boolean onDemand) {
        return new SnapshotContext<>(partition);
    }
}
