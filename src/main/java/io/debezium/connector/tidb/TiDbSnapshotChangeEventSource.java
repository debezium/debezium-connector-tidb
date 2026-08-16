/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.tidb;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.SnapshotRecord;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.EventDispatcher.SnapshotReceiver;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.actions.snapshotting.SnapshotConfiguration;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.relational.TableId;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Clock;

/**
 * Snapshot source of the TiDB connector.
 * <p>
 * Data is read through TiDB's MySQL compatible SQL endpoint. The source captures the current TSO
 * once, pins the session to it with {@code tidb_snapshot}, and reads every captured table at that
 * consistent point; no locks are taken. The snapshot TSO is recorded in the offsets and the
 * streaming source drops any TiCDC event whose commit timestamp is not newer, so a changefeed
 * whose {@code start-ts} lies before the snapshot TSO does not produce duplicates.
 * <p>
 * The snapshot is not resumable in the middle: when the connector restarts before completion the
 * snapshot runs again from the start. Incremental snapshots arrive in a later iteration.
 *
 * @author Aviral Srivastava
 */
public class TiDbSnapshotChangeEventSource extends AbstractSnapshotChangeEventSource<TiDbPartition, TiDbOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TiDbSnapshotChangeEventSource.class);

    private final TiDbConnectorConfig connectorConfig;
    private final SnapshotterService snapshotterService;
    private final EventDispatcher<TiDbPartition, TableId> dispatcher;
    private final Clock clock;
    private final TiDbSchema schema;
    private final TopicNamingStrategy<TableId> topicNamingStrategy;
    private final TiDbSnapshotSchemaBuilder schemaBuilder;

    public TiDbSnapshotChangeEventSource(TiDbConnectorConfig connectorConfig,
                                         SnapshotterService snapshotterService,
                                         EventDispatcher<TiDbPartition, TableId> dispatcher,
                                         Clock clock,
                                         TiDbSchema schema,
                                         TopicNamingStrategy<TableId> topicNamingStrategy,
                                         SnapshotProgressListener<TiDbPartition> snapshotProgressListener,
                                         NotificationService<TiDbPartition, TiDbOffsetContext> notificationService) {
        super(connectorConfig, snapshotProgressListener, notificationService);
        this.connectorConfig = connectorConfig;
        this.snapshotterService = snapshotterService;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
        this.topicNamingStrategy = topicNamingStrategy;
        this.schemaBuilder = new TiDbSnapshotSchemaBuilder(connectorConfig.schemaNameAdjuster());
    }

    @Override
    public SnapshottingTask getSnapshottingTask(TiDbPartition partition, TiDbOffsetContext previousOffset) {
        final boolean offsetExists = previousOffset != null;
        final boolean snapshotInProgress = previousOffset != null && previousOffset.isInitialSnapshotRunning();
        // Table structure is never snapshotted separately: the row schemas travel with the data,
        // both in snapshot events and in the TiCDC messages
        final boolean snapshotData = snapshotterService.getSnapshotter().shouldSnapshotData(offsetExists, snapshotInProgress);
        return new SnapshottingTask(false, snapshotData, List.of(), Map.of(), false);
    }

    @Override
    public SnapshottingTask getBlockingSnapshottingTask(TiDbPartition partition, TiDbOffsetContext previousOffset,
                                                        SnapshotConfiguration snapshotConfiguration) {
        return new SnapshottingTask(false, true, snapshotConfiguration.getDataCollections(), Map.of(), true);
    }

    @Override
    protected SnapshotResult<TiDbOffsetContext> doExecute(ChangeEventSourceContext context, TiDbOffsetContext previousOffset,
                                                          SnapshotContext<TiDbPartition, TiDbOffsetContext> snapshotContext,
                                                          SnapshottingTask snapshottingTask)
            throws Exception {
        final TiDbOffsetContext offset = previousOffset != null ? previousOffset : TiDbOffsetContext.empty(connectorConfig);
        snapshotContext.offset = offset;

        try (TiDbConnection connection = createConnection()) {
            final long tso = connection.currentTso();
            final List<TableId> tables = connection.capturedTables(connectorConfig.getTableFilters().dataCollectionFilter());
            LOGGER.info("Snapshotting {} table(s) at TSO {}", tables.size(), tso);

            connection.setSnapshotTso(tso);
            offset.snapshotStarted(tso, snapshottingTask.isOnDemand());

            final SnapshotReceiver<TiDbPartition> receiver = dispatcher.getSnapshotChangeEventReceiver();
            long totalRows = 0;
            for (int i = 0; i < tables.size(); i++) {
                if (!context.isRunning()) {
                    throw new InterruptedException("Interrupted while snapshotting table " + tables.get(i));
                }
                totalRows += snapshotTable(context, snapshotContext, receiver, connection, tables.get(i),
                        tso, i == tables.size() - 1);
            }
            offset.preSnapshotCompletion();
            receiver.completeSnapshot();
            offset.postSnapshotCompletion();
            LOGGER.info("Snapshot of {} table(s) with {} row(s) completed at TSO {}", tables.size(), totalRows, tso);
        }
        return SnapshotResult.completed(offset);
    }

    private long snapshotTable(ChangeEventSourceContext context, SnapshotContext<TiDbPartition, TiDbOffsetContext> snapshotContext,
                               SnapshotReceiver<TiDbPartition> receiver, TiDbConnection connection, TableId tableId,
                               long tso, boolean lastTable)
            throws Exception {
        final List<TiDbColumn> columns = connection.readColumns(tableId);
        final String topicName = topicNamingStrategy.dataChangeTopic(tableId);
        final Schema keySchema = schemaBuilder.keySchema(topicName, columns);
        final Schema rowSchema = schemaBuilder.rowSchema(topicName, columns);
        final TiDbTableSchema tableSchema = schema.refresh(tableId, keySchema, rowSchema);

        final String query = snapshotterService.getSnapshotQuery()
                .snapshotQuery(quoted(tableId), columns.stream().map(c -> "`" + c.name() + "`").collect(Collectors.toList()))
                .orElseThrow(() -> new DebeziumException("No snapshot query for table " + tableId));
        LOGGER.info("Snapshotting table {}", tableId);

        final TiDbOffsetContext offset = snapshotContext.offset;
        final long[] rows = { 0 };
        // Rows are emitted one behind the cursor so that the last row of the last table can be
        // marked as the final snapshot record
        final Struct[] pendingKey = new Struct[1];
        final Struct[] pendingRow = new Struct[1];
        connection.fetchRows(query, columns, values -> {
            if (!context.isRunning()) {
                throw new InterruptedException("Interrupted while snapshotting table " + tableId);
            }
            if (pendingRow[0] != null) {
                emitRow(snapshotContext, receiver, tableId, tableSchema, tso, pendingKey[0], pendingRow[0], SnapshotRecord.TRUE);
            }
            pendingKey[0] = schemaBuilder.keyValue(keySchema, columns, values);
            pendingRow[0] = schemaBuilder.rowValue(rowSchema, columns, values);
            rows[0]++;
        });
        if (pendingRow[0] != null) {
            emitRow(snapshotContext, receiver, tableId, tableSchema, tso, pendingKey[0], pendingRow[0],
                    lastTable ? SnapshotRecord.LAST : SnapshotRecord.LAST_IN_DATA_COLLECTION);
        }
        return rows[0];
    }

    private void emitRow(SnapshotContext<TiDbPartition, TiDbOffsetContext> snapshotContext, SnapshotReceiver<TiDbPartition> receiver,
                         TableId tableId, TiDbTableSchema tableSchema, long tso, Struct key, Struct row, SnapshotRecord marker)
            throws InterruptedException {
        final TiDbOffsetContext offset = snapshotContext.offset;
        offset.snapshotEvent(tableId, clock.currentTimeAsInstant(), tso);
        offset.markSnapshotRecord(marker);
        dispatcher.dispatchSnapshotEvent(snapshotContext.partition, tableId,
                new TiDbSnapshotChangeRecordEmitter(snapshotContext.partition, offset, clock, connectorConfig, key, row),
                receiver);
    }

    private static String quoted(TableId tableId) {
        return "`" + tableId.catalog() + "`.`" + tableId.table() + "`";
    }

    /**
     * Creates the JDBC connection used for the snapshot. Visible so that tests can substitute a
     * fake connection.
     */
    protected TiDbConnection createConnection() {
        return new TiDbConnection(connectorConfig);
    }

    @Override
    protected SnapshotContext<TiDbPartition, TiDbOffsetContext> prepare(TiDbPartition partition, boolean onDemand) {
        return new SnapshotContext<>(partition);
    }
}
