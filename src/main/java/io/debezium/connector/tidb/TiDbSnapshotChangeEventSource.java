/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.tidb;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.SnapshotRecord;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.EventDispatcher.SnapshotReceiver;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.actions.snapshotting.AdditionalCondition;
import io.debezium.pipeline.signal.actions.snapshotting.SnapshotConfiguration;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.SnapshotChangeRecordEmitter;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.schema.DataCollectionId;
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

    public TiDbSnapshotChangeEventSource(TiDbConnectorConfig connectorConfig,
                                         SnapshotterService snapshotterService,
                                         EventDispatcher<TiDbPartition, TableId> dispatcher,
                                         Clock clock,
                                         TiDbSchema schema,
                                         SnapshotProgressListener<TiDbPartition> snapshotProgressListener,
                                         NotificationService<TiDbPartition, TiDbOffsetContext> notificationService) {
        super(connectorConfig, snapshotProgressListener, notificationService);
        this.connectorConfig = connectorConfig;
        this.snapshotterService = snapshotterService;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
    }

    @Override
    public SnapshottingTask getSnapshottingTask(TiDbPartition partition, TiDbOffsetContext previousOffset) {
        final boolean offsetExists = previousOffset != null;
        final boolean snapshotInProgress = offsetExists && previousOffset.isInitialSnapshotRunning();
        // Table structure is never snapshotted separately: the row schemas travel with the data,
        // both in snapshot events and in the TiCDC messages
        final boolean snapshotData = snapshotterService.getSnapshotter().shouldSnapshotData(offsetExists, snapshotInProgress);
        return new SnapshottingTask(false, snapshotData, connectorConfig.getDataCollectionsToBeSnapshotted(), Map.of(), false);
    }

    @Override
    public SnapshottingTask getBlockingSnapshottingTask(TiDbPartition partition, TiDbOffsetContext previousOffset,
                                                        SnapshotConfiguration snapshotConfiguration) {
        final Map<DataCollectionId, String> filtersByTable = snapshotConfiguration.getAdditionalConditions().stream()
                .collect(Collectors.toMap(k -> TableId.parse(k.getDataCollection().toString()), AdditionalCondition::getFilter));
        return new SnapshottingTask(false, true, snapshotConfiguration.getDataCollections(), filtersByTable, true);
    }

    @Override
    protected SnapshotResult<TiDbOffsetContext> doExecute(ChangeEventSourceContext context, TiDbOffsetContext previousOffset,
                                                          SnapshotContext<TiDbPartition, TiDbOffsetContext> snapshotContext,
                                                          SnapshottingTask snapshottingTask)
            throws Exception {
        final TiDbOffsetContext offset = previousOffset != null ? previousOffset : TiDbOffsetContext.empty(connectorConfig);
        snapshotContext.offset = offset;
        warnAboutUnsupportedOptions();

        try (TiDbConnection connection = createConnection()) {
            final long tso = connection.currentTso();
            // Pin the session first, so that the table list is read at the snapshot TSO as well
            // and a table created after it is not snapshotted
            connection.initSnapshotSession(tso);
            // The snapshot table list, or the tables named by an on demand snapshot signal; an
            // empty list means all captured tables
            final List<TableId> tables = determineDataCollectionsToBeSnapshotted(
                    connection.capturedTables(connectorConfig.getTableFilters().dataCollectionFilter()),
                    getDataCollectionPattern(snapshottingTask.getDataCollections()))
                    .toList();
            LOGGER.info("Snapshotting {} table(s) at TSO {}", tables.size(), tso);

            offset.snapshotStarted(tso, snapshottingTask.isOnDemand());

            final SnapshotReceiver<TiDbPartition> receiver = dispatcher.getSnapshotChangeEventReceiver();
            long totalRows = 0;
            for (int i = 0; i < tables.size(); i++) {
                if (!context.isRunning()) {
                    throw new InterruptedException("Interrupted while snapshotting table " + tables.get(i));
                }
                totalRows += snapshotTable(context, snapshotContext, receiver, connection, tables.get(i),
                        snapshottingTask.getFilterQueries(), tso, i == tables.size() - 1);
            }
            offset.preSnapshotCompletion();
            receiver.completeSnapshot();
            offset.postSnapshotCompletion();
            postSnapshot();
            // The last snapshot record carries the completed snapshot state to the offsets; the
            // heartbeat also covers a snapshot without rows when heartbeats are enabled, the same
            // way as RelationalSnapshotChangeEventSource
            dispatcher.alwaysDispatchHeartbeatEvent(snapshotContext.partition, offset);
            LOGGER.info("Snapshot of {} table(s) with {} row(s) completed at TSO {}", tables.size(), totalRows, tso);
        }
        return SnapshotResult.completed(offset);
    }

    private long snapshotTable(ChangeEventSourceContext context, SnapshotContext<TiDbPartition, TiDbOffsetContext> snapshotContext,
                               SnapshotReceiver<TiDbPartition> receiver, TiDbConnection connection, TableId tableId,
                               Map<DataCollectionId, String> filterQueries, long tso, boolean lastTable)
            throws Exception {
        final Table table = connection.readTableStructure(tableId);
        schema.refresh(table);
        if (table.primaryKeyColumnNames().isEmpty()) {
            LOGGER.warn("Table {} has neither a primary key nor a unique key on NOT NULL columns; TiCDC does not stream changes of such "
                    + "tables unless the changefeed sets force-replicate", tableId);
        }

        final List<Column> columns = TiDbConnection.storedColumns(table);
        final String query = snapshotQuery(connection, table, columns, filterQueries);
        LOGGER.info("Snapshotting table {}", tableId);

        final long[] rows = { 0 };
        // Rows are emitted one behind the cursor so that the last row of a table can be marked
        final Object[][] pendingRow = new Object[1][];
        connection.fetchRows(query, columns, table.columns().size(), values -> {
            if (!context.isRunning()) {
                throw new InterruptedException("Interrupted while snapshotting table " + tableId);
            }
            if (pendingRow[0] != null) {
                emitRow(snapshotContext, receiver, tableId, tso, pendingRow[0], SnapshotRecord.TRUE);
            }
            pendingRow[0] = values;
            rows[0]++;
        });
        if (pendingRow[0] != null) {
            emitRow(snapshotContext, receiver, tableId, tso, pendingRow[0],
                    lastTable ? SnapshotRecord.LAST : SnapshotRecord.LAST_IN_DATA_COLLECTION);
        }
        return rows[0];
    }

    /**
     * @return the select statement for the table: the {@code additional-conditions} of an on
     *         demand snapshot signal if one is given, otherwise the snapshot query of the
     *         snapshotter. Like in the relational connectors, a custom statement has to return all
     *         stored columns of the table in table order.
     */
    private String snapshotQuery(TiDbConnection connection, Table table, List<Column> columns, Map<DataCollectionId, String> filterQueries) {
        final String filterQuery = filterQueries.get(table.id());
        if (filterQuery != null) {
            LOGGER.info("Using the select statement of the snapshot signal for table {}: {}", table.id(), filterQuery);
            return filterQuery;
        }
        return snapshotterService.getSnapshotQuery()
                .snapshotQuery(table.id().toQuotedString('`'),
                        columns.stream().map(c -> connection.quoteIdentifier(c.name())).collect(Collectors.toList()))
                .orElseThrow(() -> new DebeziumException("No snapshot query for table " + table.id()));
    }

    private void emitRow(SnapshotContext<TiDbPartition, TiDbOffsetContext> snapshotContext, SnapshotReceiver<TiDbPartition> receiver,
                         TableId tableId, long tso, Object[] row, SnapshotRecord marker)
            throws InterruptedException {
        final TiDbOffsetContext offset = snapshotContext.offset;
        offset.snapshotEvent(tableId, clock.currentTimeAsInstant(), tso);
        offset.markSnapshotRecord(marker);
        dispatcher.dispatchSnapshotEvent(snapshotContext.partition, tableId,
                new SnapshotChangeRecordEmitter<>(snapshotContext.partition, offset, row, clock, connectorConfig),
                receiver);
    }

    private void warnAboutUnsupportedOptions() {
        if (!connectorConfig.getSnapshotSelectOverridesByTable().isEmpty()) {
            LOGGER.warn("'{}' is not supported by the TiDB connector and is ignored, the snapshot reads all rows of the tables",
                    RelationalDatabaseConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE.name());
        }
        if (connectorConfig.getSnapshotMaxThreads() > 1) {
            LOGGER.warn("'{}' is not supported by the TiDB connector, the snapshot reads the tables with a single thread",
                    CommonConnectorConfig.SNAPSHOT_MAX_THREADS.name());
        }
        if (connectorConfig.snapshotOrderByRowCount() != RelationalDatabaseConnectorConfig.SnapshotTablesRowCountOrder.DISABLED) {
            LOGGER.warn("'{}' is not supported by the TiDB connector, the tables are snapshotted in the order of their names",
                    RelationalDatabaseConnectorConfig.SNAPSHOT_TABLES_ORDER_BY_ROW_COUNT.name());
        }
    }

    /**
     * Creates the JDBC connection used for the snapshot. Visible so that tests can substitute a
     * fake connection.
     */
    protected TiDbConnection createConnection() {
        return new TiDbConnection(connectorConfig);
    }

    /**
     * Hook invoked after the snapshot completed, mirroring
     * {@code RelationalSnapshotChangeEventSource}.
     */
    protected void postSnapshot() throws InterruptedException {
    }

    @Override
    protected SnapshotContext<TiDbPartition, TiDbOffsetContext> prepare(TiDbPartition partition, boolean onDemand) {
        return new SnapshotContext<>(partition);
    }
}
