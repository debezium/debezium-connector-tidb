/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.tidb;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.sql.Types;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.DebeziumHeaderProducer;
import io.debezium.connector.tidb.snapshot.query.SelectAllSnapshotQuery;
import io.debezium.data.Envelope;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.schema.SchemaFactory;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.snapshot.spi.SnapshotLock;
import io.debezium.spi.snapshot.Snapshotter;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Clock;
import io.debezium.util.LoggingContext;

/**
 * Tests of the snapshot pipeline: rows read from a fake JDBC connection all the way through the
 * event dispatcher to the emitted {@link SourceRecord}s.
 *
 * @author Aviral Srivastava
 */
public class TiDbSnapshotChangeEventSourceTest {

    private static final long TSO = 446245805252059200L;
    private static final TableId PRODUCTS = new TableId("inventory", null, "products");

    private static final Table PRODUCTS_TABLE = Table.editor()
            .tableId(PRODUCTS)
            .addColumn(Column.editor().name("id").type("bigint").jdbcType(Types.BIGINT).optional(false).position(1).create())
            .addColumn(Column.editor().name("name").type("varchar(255)").jdbcType(Types.VARCHAR).optional(true).position(2).create())
            .addColumn(Column.editor().name("weight").type("decimal(10,2)").jdbcType(Types.DECIMAL).optional(true).position(3).create())
            .setPrimaryKeyNames(List.of("id"))
            .create();

    private static final List<Object[]> PRODUCT_ROWS = List.of(
            new Object[]{ 17L, "scooter", new BigDecimal("3.14") },
            new Object[]{ 18L, "rocks", new BigDecimal("5.30") });

    /**
     * A self-contained harness around the snapshot source with a fake JDBC connection and a real
     * dispatcher and queue.
     */
    private static class SnapshotHarness {

        final TiDbConnectorConfig connectorConfig;
        final ChangeEventQueue<DataChangeEvent> queue;
        final TiDbSnapshotChangeEventSource source;
        final TiDbSchema schema;

        SnapshotHarness(Configuration config, Snapshotter snapshotter) {
            this.connectorConfig = new TiDbConnectorConfig(config);
            @SuppressWarnings("unchecked")
            final TopicNamingStrategy<TableId> topicNamingStrategy = connectorConfig.getTopicNamingStrategy(TiDbConnectorConfig.TOPIC_NAMING_STRATEGY);
            final SchemaNameAdjuster adjuster = connectorConfig.schemaNameAdjuster();

            this.schema = new TiDbSchema(topicNamingStrategy, connectorConfig.getSourceInfoStructMaker().schema(), adjuster);
            this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                    .pollInterval(connectorConfig.getPollInterval())
                    .maxBatchSize(connectorConfig.getMaxBatchSize())
                    .maxQueueSize(connectorConfig.getMaxQueueSize())
                    .loggingContextSupplier(() -> LoggingContext.forConnector("TiDB", "tidb_server", "snapshot-test"))
                    .build();
            final TiDbTaskContext taskContext = new TiDbTaskContext(config, connectorConfig);

            final EventDispatcher<TiDbPartition, TableId> dispatcher = new EventDispatcher<>(
                    connectorConfig,
                    topicNamingStrategy,
                    schema,
                    queue,
                    connectorConfig.getTableFilters().dataCollectionFilter(),
                    DataChangeEvent::new,
                    new TiDbEventMetadataProvider(),
                    adjuster,
                    new DebeziumHeaderProducer(taskContext));

            final SnapshotterService snapshotterService = new SnapshotterService(snapshotter, new SelectAllSnapshotQuery(), new NoOpLock());
            final NotificationService<TiDbPartition, TiDbOffsetContext> notificationService = new NotificationService<>(
                    List.of(), connectorConfig, SchemaFactory.get(), record -> {
                    });

            this.source = new TiDbSnapshotChangeEventSource(connectorConfig, snapshotterService, dispatcher,
                    Clock.system(), schema, topicNamingStrategy, SnapshotProgressListener.NO_OP(), notificationService) {
                @Override
                protected TiDbConnection createConnection() {
                    return new FakeTiDbConnection(connectorConfig);
                }
            };
        }

        List<SourceRecord> poll() throws InterruptedException {
            return queue.poll().stream()
                    .map(DataChangeEvent::getRecord)
                    .collect(Collectors.toList());
        }
    }

    private static class FakeTiDbConnection extends TiDbConnection {

        FakeTiDbConnection(TiDbConnectorConfig connectorConfig) {
            super(connectorConfig);
        }

        @Override
        public long currentTso() {
            return TSO;
        }

        @Override
        public void initSnapshotSession(long tso) {
            assertThat(tso).isEqualTo(TSO);
        }

        @Override
        public List<TableId> capturedTables(TableFilter tableFilter) {
            return List.of(PRODUCTS).stream().filter(tableFilter::isIncluded).collect(Collectors.toList());
        }

        @Override
        public Table readTableStructure(TableId tableId) {
            return PRODUCTS_TABLE;
        }

        @Override
        public void fetchRows(String snapshotQuery, List<Column> columns, RowConsumer consumer) throws InterruptedException {
            assertThat(snapshotQuery).isEqualTo("SELECT `id`, `name`, `weight` FROM `inventory`.`products`");
            for (Object[] row : PRODUCT_ROWS) {
                consumer.accept(row);
            }
        }

        @Override
        public void close() {
        }
    }

    private static class InitialSnapshotter implements Snapshotter {

        @Override
        public String name() {
            return "initial";
        }

        @Override
        public void configure(Map<String, ?> properties) {
        }

        @Override
        public boolean shouldSnapshotData(boolean offsetExists, boolean snapshotInProgress) {
            return !offsetExists || snapshotInProgress;
        }

        @Override
        public boolean shouldSnapshotSchema(boolean offsetExists, boolean snapshotInProgress) {
            return false;
        }

        @Override
        public boolean shouldStream() {
            return true;
        }

        @Override
        public boolean shouldSnapshotOnSchemaError() {
            return false;
        }

        @Override
        public boolean shouldSnapshotOnDataError() {
            return false;
        }
    }

    private static class NoOpLock implements SnapshotLock {

        @Override
        public String name() {
            return "none";
        }

        @Override
        public void configure(Map<String, ?> properties) {
        }

        @Override
        public Optional<String> tableLockingStatement(Duration lockTimeout, String tableId) {
            return Optional.empty();
        }
    }

    private static class AlwaysRunningContext implements ChangeEventSourceContext {

        private final AtomicBoolean running = new AtomicBoolean(true);

        @Override
        public boolean isPaused() {
            return false;
        }

        @Override
        public boolean isRunning() {
            return running.get();
        }

        @Override
        public void resumeStreaming() {
        }

        @Override
        public void waitSnapshotCompletion() {
        }

        @Override
        public void streamingPaused() {
        }

        @Override
        public void waitStreamingPaused() {
        }
    }

    private static Configuration.Builder config() {
        return Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "tidb_server")
                .with(TiDbConnectorConfig.TICDC_BOOTSTRAP_SERVERS, "localhost:9092")
                .with(TiDbConnectorConfig.TICDC_TOPICS, "ticdc-inventory")
                .with(TiDbConnectorConfig.SNAPSHOT_MODE, "initial")
                .with(TiDbConnectorConfig.JDBC_HOSTNAME, "localhost")
                .with(TiDbConnectorConfig.JDBC_USER, "root")
                .with(CommonConnectorConfig.POLL_INTERVAL_MS, 10);
    }

    @Test
    public void shouldSnapshotTablesAtOneTso() throws Exception {
        final SnapshotHarness harness = new SnapshotHarness(config().build(), new InitialSnapshotter());
        final TiDbPartition partition = new TiDbPartition("tidb_server");

        final SnapshottingTask task = harness.source.getSnapshottingTask(partition, null);
        assertThat(task.snapshotData()).isTrue();
        assertThat(task.shouldSkipSnapshot()).isFalse();

        final SnapshotResult<TiDbOffsetContext> result = harness.source.execute(new AlwaysRunningContext(), partition, null, task);
        assertThat(result.getStatus()).isEqualTo(SnapshotResult.SnapshotResultStatus.COMPLETED);
        assertThat(result.getOffset().getSnapshotTs()).isEqualTo(TSO);
        assertThat(result.getOffset().getCommitTs()).isEqualTo(TSO);

        final List<SourceRecord> records = harness.poll();
        assertThat(records).hasSize(2);

        final SourceRecord first = records.get(0);
        assertThat(first.topic()).isEqualTo("tidb_server.inventory.products");
        assertThat(((Struct) first.key()).getInt64("id")).isEqualTo(17L);

        final Struct firstValue = (Struct) first.value();
        assertThat(firstValue.getString(Envelope.FieldName.OPERATION)).isEqualTo(Envelope.Operation.READ.code());
        assertThat(firstValue.getStruct(Envelope.FieldName.AFTER).getString("name")).isEqualTo("scooter");
        assertThat(firstValue.getStruct(Envelope.FieldName.AFTER).getFloat64("weight")).isEqualTo(3.14d);

        final Struct firstSource = firstValue.getStruct(Envelope.FieldName.SOURCE);
        assertThat(firstSource.getString("db")).isEqualTo("inventory");
        assertThat(firstSource.getString("table")).isEqualTo("products");
        assertThat(firstSource.getInt64(SourceInfo.COMMIT_TS_KEY)).isEqualTo(TSO);
        assertThat(firstSource.getString("snapshot")).isEqualTo("true");

        final Struct lastSource = ((Struct) records.get(1).value()).getStruct(Envelope.FieldName.SOURCE);
        assertThat(lastSource.getString("snapshot")).isEqualTo("last");

        // The offsets of the last snapshot record carry the snapshot TSO for the streaming handoff
        @SuppressWarnings("unchecked")
        final Map<String, Object> sourceOffset = (Map<String, Object>) records.get(1).sourceOffset();
        assertThat(sourceOffset).containsEntry(TiDbOffsetContext.SNAPSHOT_TS_KEY, TSO);
    }

    @Test
    public void shouldSkipSnapshotWhenOffsetsExist() {
        final SnapshotHarness harness = new SnapshotHarness(config().build(), new InitialSnapshotter());
        final TiDbPartition partition = new TiDbPartition("tidb_server");
        final TiDbOffsetContext existingOffset = TiDbOffsetContext.empty(harness.connectorConfig);

        final SnapshottingTask task = harness.source.getSnapshottingTask(partition, existingOffset);
        assertThat(task.shouldSkipSnapshot()).isTrue();
    }

    @Test
    public void shouldApplyTableFilterToSnapshot() throws Exception {
        final SnapshotHarness harness = new SnapshotHarness(config()
                .with("table.include.list", "inventory\\.orders")
                .build(), new InitialSnapshotter());
        final TiDbPartition partition = new TiDbPartition("tidb_server");

        final SnapshottingTask task = harness.source.getSnapshottingTask(partition, null);
        final SnapshotResult<TiDbOffsetContext> result = harness.source.execute(new AlwaysRunningContext(), partition, null, task);

        assertThat(result.getStatus()).isEqualTo(SnapshotResult.SnapshotResultStatus.COMPLETED);
        assertThat(harness.poll()).isEmpty();
    }
}
