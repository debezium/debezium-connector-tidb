/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.tidb;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.tidb.ticdc.TiCdcEvent;
import io.debezium.connector.tidb.ticdc.TiCdcEventParser;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.monitor.OffsetActivityMonitor;
import io.debezium.pipeline.monitor.OffsetActivityMonitorService;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

/**
 * Streams change events from the Kafka topics written by a TiCDC changefeed running with
 * {@code protocol=debezium}.
 * <p>
 * This is the first phase of the TiDB connector agreed in DBZ-6269: TiCDC remains the component
 * that reads TiKV's event stream and resolves raw key-values into rows, while Debezium provides
 * connector lifecycle management, offset tracking and the standard event envelope. A later phase
 * may replace this source with a direct TiKV {@code EventFeed} client without affecting the rest
 * of the connector.
 * <p>
 * The consumer uses manual partition assignment; progress is tracked exclusively through the
 * Kafka Connect offsets of this connector, no consumer group offsets are committed.
 *
 * @author Aviral Srivastava
 */
public class TiCdcStreamingChangeEventSource implements StreamingChangeEventSource<TiDbPartition, TiDbOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TiCdcStreamingChangeEventSource.class);

    private final TiDbConnectorConfig connectorConfig;
    private final EventDispatcher<TiDbPartition, TableId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final TiDbSchema schema;

    private volatile TiDbOffsetContext effectiveOffsetContext;
    private final OffsetActivityMonitorService offsetActivityMonitorService;
    private OffsetActivityMonitor<TiDbPartition, TiDbOffsetContext> offsetActivityMonitor;

    public TiCdcStreamingChangeEventSource(TiDbConnectorConfig connectorConfig,
                                           EventDispatcher<TiDbPartition, TableId> dispatcher,
                                           ErrorHandler errorHandler, Clock clock, TiDbSchema schema) {
        this.connectorConfig = connectorConfig;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.schema = schema;
        this.offsetActivityMonitorService = OffsetActivityMonitorService.lookup(connectorConfig.getServiceRegistry());
    }

    @Override
    public void init(TiDbOffsetContext offsetContext) {
        this.effectiveOffsetContext = offsetContext != null ? offsetContext : TiDbOffsetContext.empty(connectorConfig);
    }

    @Override
    public void execute(ChangeEventSourceContext context, TiDbPartition partition, TiDbOffsetContext offsetContext)
            throws InterruptedException {
        final TiDbOffsetContext effectiveOffset = this.effectiveOffsetContext;

        try (TiCdcEventParser parser = new TiCdcEventParser();
                Consumer<byte[], byte[]> consumer = createConsumer();
                TiCdcTableResolver tableResolver = createTableResolver()) {

            seekToStartPosition(consumer, effectiveOffset);

            while (context.isRunning()) {
                if (context.isPaused()) {
                    // A blocking snapshot runs; the consumer keeps its position and resumes from
                    // there once the snapshot completed
                    LOGGER.info("Streaming will now pause");
                    context.streamingPaused();
                    context.waitSnapshotCompletion();
                    LOGGER.info("Streaming resumed");
                }

                final ConsumerRecords<byte[], byte[]> records = consumer.poll(connectorConfig.getTicdcPollTimeout());
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    if (!context.isRunning()) {
                        return;
                    }
                    processRecord(parser, tableResolver, partition, effectiveOffset, record);
                }

                offsetActivityMonitorService.pulse(partition, effectiveOffset);
            }
        }
        catch (InterruptedException e) {
            throw e;
        }
        catch (InterruptException e) {
            // KafkaConsumer reports a thread interrupt (forced shutdown after the graceful stop
            // timeout) through its own unchecked exception; surface it as a regular interruption
            // rather than a connector failure
            throw (InterruptedException) new InterruptedException("Interrupted while polling the TiCDC topics").initCause(e);
        }
        catch (Throwable t) {
            errorHandler.setProducerThrowable(t);
        }
    }

    private void processRecord(TiCdcEventParser parser, TiCdcTableResolver tableResolver, TiDbPartition partition,
                               TiDbOffsetContext offsetContext, ConsumerRecord<byte[], byte[]> record)
            throws InterruptedException {
        final TiCdcEvent event = parse(parser, record);
        if (event == null) {
            // Still advance the stream position so non data change messages are not re-read on restart
            offsetContext.event(null, clock.currentTimeAsInstant(), offsetContext.getCommitTs(), null,
                    record.topic(), record.partition(), record.offset() + 1);
            return;
        }

        if (event.commitTs() > 0 && event.commitTs() <= offsetContext.getSnapshotTs()) {
            // The initial snapshot already captured this state; the changefeed replays history
            // from before the snapshot TSO
            LOGGER.debug("Skipping event with commit_ts {} at or before the snapshot TSO {}",
                    event.commitTs(), offsetContext.getSnapshotTs());
            offsetContext.event(null, clock.currentTimeAsInstant(), offsetContext.getCommitTs(), null,
                    record.topic(), record.partition(), record.offset() + 1);
            return;
        }

        final TableId tableId = event.tableId();
        offsetContext.event(tableId, event.sourceTimestamp(), event.commitTs(), event.clusterId(),
                record.topic(), record.partition(), record.offset() + 1);

        // Excluded tables are filtered by the dispatcher and need no table model
        Table table = null;
        if (connectorConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId)) {
            table = schema.isCurrentForStream(tableId, event.keySchema(), event.rowSchema())
                    ? schema.tableFor(tableId).orElseThrow()
                    : refreshSchema(tableResolver, event);
        }

        dispatcher.dispatchDataChangeEvent(partition, tableId,
                new TiDbChangeRecordEmitter(partition, offsetContext, clock, connectorConfig, event, table));
    }

    private Table refreshSchema(TiCdcTableResolver tableResolver, TiCdcEvent event) {
        final Table table = tableResolver.resolve(event);
        schema.refreshFromStream(table, event.keySchema(), event.rowSchema());
        return table;
    }

    /**
     * Parses a TiCDC message, applying {@code event.processing.failure.handling.mode} to messages
     * that cannot be parsed.
     *
     * @return the parsed event, or {@code null} if the message carries no data change or is skipped
     */
    private TiCdcEvent parse(TiCdcEventParser parser, ConsumerRecord<byte[], byte[]> record) {
        try {
            return parser.parse(record.topic(), record.key(), record.value());
        }
        catch (RuntimeException e) {
            final String position = record.topic() + "-" + record.partition() + "@" + record.offset();
            switch (connectorConfig.getEventProcessingFailureHandlingMode()) {
                case WARN:
                    LOGGER.warn("Skipping TiCDC message at {} that could not be parsed", position, e);
                    return null;
                case SKIP:
                case IGNORE:
                    LOGGER.debug("Skipping TiCDC message at {} that could not be parsed", position, e);
                    return null;
                default:
                    throw new DebeziumException("Failed to parse TiCDC message at " + position, e);
            }
        }
    }

    /**
     * Creates the resolver of the table models of streamed tables. Visible so that tests can
     * substitute one that works without a SQL endpoint.
     */
    protected TiCdcTableResolver createTableResolver() {
        return new TiCdcTableResolver(connectorConfig);
    }

    /**
     * Creates the Kafka consumer reading the TiCDC topics. Visible so that tests can substitute
     * a mock consumer.
     */
    protected Consumer<byte[], byte[]> createConsumer() {
        final Properties props = connectorConfig.getTicdcConsumerProperties();
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.putIfAbsent(ConsumerConfig.CLIENT_ID_CONFIG, "debezium-tidb-" + connectorConfig.getLogicalName());
        return new KafkaConsumer<>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    private void seekToStartPosition(Consumer<byte[], byte[]> consumer, TiDbOffsetContext offsetContext) {
        final List<TopicPartition> assignment = new ArrayList<>();
        for (String topic : connectorConfig.getTicdcTopics()) {
            final List<PartitionInfo> partitions = consumer.partitionsFor(topic);
            if (partitions == null || partitions.isEmpty()) {
                throw new DebeziumException("TiCDC topic '" + topic + "' does not exist or has no partitions. "
                        + "Verify that the TiCDC changefeed is running and writing to the configured topics");
            }
            for (PartitionInfo partition : partitions) {
                assignment.add(new TopicPartition(partition.topic(), partition.partition()));
            }
        }
        consumer.assign(assignment);

        final List<TopicPartition> withoutStoredOffset = new ArrayList<>();
        for (TopicPartition topicPartition : assignment) {
            offsetContext.nextOffsetFor(topicPartition.topic(), topicPartition.partition())
                    .ifPresentOrElse(
                            nextOffset -> consumer.seek(topicPartition, nextOffset),
                            () -> withoutStoredOffset.add(topicPartition));
        }
        if (!withoutStoredOffset.isEmpty()) {
            if (connectorConfig.getTicdcInitialOffset() == TiDbConnectorConfig.InitialOffset.LATEST) {
                consumer.seekToEnd(withoutStoredOffset);
            }
            else {
                consumer.seekToBeginning(withoutStoredOffset);
            }
        }
        LOGGER.info("Consuming TiCDC stream from {} topic partition(s): {}", assignment.size(), assignment);
    }

    @Override
    public TiDbOffsetContext getOffsetContext() {
        return effectiveOffsetContext;
    }

    @Override
    public Optional<OffsetActivityMonitor<TiDbPartition, TiDbOffsetContext>> getOffsetActivityMonitor() {
        if (offsetActivityMonitor == null) {
            offsetActivityMonitor = new TiDbOffsetActivityMonitor(connectorConfig.getOffsetActivityMonitorInterval());
        }
        return Optional.of(offsetActivityMonitor);
    }
}
