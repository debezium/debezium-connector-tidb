/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.tidb;

import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.OptionalLong;

import org.apache.kafka.connect.data.Schema;

import io.debezium.DebeziumException;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SnapshotType;
import io.debezium.pipeline.CommonOffsetContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Strings;

/**
 * Offset context of the TiDB connector.
 * <p>
 * Two positions are tracked:
 * <ul>
 * <li>the logical TiDB position — the TSO commit timestamp ({@code commit_ts}) of the last
 * processed transaction;</li>
 * <li>the physical TiCDC stream position — the next offset to read for every Kafka
 * topic-partition of the TiCDC changefeed output the connector consumes.</li>
 * </ul>
 * The stream position is authoritative for resuming; {@code commit_ts} is informational and will
 * be used to resume a re-created changefeed from a consistent point once direct TiKV streaming
 * (phase 2) is implemented.
 *
 * @author Aviral Srivastava
 */
public class TiDbOffsetContext extends CommonOffsetContext<SourceInfo> {

    public static final String COMMIT_TS_KEY = SourceInfo.COMMIT_TS_KEY;
    public static final String TICDC_OFFSETS_KEY = "ticdc_offsets";
    public static final String SNAPSHOT_TS_KEY = "snapshot_ts";

    private static final String PARTITION_SEPARATOR = ",";
    private static final String OFFSET_SEPARATOR = "=";

    private final TransactionContext transactionContext;

    /**
     * Next offset to consume, keyed by {@code topic:partition}.
     */
    private final Map<String, Long> ticdcOffsets = new LinkedHashMap<>();

    private long commitTs;

    /**
     * The TSO at which the initial snapshot was taken, 0 when no snapshot ran. Streamed events
     * with a commit timestamp not newer than this are dropped as already captured.
     */
    private long snapshotTs;

    public TiDbOffsetContext(SourceInfo sourceInfo, TransactionContext transactionContext) {
        super(sourceInfo);
        this.transactionContext = transactionContext;
    }

    public static TiDbOffsetContext empty(TiDbConnectorConfig connectorConfig) {
        return new TiDbOffsetContext(new SourceInfo(connectorConfig), new TransactionContext());
    }

    @Override
    public Map<String, ?> getOffset() {
        final Map<String, Object> offset = new HashMap<>();
        offset.put(COMMIT_TS_KEY, commitTs);
        if (snapshotTs > 0) {
            offset.put(SNAPSHOT_TS_KEY, snapshotTs);
        }
        if (!ticdcOffsets.isEmpty()) {
            offset.put(TICDC_OFFSETS_KEY, Strings.join(PARTITION_SEPARATOR, ticdcOffsets.entrySet(),
                    e -> e.getKey() + OFFSET_SEPARATOR + e.getValue()));
        }
        if (getSnapshot().isPresent()) {
            offset.put(AbstractSourceInfo.SNAPSHOT_KEY, getSnapshot().get().toString());
            offset.put(SNAPSHOT_COMPLETED_KEY, snapshotCompleted);
            return offset;
        }
        return transactionContext.store(offset);
    }

    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfo.schema();
    }

    @Override
    public TransactionContext getTransactionContext() {
        return transactionContext;
    }

    @Override
    public void event(DataCollectionId collectionId, Instant timestamp) {
        sourceInfo.update((TableId) collectionId, timestamp, commitTs, sourceInfo.clusterId());
    }

    /**
     * Records the position of a change event consumed from the TiCDC stream.
     *
     * @param tableId the table the event applies to
     * @param timestamp the source-side timestamp of the change
     * @param eventCommitTs the TSO commit timestamp of the transaction
     * @param clusterId the TiCDC cluster id, may be {@code null}
     * @param topic the TiCDC Kafka topic the event was read from
     * @param kafkaPartition the Kafka partition the event was read from
     * @param nextKafkaOffset the offset to continue reading from after this event
     */
    public void event(TableId tableId, Instant timestamp, long eventCommitTs, String clusterId,
                      String topic, int kafkaPartition, long nextKafkaOffset) {
        this.commitTs = eventCommitTs;
        this.ticdcOffsets.put(topic + ":" + kafkaPartition, nextKafkaOffset);
        sourceInfo.update(tableId, timestamp, eventCommitTs, clusterId);
    }

    public long getCommitTs() {
        return commitTs;
    }

    public long getSnapshotTs() {
        return snapshotTs;
    }

    /**
     * Marks the start of the initial snapshot taken at the given TSO.
     */
    public void snapshotStarted(long tso, boolean onDemand) {
        preSnapshotStart(onDemand);
        this.snapshotTs = tso;
        this.commitTs = tso;
    }

    /**
     * Records the position of a snapshotted row: the logical position is the snapshot TSO, there
     * is no TiCDC stream position during a snapshot.
     */
    public void snapshotEvent(TableId tableId, Instant timestamp, long tso) {
        this.commitTs = tso;
        sourceInfo.update(tableId, timestamp, tso, null);
    }

    /**
     * @return the next offset to consume for the given TiCDC topic-partition, if one was recorded
     */
    public OptionalLong nextOffsetFor(String topic, int kafkaPartition) {
        final Long next = ticdcOffsets.get(topic + ":" + kafkaPartition);
        return next != null ? OptionalLong.of(next) : OptionalLong.empty();
    }

    public boolean hasStreamPosition() {
        return !ticdcOffsets.isEmpty();
    }

    public static class Loader implements OffsetContext.Loader<TiDbOffsetContext> {

        private final TiDbConnectorConfig connectorConfig;

        public Loader(TiDbConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public TiDbOffsetContext load(Map<String, ?> offset) {
            final TiDbOffsetContext context = new TiDbOffsetContext(
                    new SourceInfo(connectorConfig), TransactionContext.load(offset));

            final Object commitTs = offset.get(COMMIT_TS_KEY);
            if (commitTs instanceof Number number) {
                context.commitTs = number.longValue();
            }

            final Object snapshotTs = offset.get(SNAPSHOT_TS_KEY);
            if (snapshotTs instanceof Number number) {
                context.snapshotTs = number.longValue();
            }

            if (offset.get(AbstractSourceInfo.SNAPSHOT_KEY) instanceof String snapshotType) {
                context.setSnapshot(SnapshotType.valueOf(snapshotType));
                context.snapshotCompleted = Boolean.TRUE.equals(offset.get(SNAPSHOT_COMPLETED_KEY))
                        || "true".equals(offset.get(SNAPSHOT_COMPLETED_KEY));
            }

            final Object encodedOffsets = offset.get(TICDC_OFFSETS_KEY);
            if (encodedOffsets != null && !Strings.isNullOrEmpty(encodedOffsets.toString())) {
                for (String entry : encodedOffsets.toString().split(PARTITION_SEPARATOR)) {
                    final int separator = entry.lastIndexOf(OFFSET_SEPARATOR);
                    if (separator <= 0 || separator == entry.length() - 1) {
                        throw new DebeziumException("Invalid TiCDC stream offset entry '" + entry + "' in stored offsets");
                    }
                    context.ticdcOffsets.put(entry.substring(0, separator), Long.parseLong(entry.substring(separator + 1)));
                }
            }
            return context;
        }
    }

    @Override
    public String toString() {
        return "TiDbOffsetContext [commitTs=" + commitTs + ", ticdcOffsets=" + ticdcOffsets + "]";
    }
}
