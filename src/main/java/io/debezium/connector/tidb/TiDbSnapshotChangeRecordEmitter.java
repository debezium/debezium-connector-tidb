/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.tidb;

import org.apache.kafka.connect.data.Struct;

import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.AbstractChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.util.Clock;

/**
 * Emits the read record of a single snapshotted row.
 *
 * @author Aviral Srivastava
 */
public class TiDbSnapshotChangeRecordEmitter extends AbstractChangeRecordEmitter<TiDbPartition, TiDbTableSchema> {

    private final Struct key;
    private final Struct row;

    public TiDbSnapshotChangeRecordEmitter(TiDbPartition partition, OffsetContext offsetContext, Clock clock,
                                           TiDbConnectorConfig connectorConfig, Struct key, Struct row) {
        super(partition, offsetContext, clock, connectorConfig);
        this.key = key;
        this.row = row;
    }

    @Override
    public Operation getOperation() {
        return Operation.READ;
    }

    @Override
    protected void emitReadRecord(Receiver<TiDbPartition> receiver, TiDbTableSchema schema) throws InterruptedException {
        final Struct envelope = schema.getEnvelopeSchema().read(
                row, getOffset().getSourceInfo(), getClock().currentTimeAsInstant());
        receiver.changeRecord(getPartition(), schema, Operation.READ, key, envelope, getOffset(), null);
    }

    @Override
    protected void emitCreateRecord(Receiver<TiDbPartition> receiver, TiDbTableSchema schema) {
        throw new UnsupportedOperationException("Snapshots emit read records only");
    }

    @Override
    protected void emitUpdateRecord(Receiver<TiDbPartition> receiver, TiDbTableSchema schema) {
        throw new UnsupportedOperationException("Snapshots emit read records only");
    }

    @Override
    protected void emitDeleteRecord(Receiver<TiDbPartition> receiver, TiDbTableSchema schema) {
        throw new UnsupportedOperationException("Snapshots emit read records only");
    }
}
