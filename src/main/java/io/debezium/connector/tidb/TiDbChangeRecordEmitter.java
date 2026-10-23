/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.tidb;

import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.tidb.ticdc.TiCdcEvent;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.relational.Table;
import io.debezium.util.Clock;

/**
 * Emits change records for data changes consumed from the TiCDC stream. The row images of the
 * TiCDC message are laid out as column values of the table model, so that they go through the
 * same table schema as snapshot rows: the configured column filters, column mappers and key
 * columns apply, and the {@code source} block carries the real TiDB position (TSO commit
 * timestamp) instead of the MySQL flavoured dummies of TiCDC's Debezium output mode.
 *
 * @author Aviral Srivastava
 */
public class TiDbChangeRecordEmitter extends RelationalChangeRecordEmitter<TiDbPartition> {

    private final TiCdcEvent event;
    private final Table table;

    public TiDbChangeRecordEmitter(TiDbPartition partition, OffsetContext offsetContext, Clock clock,
                                   TiDbConnectorConfig connectorConfig, TiCdcEvent event, Table table) {
        super(partition, offsetContext, clock, connectorConfig);
        this.event = event;
        this.table = table;
    }

    @Override
    public Operation getOperation() {
        return event.operation();
    }

    @Override
    protected Object[] getOldColumnValues() {
        return columnValues(event.before());
    }

    @Override
    protected Object[] getNewColumnValues() {
        return columnValues(event.after());
    }

    private Object[] columnValues(Struct row) {
        if (row == null) {
            return null;
        }
        final Object[] values = new Object[table.columns().size()];
        for (Column column : table.columns()) {
            if (row.schema().field(column.name()) != null) {
                values[column.position() - 1] = row.get(column.name());
            }
        }
        return values;
    }
}
