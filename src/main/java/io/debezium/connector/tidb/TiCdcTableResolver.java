/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.tidb;

import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Optional;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.tidb.ticdc.TiCdcEvent;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;

/**
 * Resolves the table model of a table whose changes are streamed from TiCDC.
 * <p>
 * When the TiDB SQL endpoint is configured, the model is read from {@code information_schema}, so
 * that streamed rows carry the same column metadata (types, lengths) as snapshot rows and the
 * column mappers treat both the same way. The current structure is tried first; if its columns do
 * not match the TiCDC row, for example because of a later DDL, the structure is read as of the
 * commit TSO of the event. Without the SQL endpoint, or when neither matches, the model is
 * derived from the TiCDC row schema, which carries the column names, order and optionality but no
 * source types.
 * <p>
 * In every case the message key columns follow the TiCDC key, which is how TiCDC identifies rows.
 *
 * @author Aviral Srivastava
 */
public class TiCdcTableResolver implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(TiCdcTableResolver.class);

    private final TiDbConnectorConfig connectorConfig;
    private TiDbConnection connection;

    public TiCdcTableResolver(TiDbConnectorConfig connectorConfig) {
        this.connectorConfig = connectorConfig;
    }

    public Table resolve(TiCdcEvent event) {
        final TableId tableId = event.tableId();
        final List<String> columnNames = event.rowSchema().fields().stream().map(Field::name).toList();

        Table table = null;
        if (connectorConfig.isSqlEndpointConfigured()) {
            table = readMatching(tableId, columnNames, null)
                    .or(() -> readMatching(tableId, columnNames, event.commitTs()))
                    .orElse(null);
            if (table == null) {
                if (connectorConfig.requiresSourceColumnMetadata()) {
                    throw new DebeziumException("Cannot read the structure of table " + tableId + " matching the TiCDC row at commit_ts "
                            + event.commitTs() + "; the configured column mappers need the source column metadata. "
                            + "The TSO may be older than the GC safe point of the cluster");
                }
                LOGGER.warn("Structure of table {} at commit_ts {} could not be read through the SQL endpoint, "
                        + "deriving it from the TiCDC row schema", tableId, event.commitTs());
            }
        }
        if (table == null) {
            table = fromTiCdcSchema(tableId, event.rowSchema());
        }
        return withTiCdcKey(table, event.keySchema());
    }

    private Optional<Table> readMatching(TableId tableId, List<String> columnNames, Long tso) {
        try {
            final Optional<Table> table = tso == null
                    ? connection().findTableStructure(tableId)
                    : connection().readTableStructureAt(tableId, tso);
            return table.filter(candidate -> candidate.retrieveColumnNames().equals(columnNames));
        }
        catch (SQLException e) {
            LOGGER.debug("Failed to read the structure of table {}{}", tableId, tso == null ? "" : " at TSO " + tso, e);
            return Optional.empty();
        }
    }

    /**
     * Derives a table model from the TiCDC row schema: column names, order and optionality.
     */
    static Table fromTiCdcSchema(TableId tableId, Schema rowSchema) {
        final TableEditor editor = Table.editor().tableId(tableId);
        int position = 1;
        for (Field field : rowSchema.fields()) {
            final Schema schema = field.schema();
            editor.addColumn(Column.editor()
                    .name(field.name())
                    .type(schema.name() != null ? schema.name() : schema.type().getName())
                    .jdbcType(jdbcTypeFor(schema))
                    .optional(schema.isOptional())
                    .position(position++)
                    .create());
        }
        return editor.create();
    }

    private static Table withTiCdcKey(Table table, Schema keySchema) {
        if (keySchema == null) {
            return table;
        }
        final List<String> keyColumns = keySchema.fields().stream().map(Field::name).toList();
        if (keyColumns.equals(table.primaryKeyColumnNames())) {
            return table;
        }
        final TableEditor editor = table.edit();
        editor.setPrimaryKeyNames(keyColumns);
        return editor.create();
    }

    private static int jdbcTypeFor(Schema schema) {
        switch (schema.type()) {
            case INT8:
            case INT16:
                return Types.SMALLINT;
            case INT32:
                return Types.INTEGER;
            case INT64:
                return Types.BIGINT;
            case FLOAT32:
                return Types.REAL;
            case FLOAT64:
                return Types.DOUBLE;
            case BOOLEAN:
                return Types.BOOLEAN;
            case BYTES:
                return Types.BIT;
            default:
                return Types.VARCHAR;
        }
    }

    private TiDbConnection connection() {
        if (connection == null) {
            connection = new TiDbConnection(connectorConfig);
        }
        return connection;
    }

    @Override
    public void close() {
        if (connection != null) {
            try {
                connection.close();
            }
            catch (SQLException e) {
                LOGGER.warn("Failed to close the TiDB connection of the streaming source", e);
            }
        }
    }
}
