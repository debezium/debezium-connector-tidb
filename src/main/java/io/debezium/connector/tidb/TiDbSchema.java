/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.tidb;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.ThreadSafe;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.DefaultValueConverter;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.relational.ValueConverterProvider;
import io.debezium.relational.mapping.ColumnMappers;
import io.debezium.schema.DatabaseSchema;
import io.debezium.schema.SchemaFactory;
import io.debezium.spi.topic.TopicNamingStrategy;

/**
 * The schemas of the TiDB tables captured by the connector.
 * <p>
 * Snapshot and streaming events go through the same {@link TableSchemaBuilder}, so the column
 * filters, column mappers (masking, truncation, source type propagation), message key columns
 * and naming options apply to both in the same way. The two paths differ only in where the field
 * schemas and values come from: the snapshot converts JDBC values with
 * {@link TiDbJdbcValueConverters}, streaming keeps the values and field schemas that TiCDC
 * already encoded. Both encodings are the same, so a table has one schema across the hand off
 * from snapshot to streaming.
 * <p>
 * The schema is not historized: the snapshot reads the table structure at the snapshot TSO and
 * streaming rebuilds a table schema whenever the row schema advertised by TiCDC changes.
 *
 * @author Aviral Srivastava
 */
@ThreadSafe
public class TiDbSchema implements DatabaseSchema<TableId> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TiDbSchema.class);

    private final TiDbConnectorConfig connectorConfig;
    private final TopicNamingStrategy<TableId> topicNamingStrategy;
    private final TableSchemaBuilder snapshotSchemaBuilder;
    private final ConcurrentMap<TableId, Entry> tables = new ConcurrentHashMap<>();

    /**
     * A table schema together with the table model it was built from and, for streamed tables,
     * the TiCDC schemas it was built for.
     */
    private record Entry(TableSchema schema, Table table, Schema ticdcKeySchema, Schema ticdcRowSchema) {
    }

    public TiDbSchema(TiDbConnectorConfig connectorConfig, TopicNamingStrategy<TableId> topicNamingStrategy) {
        this.connectorConfig = connectorConfig;
        this.topicNamingStrategy = topicNamingStrategy;
        this.snapshotSchemaBuilder = schemaBuilder(new TiDbJdbcValueConverters(), new TiDbDefaultValueConverter());
    }

    private TableSchemaBuilder schemaBuilder(ValueConverterProvider valueConverters,
                                             DefaultValueConverter defaultValueConverter) {
        return new TableSchemaBuilder(
                valueConverters,
                defaultValueConverter,
                connectorConfig.schemaNameAdjuster(),
                new CustomConverterRegistry(List.of()),
                connectorConfig.getSourceInfoStructMaker().schema(),
                SchemaFactory.get().transactionBlockSchema(),
                connectorConfig.getFieldNamer(),
                false,
                connectorConfig.getEventConvertingFailureHandlingMode());
    }

    @Override
    public void close() {
    }

    @Override
    public TableSchema schemaFor(TableId tableId) {
        final Entry entry = tables.get(tableId);
        return entry != null ? entry.schema() : null;
    }

    /**
     * @return the table model the current schema of the table was built from, if any
     */
    public Optional<Table> tableFor(TableId tableId) {
        return Optional.ofNullable(tables.get(tableId)).map(Entry::table);
    }

    /**
     * Registers the schema of a table read through the SQL endpoint during a snapshot. Rows of the
     * table are converted from JDBC values.
     */
    public TableSchema refresh(Table table) {
        final TableSchema tableSchema = snapshotSchemaBuilder.create(topicNamingStrategy, table,
                connectorConfig.getColumnFilter(), ColumnMappers.create(connectorConfig), connectorConfig.getKeyMapper());
        return register(table.id(), new Entry(tableSchema, table, null, null)).schema();
    }

    /**
     * @return {@code true} if the current schema of the table was built for exactly these TiCDC
     *         key and row schemas, so streamed rows can be emitted without a rebuild
     */
    public boolean isCurrentForStream(TableId tableId, Schema ticdcKeySchema, Schema ticdcRowSchema) {
        final Entry entry = tables.get(tableId);
        return entry != null && entry.ticdcRowSchema() != null
                && Objects.equals(entry.ticdcKeySchema(), ticdcKeySchema)
                && Objects.equals(entry.ticdcRowSchema(), ticdcRowSchema);
    }

    /**
     * Registers the schema of a streamed table. The values of streamed rows are already encoded by
     * TiCDC, so the field schemas are taken from the TiCDC row schema and the values pass through
     * unchanged apart from the configured column filters and mappers.
     */
    public TableSchema refreshFromStream(Table table, Schema ticdcKeySchema, Schema ticdcRowSchema) {
        // Defaults are part of the TiCDC field schemas already
        final TableSchema tableSchema = schemaBuilder(new TiCdcValueConverters(ticdcRowSchema), (column, expression) -> Optional.empty())
                .create(topicNamingStrategy, table, connectorConfig.getColumnFilter(), ColumnMappers.create(connectorConfig),
                        connectorConfig.getKeyMapper());
        return register(table.id(), new Entry(tableSchema, table, ticdcKeySchema, ticdcRowSchema)).schema();
    }

    private Entry register(TableId tableId, Entry entry) {
        return tables.compute(tableId, (id, existing) -> {
            if (existing != null && !sameSchemas(existing.schema(), entry.schema())) {
                LOGGER.info("Schema of table {} has changed, rebuilding envelope schema", id);
            }
            return entry;
        });
    }

    private static boolean sameSchemas(TableSchema first, TableSchema second) {
        return Objects.equals(first.keySchema(), second.keySchema()) && Objects.equals(first.valueSchema(), second.valueSchema());
    }

    @Override
    public boolean tableInformationComplete() {
        // Table schemas are learned dynamically, nothing is recovered upfront
        return false;
    }

    @Override
    public void assureNonEmptySchema() {
        if (tables.isEmpty()) {
            LOGGER.warn(DatabaseSchema.NO_CAPTURED_DATA_COLLECTIONS_WARNING);
        }
    }

    @Override
    public boolean isHistorized() {
        return false;
    }

    public Set<TableId> tableIds() {
        return tables.keySet();
    }
}
