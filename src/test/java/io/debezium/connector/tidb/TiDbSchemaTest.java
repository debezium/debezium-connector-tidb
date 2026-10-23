/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.tidb;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Types;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.spi.topic.TopicNamingStrategy;

/**
 * Unit tests for {@link TiDbSchema}.
 *
 * @author Aviral Srivastava
 */
public class TiDbSchemaTest {

    private static final TableId TABLE_ID = new TableId("inventory", null, "products");

    /**
     * The table as the snapshot reads it from {@code information_schema}.
     */
    private static final Table JDBC_TABLE = Table.editor()
            .tableId(TABLE_ID)
            .addColumn(Column.editor().name("id").type("bigint").jdbcType(Types.BIGINT).optional(false).position(1).create())
            .addColumn(Column.editor().name("name").type("varchar").jdbcType(Types.VARCHAR).optional(true).position(2).create())
            .setPrimaryKeyNames(List.of("id"))
            .create();

    private static TiDbSchema schema(Configuration.Builder config) {
        final TiDbConnectorConfig connectorConfig = new TiDbConnectorConfig(config
                .with(CommonConnectorConfig.TOPIC_PREFIX, "tidb_server")
                .with(TiDbConnectorConfig.TICDC_BOOTSTRAP_SERVERS, "localhost:9092")
                .with(TiDbConnectorConfig.TICDC_TOPICS, "ticdc-inventory")
                .build());
        @SuppressWarnings("unchecked")
        final TopicNamingStrategy<TableId> topicNamingStrategy = connectorConfig.getTopicNamingStrategy(TiDbConnectorConfig.TOPIC_NAMING_STRATEGY);
        return new TiDbSchema(connectorConfig, topicNamingStrategy);
    }

    /**
     * The row schema of the same table as TiCDC advertises it.
     */
    private static Schema ticdcRowSchema(boolean withDescription) {
        final SchemaBuilder builder = SchemaBuilder.struct().optional().name("default.inventory.products.Value")
                .field("id", Schema.INT64_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA);
        if (withDescription) {
            builder.field("description", Schema.OPTIONAL_STRING_SCHEMA);
        }
        return builder.build();
    }

    private static Schema ticdcKeySchema() {
        return SchemaBuilder.struct().name("default.inventory.products.Key").field("id", Schema.INT64_SCHEMA).build();
    }

    private static Table streamedTable(Schema rowSchema) {
        final Table table = TiCdcTableResolver.fromTiCdcSchema(TABLE_ID, rowSchema);
        return table.edit().setPrimaryKeyNames(List.of("id")).create();
    }

    @Test
    public void shouldKnowNoTablesInitially() {
        final TiDbSchema schema = schema(Configuration.create());
        assertThat(schema.schemaFor(TABLE_ID)).isNull();
        assertThat(schema.tableIds()).isEmpty();
    }

    @Test
    public void shouldBuildEnvelopeFromSnapshotTable() {
        final TiDbSchema schema = schema(Configuration.create());
        final TableSchema tableSchema = schema.refresh(JDBC_TABLE);

        assertThat(schema.schemaFor(TABLE_ID)).isSameAs(tableSchema);
        assertThat(tableSchema.keySchema().field("id")).isNotNull();

        final Schema valueSchema = tableSchema.getEnvelopeSchema().schema();
        assertThat(valueSchema.name()).isEqualTo("tidb_server.inventory.products.Envelope");
        assertThat(valueSchema.field("after").schema().name()).isEqualTo("tidb_server.inventory.products.Value");
        assertThat(valueSchema.field("source").schema().field(SourceInfo.COMMIT_TS_KEY)).isNotNull();
    }

    @Test
    public void shouldKeepOneSchemaAcrossSnapshotToStreamingHandoff() {
        final TiDbSchema schema = schema(Configuration.create());
        final TableSchema snapshot = schema.refresh(JDBC_TABLE);

        assertThat(schema.isCurrentForStream(TABLE_ID, ticdcKeySchema(), ticdcRowSchema(false))).isFalse();
        final TableSchema streamed = schema.refreshFromStream(streamedTable(ticdcRowSchema(false)), ticdcKeySchema(), ticdcRowSchema(false));

        // The record names follow the connector's topic naming, not TiCDC's
        assertThat(streamed.keySchema()).isEqualTo(snapshot.keySchema());
        assertThat(streamed.valueSchema()).isEqualTo(snapshot.valueSchema());
        assertThat(schema.isCurrentForStream(TABLE_ID, ticdcKeySchema(), ticdcRowSchema(false))).isTrue();
    }

    @Test
    public void shouldRebuildSchemaWhenTiCdcRowSchemaChanges() {
        final TiDbSchema schema = schema(Configuration.create());
        schema.refreshFromStream(streamedTable(ticdcRowSchema(false)), ticdcKeySchema(), ticdcRowSchema(false));

        assertThat(schema.isCurrentForStream(TABLE_ID, ticdcKeySchema(), ticdcRowSchema(true))).isFalse();
        final TableSchema changed = schema.refreshFromStream(streamedTable(ticdcRowSchema(true)), ticdcKeySchema(), ticdcRowSchema(true));
        assertThat(changed.valueSchema().field("description")).isNotNull();
    }

    @Test
    public void shouldApplyColumnFilterAndKeyColumnsToStreamedTables() {
        final TiDbSchema schema = schema(Configuration.create()
                .with("column.exclude.list", "inventory\\.products\\.name")
                .with("message.key.columns", "inventory.products:id,name"));

        final TableSchema snapshot = schema.refresh(JDBC_TABLE);
        final TableSchema streamed = schema.refreshFromStream(streamedTable(ticdcRowSchema(false)), ticdcKeySchema(), ticdcRowSchema(false));

        for (TableSchema tableSchema : List.of(snapshot, streamed)) {
            assertThat(tableSchema.valueSchema().field("name")).isNull();
            assertThat(tableSchema.keySchema().fields()).extracting(org.apache.kafka.connect.data.Field::name).containsExactly("id", "name");
        }
        assertThat(streamed.valueSchema()).isEqualTo(snapshot.valueSchema());
        assertThat(streamed.keySchema()).isEqualTo(snapshot.keySchema());
    }

    @Test
    public void shouldApplyColumnMasksToStreamedTables() {
        final TiDbSchema schema = schema(Configuration.create()
                .with("column.mask.with.5.chars", "inventory\\.products\\.name"));

        // With the SQL endpoint configured, streamed tables carry the JDBC column metadata
        final TableSchema snapshot = schema.refresh(JDBC_TABLE);
        final TableSchema streamed = schema.refreshFromStream(JDBC_TABLE, ticdcKeySchema(), ticdcRowSchema(false));

        assertThat(streamed.valueSchema()).isEqualTo(snapshot.valueSchema());
        assertThat(streamed.valueFromColumnData(new Object[]{ 1L, "secret" }).getString("name")).isEqualTo("*****");
        assertThat(snapshot.valueFromColumnData(new Object[]{ 1L, "secret" }).getString("name")).isEqualTo("*****");
    }
}
