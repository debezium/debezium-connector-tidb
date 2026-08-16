/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.tidb;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

import io.debezium.schema.SchemaNameAdjuster;

/**
 * Unit tests for {@link TiDbSnapshotSchemaBuilder}.
 *
 * @author Aviral Srivastava
 */
public class TiDbSnapshotSchemaBuilderTest {

    private static final String TOPIC = "tidb_server.inventory.products";

    private final TiDbSnapshotSchemaBuilder builder = new TiDbSnapshotSchemaBuilder(SchemaNameAdjuster.NO_OP);

    private static List<TiDbColumn> columns() {
        return List.of(
                new TiDbColumn("id", "bigint", "bigint", true),
                new TiDbColumn("name", "varchar", "varchar(255)", false),
                new TiDbColumn("weight", "decimal", "decimal(10,2)", false),
                new TiDbColumn("quantity", "int", "int unsigned", false),
                new TiDbColumn("created_on", "date", "date", false),
                new TiDbColumn("updated_at", "datetime", "datetime", false),
                new TiDbColumn("work_time", "time", "time", false));
    }

    @Test
    public void shouldBuildKeySchemaFromPrimaryKeyColumns() {
        final Schema keySchema = builder.keySchema(TOPIC, columns());
        assertThat(keySchema.name()).isEqualTo(TOPIC + ".Key");
        assertThat(keySchema.fields()).hasSize(1);
        assertThat(keySchema.field("id").schema()).isEqualTo(Schema.OPTIONAL_INT64_SCHEMA);
    }

    @Test
    public void shouldReturnNullKeySchemaWithoutPrimaryKey() {
        assertThat(builder.keySchema(TOPIC, List.of(new TiDbColumn("name", "varchar", "varchar(20)", false)))).isNull();
    }

    @Test
    public void shouldMapColumnTypesLikeTiCdc() {
        final Schema rowSchema = builder.rowSchema(TOPIC, columns());
        assertThat(rowSchema.name()).isEqualTo(TOPIC + ".Value");
        assertThat(rowSchema.field("id").schema().type()).isEqualTo(Schema.Type.INT64);
        assertThat(rowSchema.field("name").schema().type()).isEqualTo(Schema.Type.STRING);
        // TiCDC's Debezium output encodes decimals as float64
        assertThat(rowSchema.field("weight").schema().type()).isEqualTo(Schema.Type.FLOAT64);
        // Unsigned int is promoted to int64
        assertThat(rowSchema.field("quantity").schema().type()).isEqualTo(Schema.Type.INT64);
        assertThat(rowSchema.field("created_on").schema().name()).isEqualTo(io.debezium.time.Date.SCHEMA_NAME);
        assertThat(rowSchema.field("updated_at").schema().name()).isEqualTo(io.debezium.time.Timestamp.SCHEMA_NAME);
        assertThat(rowSchema.field("work_time").schema().name()).isEqualTo(io.debezium.time.MicroTime.SCHEMA_NAME);
    }

    @Test
    public void shouldConvertJdbcValues() {
        final Schema rowSchema = builder.rowSchema(TOPIC, columns());
        final Struct row = builder.rowValue(rowSchema, columns(), new Object[]{
                17L,
                "scooter",
                new BigDecimal("3.14"),
                4294967295L,
                LocalDate.of(2026, 8, 16),
                LocalDateTime.of(2026, 8, 16, 12, 30, 0),
                "13:45:30"
        });

        assertThat(row.getInt64("id")).isEqualTo(17L);
        assertThat(row.getString("name")).isEqualTo("scooter");
        assertThat(row.getFloat64("weight")).isEqualTo(3.14d);
        assertThat(row.getInt64("quantity")).isEqualTo(4294967295L);
        assertThat(row.getInt32("created_on")).isEqualTo((int) LocalDate.of(2026, 8, 16).toEpochDay());
        assertThat(row.getInt64("updated_at"))
                .isEqualTo(LocalDateTime.of(2026, 8, 16, 12, 30, 0).toInstant(java.time.ZoneOffset.UTC).toEpochMilli());
        assertThat(row.getInt64("work_time")).isEqualTo(((13L * 3600 + 45 * 60 + 30) * 1_000_000));
    }

    @Test
    public void shouldConvertNegativeAndOversizedTimeValues() {
        final List<TiDbColumn> columns = List.of(new TiDbColumn("t", "time", "time", false));
        final Schema rowSchema = builder.rowSchema(TOPIC, columns);

        assertThat(builder.rowValue(rowSchema, columns, new Object[]{ "-838:59:59" }).getInt64("t"))
                .isEqualTo(-((838L * 3600 + 59 * 60 + 59) * 1_000_000));
        assertThat(builder.rowValue(rowSchema, columns, new Object[]{ "100:00:00.5" }).getInt64("t"))
                .isEqualTo(100L * 3600 * 1_000_000 + 500_000);
    }

    @Test
    public void shouldKeepNullValues() {
        final List<TiDbColumn> cols = columns();
        final Schema rowSchema = builder.rowSchema(TOPIC, cols);
        final Struct row = builder.rowValue(rowSchema, cols, new Object[]{ 17L, null, null, null, null, null, null });
        assertThat(row.getInt64("id")).isEqualTo(17L);
        assertThat(row.getString("name")).isNull();
        assertThat(row.get("created_on")).isNull();
    }
}
