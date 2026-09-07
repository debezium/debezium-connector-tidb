/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.tidb;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Test;

import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;

/**
 * Unit tests for {@link TiDbJdbcValueConverters}.
 *
 * @author Aviral Srivastava
 */
public class TiDbJdbcValueConvertersTest {

    private final TiDbJdbcValueConverters converters = new TiDbJdbcValueConverters();

    private static Column column(String name, String typeName, int jdbcType) {
        return Column.editor().name(name).type(typeName).jdbcType(jdbcType).optional(true).create();
    }

    private Object convert(Column column, Object value) {
        final ValueConverter converter = converters.converter(column, null);
        return converter.convert(value);
    }

    @Test
    public void shouldMapColumnTypesLikeTiCdc() {
        assertThat(converters.schemaBuilder(column("id", "bigint", Types.BIGINT)).build().type())
                .isEqualTo(Schema.Type.INT64);
        assertThat(converters.schemaBuilder(column("name", "varchar(255)", Types.VARCHAR)).build().type())
                .isEqualTo(Schema.Type.STRING);
        // TiCDC's Debezium output encodes decimals as float64
        assertThat(converters.schemaBuilder(column("weight", "decimal(10,2)", Types.DECIMAL)).build().type())
                .isEqualTo(Schema.Type.FLOAT64);
        // Unsigned int is promoted to int64
        assertThat(converters.schemaBuilder(column("quantity", "int(10) unsigned", Types.INTEGER)).build().type())
                .isEqualTo(Schema.Type.INT64);
        assertThat(converters.schemaBuilder(column("created_on", "date", Types.DATE)).build().name())
                .isEqualTo(io.debezium.time.Date.SCHEMA_NAME);
        assertThat(converters.schemaBuilder(column("updated_at", "datetime", Types.TIMESTAMP)).build().name())
                .isEqualTo(io.debezium.time.Timestamp.SCHEMA_NAME);
        assertThat(converters.schemaBuilder(column("registered_at", "timestamp", Types.TIMESTAMP)).build().name())
                .isEqualTo(io.debezium.time.ZonedTimestamp.SCHEMA_NAME);
        assertThat(converters.schemaBuilder(column("work_time", "time", Types.TIME)).build().name())
                .isEqualTo(io.debezium.time.MicroTime.SCHEMA_NAME);
    }

    @Test
    public void shouldConvertJdbcValues() {
        assertThat(convert(column("id", "bigint", Types.BIGINT), 17L)).isEqualTo(17L);
        assertThat(convert(column("weight", "decimal(10,2)", Types.DECIMAL), new BigDecimal("3.14"))).isEqualTo(3.14d);
        assertThat(convert(column("quantity", "int(10) unsigned", Types.INTEGER), 4294967295L)).isEqualTo(4294967295L);
        assertThat(convert(column("created_on", "date", Types.DATE), LocalDate.of(2026, 8, 16)))
                .isEqualTo((int) LocalDate.of(2026, 8, 16).toEpochDay());
        assertThat(convert(column("updated_at", "datetime", Types.TIMESTAMP), LocalDateTime.of(2026, 8, 16, 12, 30, 0)))
                .isEqualTo(LocalDateTime.of(2026, 8, 16, 12, 30, 0).toInstant(ZoneOffset.UTC).toEpochMilli());
        assertThat(convert(column("registered_at", "timestamp", Types.TIMESTAMP), LocalDateTime.of(2026, 8, 16, 12, 30, 0)))
                .isEqualTo("2026-08-16T12:30:00Z");
    }

    @Test
    public void shouldConvertNegativeAndOversizedTimeValues() {
        final Column time = column("t", "time", Types.TIME);
        assertThat(convert(time, "13:45:30")).isEqualTo((13L * 3600 + 45 * 60 + 30) * 1_000_000);
        assertThat(convert(time, "-838:59:59")).isEqualTo(-((838L * 3600 + 59 * 60 + 59) * 1_000_000));
        assertThat(convert(time, "100:00:00.5")).isEqualTo(100L * 3600 * 1_000_000 + 500_000);
    }

    @Test
    public void shouldKeepNullValues() {
        assertThat(convert(column("name", "varchar(255)", Types.VARCHAR), null)).isNull();
        assertThat(convert(column("created_on", "date", Types.DATE), null)).isNull();
    }

    @Test
    public void shouldParseBaseTypes() {
        assertThat(TiDbJdbcValueConverters.baseType(column("a", "int(11) unsigned", Types.INTEGER))).isEqualTo("int");
        assertThat(TiDbJdbcValueConverters.baseType(column("b", "decimal(10,2)", Types.DECIMAL))).isEqualTo("decimal");
        assertThat(TiDbJdbcValueConverters.baseType(column("c", "varchar(255)", Types.VARCHAR))).isEqualTo("varchar");
        assertThat(TiDbJdbcValueConverters.isUnsigned(column("a", "int(11) unsigned", Types.INTEGER))).isTrue();
        assertThat(TiDbJdbcValueConverters.isUnsigned(column("b", "int(11)", Types.INTEGER))).isFalse();
    }
}
