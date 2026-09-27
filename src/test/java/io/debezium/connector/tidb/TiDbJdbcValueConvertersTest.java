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
import java.util.List;

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
    public void shouldPromoteUnsignedSmallintLikeTiCdc() {
        final Column smallintUnsigned = column("s", "smallint unsigned", Types.SMALLINT);
        assertThat(converters.schemaBuilder(smallintUnsigned).build().type()).isEqualTo(Schema.Type.INT32);
        // 65535 does not fit a short and would wrap to -1
        assertThat(convert(smallintUnsigned, 65535)).isEqualTo(65535);
        assertThat(converters.schemaBuilder(column("s", "smallint", Types.SMALLINT)).build().type()).isEqualTo(Schema.Type.INT16);
    }

    @Test
    public void shouldWrapUnsignedBigintLikeTiCdc() {
        final Column bigintUnsigned = column("b", "bigint unsigned", Types.BIGINT);
        assertThat(converters.schemaBuilder(bigintUnsigned).build().type()).isEqualTo(Schema.Type.INT64);
        assertThat(convert(bigintUnsigned, new java.math.BigInteger("18446744073709551615"))).isEqualTo(-1L);
        assertThat(convert(bigintUnsigned, new java.math.BigInteger("9223372036854775808"))).isEqualTo(Long.MIN_VALUE);
    }

    @Test
    public void shouldMapFloatWithScaleToDouble() {
        final Column plain = column("f", "float", Types.REAL);
        final Column scaled = Column.editor().name("f").type("float(7,4)").jdbcType(Types.REAL).scale(4).optional(true).create();
        assertThat(converters.schemaBuilder(plain).build().type()).isEqualTo(Schema.Type.FLOAT32);
        assertThat(converters.schemaBuilder(scaled).build().type()).isEqualTo(Schema.Type.FLOAT64);
        assertThat(convert(scaled, 1.5f)).isEqualTo(1.5d);
    }

    @Test
    public void shouldMapBitsLikeTiCdc() {
        final Column bit1 = Column.editor().name("b").type("bit(1)").jdbcType(Types.BIT).length(1).optional(true).create();
        assertThat(converters.schemaBuilder(bit1).build().type()).isEqualTo(Schema.Type.BOOLEAN);
        assertThat(convert(bit1, Boolean.TRUE)).isEqualTo(true);
        assertThat(convert(bit1, new byte[]{ 0 })).isEqualTo(false);

        final Column bit12 = Column.editor().name("b").type("bit(12)").jdbcType(Types.BIT).length(12).optional(true).create();
        final Schema bitsSchema = converters.schemaBuilder(bit12).build();
        assertThat(bitsSchema.name()).isEqualTo(io.debezium.data.Bits.LOGICAL_NAME);
        assertThat(bitsSchema.parameters()).containsEntry(io.debezium.data.Bits.LENGTH_FIELD, "12");
        // The driver returns big endian bytes, TiCDC and Debezium use little endian
        assertThat((byte[]) convert(bit12, new byte[]{ 0x0A, 0x01 })).containsExactly(0x01, 0x0A);
    }

    @Test
    public void shouldEncodeBinaryStringsAsBase64LikeTiCdc() {
        final Column blob = column("data", "blob", Types.BLOB);
        assertThat(converters.schemaBuilder(blob).build().type()).isEqualTo(Schema.Type.STRING);
        assertThat(convert(blob, new byte[]{ 1, 2, 3 })).isEqualTo("AQID");
        assertThat(convert(column("v", "varbinary(16)", Types.VARBINARY), new byte[]{ (byte) 0xff })).isEqualTo("/w==");
    }

    @Test
    public void shouldUseMicrosecondDatetimeAbovePrecisionThree() {
        final LocalDateTime value = LocalDateTime.of(2026, 8, 16, 12, 30, 0, 123_456_000);
        final Column millis = Column.editor().name("d").type("datetime(3)").jdbcType(Types.TIMESTAMP).length(3).optional(true).create();
        final Column micros = Column.editor().name("d").type("datetime(6)").jdbcType(Types.TIMESTAMP).length(6).optional(true).create();

        assertThat(converters.schemaBuilder(millis).build().name()).isEqualTo(io.debezium.time.Timestamp.SCHEMA_NAME);
        assertThat(converters.schemaBuilder(micros).build().name()).isEqualTo(io.debezium.time.MicroTimestamp.SCHEMA_NAME);

        final long epochMicros = value.toInstant(ZoneOffset.UTC).getEpochSecond() * 1_000_000 + 123_456;
        assertThat(convert(millis, value)).isEqualTo(epochMicros / 1_000);
        assertThat(convert(micros, value)).isEqualTo(epochMicros);
    }

    @Test
    public void shouldFormatTimestampFractionToDeclaredPrecision() {
        final LocalDateTime value = LocalDateTime.of(2026, 8, 16, 12, 30, 0, 120_000_000);
        final Column fsp3 = Column.editor().name("t").type("timestamp(3)").jdbcType(Types.TIMESTAMP).length(3).optional(true).create();
        final Column fsp6 = Column.editor().name("t").type("timestamp(6)").jdbcType(Types.TIMESTAMP).length(6).optional(true).create();
        assertThat(convert(fsp3, value)).isEqualTo("2026-08-16T12:30:00.120Z");
        assertThat(convert(fsp6, value)).isEqualTo("2026-08-16T12:30:00.120000Z");
    }

    @Test
    public void shouldCarryEnumAndSetLabelsLikeTiCdc() {
        final Column enumColumn = Column.editor().name("e").type("enum('a','x,y')").jdbcType(Types.CHAR)
                .enumValues(List.of("a", "x,y")).optional(true).create();
        final Column setColumn = Column.editor().name("s").type("set('a','b')").jdbcType(Types.CHAR)
                .enumValues(List.of("a", "b")).optional(true).create();

        // TiCDC escapes commas inside ENUM labels but not inside SET labels
        assertThat(converters.schemaBuilder(enumColumn).build().parameters())
                .containsEntry(io.debezium.data.Enum.VALUES_FIELD, "a,x\\,y");
        assertThat(converters.schemaBuilder(setColumn).build().parameters())
                .containsEntry(io.debezium.data.EnumSet.VALUES_FIELD, "a,b");
        assertThat(convert(enumColumn, "x,y")).isEqualTo("x,y");
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
