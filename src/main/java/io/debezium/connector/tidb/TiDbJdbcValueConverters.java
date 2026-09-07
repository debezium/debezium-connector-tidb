/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.tidb;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.DebeziumException;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;
import io.debezium.relational.ValueConverterProvider;
import io.debezium.time.MicroTime;
import io.debezium.time.Year;
import io.debezium.time.ZonedTimestamp;

/**
 * {@link ValueConverterProvider} for rows read from TiDB's MySQL compatible SQL endpoint during
 * snapshots.
 * <p>
 * The mapping follows the schemas TiCDC produces in its Debezium output mode, so that snapshot
 * events and streamed events describe the same table the same way: integer types map by width
 * with unsigned types promoted, decimals map to float64, and temporal types map to the Debezium
 * semantic time types. The snapshot session is pinned to UTC, so TIMESTAMP values arrive as UTC
 * wall time; DATETIME carries no zone and is interpreted as UTC by convention.
 *
 * @author Aviral Srivastava
 */
public class TiDbJdbcValueConverters implements ValueConverterProvider {

    @Override
    public SchemaBuilder schemaBuilder(Column column) {
        switch (baseType(column)) {
            case "tinyint":
            case "smallint":
                return SchemaBuilder.int16();
            case "mediumint":
                return SchemaBuilder.int32();
            case "int":
                return isUnsigned(column) ? SchemaBuilder.int64() : SchemaBuilder.int32();
            case "bigint":
                return SchemaBuilder.int64();
            case "float":
                return SchemaBuilder.float32();
            case "double":
            case "decimal":
                // TiCDC's Debezium output encodes decimals as float64
                return SchemaBuilder.float64();
            case "binary":
            case "varbinary":
            case "tinyblob":
            case "blob":
            case "mediumblob":
            case "longblob":
            case "bit":
                return SchemaBuilder.bytes();
            case "date":
                return io.debezium.time.Date.builder();
            case "datetime":
                return io.debezium.time.Timestamp.builder();
            case "timestamp":
                return ZonedTimestamp.builder();
            case "time":
                return MicroTime.builder();
            case "year":
                return Year.builder();
            case "json":
                return io.debezium.data.Json.builder();
            case "enum":
                return io.debezium.data.Enum.builder("");
            case "set":
                return io.debezium.data.EnumSet.builder("");
            default:
                return SchemaBuilder.string();
        }
    }

    @Override
    public ValueConverter converter(Column column, Field fieldDefn) {
        switch (baseType(column)) {
            case "tinyint":
            case "smallint":
                return nullSafe(value -> ((Number) value).shortValue());
            case "mediumint":
                return nullSafe(value -> ((Number) value).intValue());
            case "int":
                return isUnsigned(column)
                        ? nullSafe(value -> ((Number) value).longValue())
                        : nullSafe(value -> ((Number) value).intValue());
            case "bigint":
                return nullSafe(value -> value instanceof BigInteger bigInteger
                        ? bigInteger.longValue()
                        : ((Number) value).longValue());
            case "float":
                return nullSafe(value -> ((Number) value).floatValue());
            case "double":
                return nullSafe(value -> ((Number) value).doubleValue());
            case "decimal":
                return nullSafe(value -> value instanceof BigDecimal bigDecimal
                        ? bigDecimal.doubleValue()
                        : ((Number) value).doubleValue());
            case "date":
                return nullSafe(value -> {
                    if (value instanceof LocalDate localDate) {
                        return (int) localDate.toEpochDay();
                    }
                    throw unexpectedValue(column, value);
                });
            case "datetime":
                return nullSafe(value -> {
                    if (value instanceof LocalDateTime localDateTime) {
                        return localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
                    }
                    throw unexpectedValue(column, value);
                });
            case "timestamp":
                return nullSafe(value -> {
                    if (value instanceof LocalDateTime localDateTime) {
                        return ZonedTimestamp.toIsoString(localDateTime.atOffset(ZoneOffset.UTC), null);
                    }
                    throw unexpectedValue(column, value);
                });
            case "time":
                return nullSafe(value -> {
                    if (value instanceof String text) {
                        return timeMicros(text);
                    }
                    throw unexpectedValue(column, value);
                });
            case "year":
                return nullSafe(value -> ((Number) value).intValue());
            default:
                return nullSafe(value -> value);
        }
    }

    /**
     * @return the base type of the column, e.g. {@code int} for a type name of
     *         {@code int(11) unsigned}
     */
    static String baseType(Column column) {
        String typeName = column.typeName().toLowerCase();
        final int parenthesis = typeName.indexOf('(');
        if (parenthesis > 0) {
            typeName = typeName.substring(0, parenthesis) + typeName.substring(typeName.indexOf(')', parenthesis) + 1);
        }
        final int space = typeName.indexOf(' ');
        return space > 0 ? typeName.substring(0, space) : typeName;
    }

    static boolean isUnsigned(Column column) {
        return column.typeName().toLowerCase().contains("unsigned");
    }

    private static ValueConverter nullSafe(ValueConverter converter) {
        return value -> value == null ? null : converter.convert(value);
    }

    /**
     * Converts a TiDB TIME literal, e.g. {@code 13:45:30} or {@code -838:59:59}, to microseconds.
     * The type is a duration and can exceed 24 hours or be negative, so it cannot be parsed as a
     * time of day.
     */
    private static long timeMicros(String text) {
        final boolean negative = text.startsWith("-");
        final String[] parts = (negative ? text.substring(1) : text).split(":");
        if (parts.length != 3) {
            throw new DebeziumException("Unexpected TIME literal '" + text + "'");
        }
        final String[] secondsAndFraction = parts[2].split("\\.");
        Duration duration = Duration.ofHours(Long.parseLong(parts[0]))
                .plusMinutes(Long.parseLong(parts[1]))
                .plusSeconds(Long.parseLong(secondsAndFraction[0]));
        if (secondsAndFraction.length > 1) {
            final String fraction = (secondsAndFraction[1] + "000000").substring(0, 6);
            duration = duration.plusNanos(Long.parseLong(fraction) * 1_000);
        }
        final long micros = duration.toNanos() / 1_000;
        return negative ? -micros : micros;
    }

    private static DebeziumException unexpectedValue(Column column, Object value) {
        return new DebeziumException("Unexpected JDBC value of type " + value.getClass().getName()
                + " for column " + column.name() + " of type " + column.typeName());
    }
}
