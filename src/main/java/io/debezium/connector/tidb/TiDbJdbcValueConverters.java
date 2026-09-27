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
import java.util.Base64;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.DebeziumException;
import io.debezium.data.Bits;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;
import io.debezium.relational.ValueConverterProvider;
import io.debezium.time.MicroTime;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.Year;
import io.debezium.time.ZonedTimestamp;

/**
 * {@link ValueConverterProvider} for rows read from TiDB's MySQL compatible SQL endpoint during
 * snapshots.
 * <p>
 * The schemas and values mirror what TiCDC emits in its Debezium output mode (tiflow
 * {@code pkg/sink/codec/debezium/codec.go}), so that snapshot events and streamed events describe
 * the same table the same way. Notable consequences of following TiCDC: decimals map to float64,
 * binary strings map to base64 encoded strings, and {@code BIGINT UNSIGNED} values above
 * {@code Long.MAX_VALUE} wrap into negative int64 values exactly as TiCDC encodes them.
 * <p>
 * The snapshot session is pinned to UTC, so TIMESTAMP values arrive as UTC wall time; DATETIME
 * carries no zone and is interpreted as UTC by convention.
 * <p>
 * Temporal fractional second precision is carried in {@link Column#length()}, BIT widths in
 * {@link Column#length()}, and the FLOAT scale in {@link Column#scale()}.
 *
 * @author Aviral Srivastava
 */
public class TiDbJdbcValueConverters implements ValueConverterProvider {

    private static final String VECTOR_SCHEMA_NAME = "io.debezium.data.TiDBVectorFloat32";

    @Override
    public SchemaBuilder schemaBuilder(Column column) {
        switch (baseType(column)) {
            case "tinyint":
                return SchemaBuilder.int16();
            case "smallint":
                return isUnsigned(column) ? SchemaBuilder.int32() : SchemaBuilder.int16();
            case "mediumint":
                return SchemaBuilder.int32();
            case "int":
                return isUnsigned(column) ? SchemaBuilder.int64() : SchemaBuilder.int32();
            case "bigint":
                return SchemaBuilder.int64();
            case "float":
                // FLOAT(M,D) is encoded as double by TiCDC, a plain FLOAT as float
                return column.scale().isPresent() ? SchemaBuilder.float64() : SchemaBuilder.float32();
            case "double":
            case "decimal":
                return SchemaBuilder.float64();
            case "bit":
                return bitLength(column) == 1 ? SchemaBuilder.bool() : Bits.builder(bitLength(column));
            case "date":
                return io.debezium.time.Date.builder();
            case "datetime":
                return fsp(column) <= 3 ? io.debezium.time.Timestamp.builder() : MicroTimestamp.builder();
            case "timestamp":
                return ZonedTimestamp.builder();
            case "time":
                return MicroTime.builder();
            case "year":
                return Year.builder();
            case "json":
                return io.debezium.data.Json.builder();
            case "enum":
                return io.debezium.data.Enum.builder(column.enumValues().stream()
                        .map(value -> value.replace(",", "\\,"))
                        .collect(Collectors.joining(",")));
            case "set":
                return io.debezium.data.EnumSet.builder(String.join(",", column.enumValues()));
            case "vector":
                return SchemaBuilder.string().name(VECTOR_SCHEMA_NAME);
            default:
                // Character and binary strings: TiCDC declares both as string and base64
                // encodes the binary ones
                return SchemaBuilder.string();
        }
    }

    @Override
    public ValueConverter converter(Column column, Field fieldDefn) {
        switch (baseType(column)) {
            case "tinyint":
                return nullSafe(value -> ((Number) value).shortValue());
            case "smallint":
                return isUnsigned(column)
                        ? nullSafe(value -> ((Number) value).intValue())
                        : nullSafe(value -> ((Number) value).shortValue());
            case "mediumint":
                return nullSafe(value -> ((Number) value).intValue());
            case "int":
                return isUnsigned(column)
                        ? nullSafe(value -> ((Number) value).longValue())
                        : nullSafe(value -> ((Number) value).intValue());
            case "bigint":
                // BigInteger.longValue() wraps values above Long.MAX_VALUE exactly like TiCDC's
                // int64(uint64) conversion, keeping snapshot and streamed values identical
                return nullSafe(value -> value instanceof BigInteger bigInteger
                        ? bigInteger.longValue()
                        : ((Number) value).longValue());
            case "float":
                return column.scale().isPresent()
                        ? nullSafe(value -> ((Number) value).doubleValue())
                        : nullSafe(value -> ((Number) value).floatValue());
            case "double":
                return nullSafe(value -> ((Number) value).doubleValue());
            case "decimal":
                return nullSafe(value -> value instanceof BigDecimal bigDecimal
                        ? bigDecimal.doubleValue()
                        : ((Number) value).doubleValue());
            case "bit":
                return bitLength(column) == 1
                        ? nullSafe(value -> toBoolean(column, value))
                        : nullSafe(value -> toLittleEndianBits(column, value));
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
                        final long micros = Duration.between(LocalDateTime.of(1970, 1, 1, 0, 0), localDateTime).toNanos() / 1_000;
                        return fsp(column) <= 3 ? Math.floorDiv(micros, 1_000L) : micros;
                    }
                    throw unexpectedValue(column, value);
                });
            case "timestamp":
                return nullSafe(value -> {
                    if (value instanceof LocalDateTime localDateTime) {
                        // Exactly the declared number of fractional digits, as TiCDC emits them
                        return ZonedTimestamp.toIsoString(localDateTime.atZone(ZoneOffset.UTC), null, fsp(column));
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
            case "binary":
            case "varbinary":
            case "tinyblob":
            case "blob":
            case "mediumblob":
            case "longblob":
                return nullSafe(value -> {
                    if (value instanceof byte[] bytes) {
                        return Base64.getEncoder().encodeToString(bytes);
                    }
                    throw unexpectedValue(column, value);
                });
            default:
                return nullSafe(value -> value instanceof byte[] bytes
                        ? new String(bytes, java.nio.charset.StandardCharsets.UTF_8)
                        : value.toString());
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
            typeName = typeName.substring(0, parenthesis) + typeName.substring(typeName.lastIndexOf(')') + 1);
        }
        final int space = typeName.indexOf(' ');
        return space > 0 ? typeName.substring(0, space) : typeName;
    }

    static boolean isUnsigned(Column column) {
        return column.typeName().toLowerCase().endsWith("unsigned")
                || column.typeName().toLowerCase().contains(" unsigned ");
    }

    private static int fsp(Column column) {
        return Math.max(column.length(), 0);
    }

    private static int bitLength(Column column) {
        return Math.max(column.length(), 1);
    }

    private static boolean toBoolean(Column column, Object value) {
        if (value instanceof Boolean bool) {
            return bool;
        }
        if (value instanceof byte[] bytes) {
            for (byte b : bytes) {
                if (b != 0) {
                    return true;
                }
            }
            return false;
        }
        if (value instanceof Number number) {
            return number.longValue() != 0;
        }
        throw unexpectedValue(column, value);
    }

    /**
     * The driver returns BIT values big endian; Debezium's {@code Bits} and TiCDC use little endian
     * bytes sized to hold the declared number of bits.
     */
    private static byte[] toLittleEndianBits(Column column, Object value) {
        final byte[] bigEndian;
        if (value instanceof byte[] bytes) {
            bigEndian = bytes;
        }
        else if (value instanceof Number number) {
            bigEndian = BigInteger.valueOf(number.longValue()).toByteArray();
        }
        else {
            throw unexpectedValue(column, value);
        }
        final int size = (bitLength(column) + 7) / 8;
        final byte[] littleEndian = new byte[size];
        for (int i = 0; i < size && i < bigEndian.length; i++) {
            littleEndian[i] = bigEndian[bigEndian.length - 1 - i];
        }
        return littleEndian;
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
