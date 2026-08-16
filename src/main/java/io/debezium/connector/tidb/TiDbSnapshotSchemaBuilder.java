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
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import io.debezium.DebeziumException;
import io.debezium.schema.SchemaNameAdjuster;

/**
 * Builds Kafka Connect schemas and values for snapshotted rows.
 * <p>
 * The mapping follows the schemas TiCDC produces in its Debezium output mode, so that snapshot
 * events and streamed events describe the same table the same way: integer types map by width
 * with unsigned types promoted, decimals map to float64, and temporal types map to the Debezium
 * semantic time types. If the mapping ever drifts from TiCDC's, the streaming side rebuilds its
 * envelope from the first consumed message, so a drift degrades schema continuity but not
 * correctness.
 *
 * @author Aviral Srivastava
 */
public class TiDbSnapshotSchemaBuilder {

    private static final String KEY_SCHEMA_SUFFIX = ".Key";
    private static final String VALUE_SCHEMA_SUFFIX = ".Value";

    private final SchemaNameAdjuster adjuster;

    public TiDbSnapshotSchemaBuilder(SchemaNameAdjuster adjuster) {
        this.adjuster = adjuster;
    }

    /**
     * @return the schema of the message key, built from the primary key columns, or {@code null}
     *         when the table has no primary key
     */
    public Schema keySchema(String topicName, List<TiDbColumn> columns) {
        final List<TiDbColumn> keyColumns = columns.stream().filter(TiDbColumn::primaryKey).toList();
        if (keyColumns.isEmpty()) {
            return null;
        }
        final SchemaBuilder builder = SchemaBuilder.struct().name(adjuster.adjust(topicName + KEY_SCHEMA_SUFFIX));
        for (TiDbColumn column : keyColumns) {
            builder.field(column.name(), fieldSchema(column));
        }
        return builder.build();
    }

    /**
     * @return the schema of a full row image
     */
    public Schema rowSchema(String topicName, List<TiDbColumn> columns) {
        final SchemaBuilder builder = SchemaBuilder.struct()
                .name(adjuster.adjust(topicName + VALUE_SCHEMA_SUFFIX))
                .optional();
        for (TiDbColumn column : columns) {
            builder.field(column.name(), fieldSchema(column));
        }
        return builder.build();
    }

    /**
     * @return the struct for a snapshotted row against the given schema, converting the raw JDBC
     *         values to the Connect representation the schema declares
     */
    public Struct rowValue(Schema rowSchema, List<TiDbColumn> columns, Object[] row) {
        final Struct struct = new Struct(rowSchema);
        for (int i = 0; i < columns.size(); i++) {
            struct.put(columns.get(i).name(), convertValue(columns.get(i), row[i]));
        }
        return struct;
    }

    /**
     * @return the key struct for a snapshotted row, or {@code null} when the table has no
     *         primary key
     */
    public Struct keyValue(Schema keySchema, List<TiDbColumn> columns, Object[] row) {
        if (keySchema == null) {
            return null;
        }
        final Struct struct = new Struct(keySchema);
        for (int i = 0; i < columns.size(); i++) {
            final TiDbColumn column = columns.get(i);
            if (column.primaryKey()) {
                struct.put(column.name(), convertValue(column, row[i]));
            }
        }
        return struct;
    }

    private Schema fieldSchema(TiDbColumn column) {
        switch (column.dataType()) {
            case "tinyint":
            case "smallint":
                return Schema.OPTIONAL_INT16_SCHEMA;
            case "mediumint":
                return Schema.OPTIONAL_INT32_SCHEMA;
            case "int":
                return column.isUnsigned() ? Schema.OPTIONAL_INT64_SCHEMA : Schema.OPTIONAL_INT32_SCHEMA;
            case "bigint":
                return Schema.OPTIONAL_INT64_SCHEMA;
            case "float":
                return Schema.OPTIONAL_FLOAT32_SCHEMA;
            case "double":
            case "decimal":
                // TiCDC's Debezium output encodes decimals as float64
                return Schema.OPTIONAL_FLOAT64_SCHEMA;
            case "char":
            case "varchar":
            case "tinytext":
            case "text":
            case "mediumtext":
            case "longtext":
                return Schema.OPTIONAL_STRING_SCHEMA;
            case "binary":
            case "varbinary":
            case "tinyblob":
            case "blob":
            case "mediumblob":
            case "longblob":
            case "bit":
                return Schema.OPTIONAL_BYTES_SCHEMA;
            case "date":
                return io.debezium.time.Date.builder().optional().build();
            case "datetime":
                return io.debezium.time.Timestamp.builder().optional().build();
            case "timestamp":
                return io.debezium.time.ZonedTimestamp.builder().optional().build();
            case "time":
                return io.debezium.time.MicroTime.builder().optional().build();
            case "year":
                return io.debezium.time.Year.builder().optional().build();
            case "json":
                return io.debezium.data.Json.builder().optional().build();
            case "enum":
                return io.debezium.data.Enum.builder("").optional().build();
            case "set":
                return io.debezium.data.EnumSet.builder("").optional().build();
            default:
                return Schema.OPTIONAL_STRING_SCHEMA;
        }
    }

    private Object convertValue(TiDbColumn column, Object value) {
        if (value == null) {
            return null;
        }
        switch (column.dataType()) {
            case "tinyint":
            case "smallint":
                return ((Number) value).shortValue();
            case "mediumint":
                return ((Number) value).intValue();
            case "int":
                return column.isUnsigned() ? ((Number) value).longValue() : ((Number) value).intValue();
            case "bigint":
                if (value instanceof BigInteger bigInteger) {
                    return bigInteger.longValue();
                }
                return ((Number) value).longValue();
            case "float":
                return ((Number) value).floatValue();
            case "double":
                return ((Number) value).doubleValue();
            case "decimal":
                if (value instanceof BigDecimal bigDecimal) {
                    return bigDecimal.doubleValue();
                }
                return ((Number) value).doubleValue();
            case "date":
                if (value instanceof LocalDate localDate) {
                    return (int) localDate.toEpochDay();
                }
                throw unexpectedValue(column, value);
            case "datetime":
                if (value instanceof LocalDateTime localDateTime) {
                    return localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
                }
                throw unexpectedValue(column, value);
            case "timestamp":
                if (value instanceof LocalDateTime localDateTime) {
                    return io.debezium.time.ZonedTimestamp.toIsoString(localDateTime.atOffset(ZoneOffset.UTC), null);
                }
                throw unexpectedValue(column, value);
            case "time":
                if (value instanceof String text) {
                    return timeMicros(text);
                }
                throw unexpectedValue(column, value);
            case "year":
                return ((Number) value).intValue();
            default:
                return value;
        }
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
            return Duration.between(LocalTime.MIN, LocalTime.parse(text)).toNanos() / 1_000;
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

    private static DebeziumException unexpectedValue(TiDbColumn column, Object value) {
        return new DebeziumException("Unexpected JDBC value of type " + value.getClass().getName()
                + " for column " + column.name() + " of type " + column.dataType());
    }
}
