/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.tidb;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.Locale;
import java.util.Optional;

import io.debezium.relational.Column;
import io.debezium.relational.DefaultValueConverter;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.ZonedTimestamp;

/**
 * {@link DefaultValueConverter} for column defaults read from {@code information_schema} during
 * snapshots.
 * <p>
 * The defaults follow the rules of TiCDC's Debezium encoder (tiflow
 * {@code pkg/sink/codec/debezium/codec.go}), so that the field schemas of snapshot events and
 * streamed events carry the same default values. The returned objects already have the Java type
 * of the field schema, since {@code TableSchemaBuilder} does not run the value converter on
 * defaults.
 *
 * @author Aviral Srivastava
 */
public class TiDbDefaultValueConverter implements DefaultValueConverter {

    private static final String CURRENT_TIMESTAMP = "CURRENT_TIMESTAMP";

    private static final DateTimeFormatter DATE_TIME = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss")
            .optionalStart()
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .optionalEnd()
            .toFormatter(Locale.ROOT);

    @Override
    public Optional<Object> parseDefaultValue(Column column, String defaultValueExpression) {
        if (defaultValueExpression == null) {
            return Optional.empty();
        }
        final String value = defaultValueExpression.trim();
        switch (TiDbJdbcValueConverters.baseType(column)) {
            case "tinyint":
                return parseNumber(value).map(number -> (Object) (short) number.doubleValue());
            case "smallint":
                return parseNumber(value).map(number -> TiDbJdbcValueConverters.isUnsigned(column)
                        ? (Object) (int) number.doubleValue()
                        : (Object) (short) number.doubleValue());
            case "mediumint":
                return parseNumber(value).map(number -> (Object) (int) number.doubleValue());
            case "int":
                return parseNumber(value).map(number -> TiDbJdbcValueConverters.isUnsigned(column)
                        ? (Object) (long) number.doubleValue()
                        : (Object) (int) number.doubleValue());
            case "bigint":
                return parseNumber(value).map(number -> (Object) (long) number.doubleValue());
            case "float":
                return parseNumber(value).map(number -> column.scale().isPresent()
                        ? (Object) number.doubleValue()
                        : (Object) number.floatValue());
            case "double":
            case "decimal":
                return parseNumber(value).map(number -> (Object) number.doubleValue());
            case "year":
                return parseNumber(value).map(number -> {
                    double year = number.doubleValue();
                    if (year < 70) {
                        // DEFAULT 1 means 2001
                        year += 2000;
                    }
                    else if (year < 100) {
                        // DEFAULT 99 means 1999
                        year += 1900;
                    }
                    return (Object) (int) year;
                });
            case "bit":
                return parseBit(value).map(bits -> TiDbJdbcValueConverters.bitLength(column) == 1
                        ? (Object) (bits != 0)
                        : (Object) TiDbJdbcValueConverters.toLittleEndianBits(column, bits));
            case "date":
                return parseDate(column, value);
            case "datetime":
                return parseDateTime(column, value);
            case "timestamp":
                return parseTimestamp(column, value);
            case "time":
                try {
                    return Optional.of(TiDbJdbcValueConverters.timeMicros(value));
                }
                catch (RuntimeException e) {
                    return Optional.empty();
                }
            default:
                // Character and binary strings, ENUM, SET, JSON and vectors: TiCDC writes the
                // default literal as is
                return Optional.of(value);
        }
    }

    private static Optional<Object> parseDate(Column column, String value) {
        try {
            return Optional.of((int) withTwoDigitYear(LocalDate.parse(value)).toEpochDay());
        }
        catch (DateTimeParseException e) {
            // An invalid date such as 0000-00-00
            return column.isOptional() ? Optional.empty() : Optional.of(0);
        }
    }

    private static Optional<Object> parseDateTime(Column column, String value) {
        if (isCurrentTimestamp(value)) {
            return Optional.of(0L);
        }
        final LocalDateTime dateTime;
        try {
            dateTime = withTwoDigitYear(LocalDateTime.parse(value, DATE_TIME));
        }
        catch (DateTimeParseException e) {
            return Optional.of(0L);
        }
        return Optional.of(TiDbJdbcValueConverters.fsp(column) <= 3
                ? io.debezium.time.Timestamp.toEpochMillis(dateTime, null)
                : MicroTimestamp.toEpochMicros(dateTime, null));
    }

    private static Optional<Object> parseTimestamp(Column column, String value) {
        final Optional<Object> epoch = column.isOptional()
                ? Optional.empty()
                : Optional.of(TiDbJdbcValueConverters.EPOCH_ZONED_TIMESTAMP);
        if (isCurrentTimestamp(value)) {
            return epoch;
        }
        final LocalDateTime dateTime;
        try {
            dateTime = LocalDateTime.parse(value, DATE_TIME);
        }
        catch (DateTimeParseException e) {
            return epoch;
        }
        if (dateTime.isBefore(LocalDateTime.of(1970, 1, 1, 0, 0, 1))) {
            // Below the smallest TIMESTAMP value, e.g. a zero date
            return epoch;
        }
        return Optional.of(ZonedTimestamp.toIsoString(dateTime.atZone(ZoneOffset.UTC), null, TiDbJdbcValueConverters.fsp(column)));
    }

    private static boolean isCurrentTimestamp(String value) {
        return value.toUpperCase(Locale.ROOT).startsWith(CURRENT_TIMESTAMP);
    }

    private static LocalDate withTwoDigitYear(LocalDate date) {
        if (date.getYear() < 70) {
            return date.plusYears(2000);
        }
        else if (date.getYear() < 100) {
            return date.plusYears(1900);
        }
        return date;
    }

    private static LocalDateTime withTwoDigitYear(LocalDateTime dateTime) {
        return withTwoDigitYear(dateTime.toLocalDate()).atTime(dateTime.toLocalTime());
    }

    private static Optional<Double> parseNumber(String value) {
        try {
            return Optional.of(Double.parseDouble(value));
        }
        catch (NumberFormatException e) {
            return Optional.empty();
        }
    }

    /**
     * Parses a BIT default as reported by {@code information_schema}, e.g. {@code b'101'}.
     */
    private static Optional<Long> parseBit(String value) {
        String digits = value;
        if (digits.length() >= 3 && (digits.startsWith("b'") || digits.startsWith("B'")) && digits.endsWith("'")) {
            digits = digits.substring(2, digits.length() - 1);
        }
        try {
            return Optional.of(Long.parseUnsignedLong(digits, 2));
        }
        catch (NumberFormatException e) {
            return Optional.empty();
        }
    }
}
