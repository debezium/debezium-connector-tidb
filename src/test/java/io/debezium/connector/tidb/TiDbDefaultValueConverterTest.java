/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.tidb;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Types;

import org.junit.jupiter.api.Test;

import io.debezium.relational.Column;

/**
 * Unit tests for {@link TiDbDefaultValueConverter}. The expected values are the defaults TiCDC
 * writes into its field schemas.
 *
 * @author Aviral Srivastava
 */
public class TiDbDefaultValueConverterTest {

    private final TiDbDefaultValueConverter converter = new TiDbDefaultValueConverter();

    private Object parse(String typeName, int length, boolean optional, String expression) {
        final Column column = Column.editor().name("c").type(typeName).jdbcType(Types.OTHER).length(length).optional(optional).create();
        return converter.parseDefaultValue(column, expression).orElse(null);
    }

    @Test
    public void shouldConvertNumericDefaultsToTheFieldType() {
        assertThat(parse("tinyint(1)", -1, true, "1")).isEqualTo((short) 1);
        assertThat(parse("int", -1, true, "7")).isEqualTo(7);
        assertThat(parse("int unsigned", -1, true, "7")).isEqualTo(7L);
        assertThat(parse("decimal(5,2)", -1, true, "1.50")).isEqualTo(1.5d);
        assertThat(parse("float", -1, true, "2.5")).isEqualTo(2.5f);
        assertThat(parse("year", -1, true, "2001")).isEqualTo(2001);
        assertThat(parse("year", -1, true, "99")).isEqualTo(1999);
    }

    @Test
    public void shouldConvertTemporalDefaults() {
        assertThat(parse("date", -1, true, "2020-02-02")).isEqualTo(18294);
        assertThat(parse("date", -1, false, "0000-00-00")).isEqualTo(0);
        assertThat(parse("date", -1, true, "0000-00-00")).isNull();
        assertThat(parse("datetime", 0, true, "CURRENT_TIMESTAMP")).isEqualTo(0L);
        assertThat(parse("datetime", 0, true, "2021-01-01 01:01:01")).isEqualTo(1609462861000L);
        assertThat(parse("timestamp", 0, false, "2000-01-01 00:00:00")).isEqualTo("2000-01-01T00:00:00Z");
        assertThat(parse("timestamp(3)", 3, false, "CURRENT_TIMESTAMP(3)")).isEqualTo("1970-01-01T00:00:00Z");
        assertThat(parse("timestamp", 0, true, "CURRENT_TIMESTAMP")).isNull();
        assertThat(parse("time", 0, true, "01:02:03")).isEqualTo(3723000000L);
    }

    @Test
    public void shouldConvertBitAndStringDefaults() {
        assertThat(parse("bit(1)", 1, true, "b'1'")).isEqualTo(true);
        assertThat((byte[]) parse("bit(3)", 3, true, "b'101'")).containsExactly(5);
        assertThat(parse("varchar(5)", -1, true, "x")).isEqualTo("x");
        assertThat(parse("enum('p','q')", -1, true, "q")).isEqualTo("q");
    }
}
