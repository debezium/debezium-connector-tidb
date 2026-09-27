/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.tidb;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link TiDbConnection}.
 *
 * @author Aviral Srivastava
 */
public class TiDbConnectionTest {

    @Test
    public void shouldParseEnumAndSetLabels() {
        assertThat(TiDbConnection.parseEnumValues("enum('small','medium','large')"))
                .containsExactly("small", "medium", "large");
        assertThat(TiDbConnection.parseEnumValues("set('a','b')")).containsExactly("a", "b");
    }

    @Test
    public void shouldParseLabelsWithQuotesCommasAndParentheses() {
        assertThat(TiDbConnection.parseEnumValues("enum('it''s','x,y','(z)')"))
                .containsExactly("it's", "x,y", "(z)");
    }

    @Test
    public void shouldReturnNoLabelsForTypesWithoutList() {
        assertThat(TiDbConnection.parseEnumValues("varchar")).isEmpty();
    }
}
