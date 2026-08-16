/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.tidb;

/**
 * A column of a snapshotted TiDB table, as read from {@code information_schema.columns}.
 *
 * @param name the column name
 * @param dataType the base data type in lower case, e.g. {@code int} or {@code varchar}
 * @param columnType the full column type in lower case including modifiers, e.g. {@code int unsigned}
 * @param primaryKey whether the column is part of the primary key
 *
 * @author Aviral Srivastava
 */
public record TiDbColumn(String name, String dataType, String columnType, boolean primaryKey) {

    public boolean isUnsigned() {
        return columnType.contains("unsigned");
    }
}
