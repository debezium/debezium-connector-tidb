/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.tidb;

import java.util.Map;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.DebeziumException;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;
import io.debezium.relational.ValueConverterProvider;

/**
 * {@link ValueConverterProvider} for rows streamed from TiCDC.
 * <p>
 * TiCDC has already encoded the values, so each field schema is a copy of the field schema in the
 * TiCDC message and the values pass through unchanged. Running them through
 * {@code TableSchemaBuilder} still applies the column filters and column mappers, the key columns
 * and the field naming of the connector, exactly as for snapshot rows.
 *
 * @author Aviral Srivastava
 */
public class TiCdcValueConverters implements ValueConverterProvider {

    private final Schema rowSchema;

    public TiCdcValueConverters(Schema rowSchema) {
        this.rowSchema = rowSchema;
    }

    @Override
    public SchemaBuilder schemaBuilder(Column column) {
        final Field field = rowSchema.field(column.name());
        if (field == null) {
            throw new DebeziumException("Column " + column.name() + " is not part of the TiCDC row schema");
        }
        return copyOf(field.schema());
    }

    @Override
    public ValueConverter converter(Column column, Field fieldDefn) {
        return ValueConverter.passthrough();
    }

    static SchemaBuilder copyOf(Schema schema) {
        final SchemaBuilder builder = new SchemaBuilder(schema.type())
                .name(schema.name())
                .version(schema.version())
                .doc(schema.doc());
        final Map<String, String> parameters = schema.parameters();
        if (parameters != null) {
            builder.parameters(parameters);
        }
        if (schema.isOptional()) {
            builder.optional();
        }
        if (schema.defaultValue() != null) {
            builder.defaultValue(schema.defaultValue());
        }
        return builder;
    }
}
