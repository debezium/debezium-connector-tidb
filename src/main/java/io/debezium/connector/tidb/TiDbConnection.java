/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.tidb;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import io.debezium.DebeziumException;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.TableFilter;

/**
 * A JDBC connection to the MySQL compatible SQL endpoint of a TiDB cluster, used for snapshots.
 * <p>
 * Consistency is provided by TiDB's {@code tidb_snapshot} session variable: after it is set to a
 * TSO timestamp, every query in the session reads the data as of that TSO, so all tables are
 * captured at one consistent point without any locking. The TSO must stay newer than the GC safe
 * point of the cluster for the duration of the snapshot.
 * <p>
 * Encryption of the connection follows the driver default (TLS is negotiated when the server
 * supports it); any {@code database.*} property beyond the standard connection settings is passed
 * through to the driver, e.g. {@code database.sslMode=VERIFY_CA}.
 *
 * @author Aviral Srivastava
 */
public class TiDbConnection extends JdbcConnection {

    private static final String URL_PATTERN = "jdbc:mysql://${hostname}:${port}/?connectTimeout=${connectTimeout}"
            + "&zeroDateTimeBehavior=CONVERT_TO_NULL&tinyInt1isBit=false";

    private static final String QUOTE = "`";

    public TiDbConnection(TiDbConnectorConfig connectorConfig) {
        super(jdbcConfig(connectorConfig),
                patternBasedFactory(URL_PATTERN, "com.mysql.cj.jdbc.Driver", TiDbConnection.class.getClassLoader()),
                QUOTE, QUOTE);
    }

    private static JdbcConfiguration jdbcConfig(TiDbConnectorConfig connectorConfig) {
        // The port default lives on the connector's Field definition and does not travel through
        // the raw config subset, so it is applied here for the JDBC url
        return JdbcConfiguration.copy(connectorConfig.getJdbcConfig())
                .withDefault(JdbcConfiguration.PORT, TiDbConnectorConfig.JDBC_PORT.defaultValue())
                .build();
    }

    /**
     * @return the current TSO timestamp of the cluster
     */
    public long currentTso() throws SQLException {
        return queryAndMap("SHOW MASTER STATUS", rs -> {
            if (rs.next()) {
                return rs.getLong("Position");
            }
            throw new DebeziumException("SHOW MASTER STATUS returned no rows, cannot determine the snapshot TSO");
        });
    }

    /**
     * Prepares the session for the snapshot: pins the time zone to UTC so that TIMESTAMP values
     * are returned as UTC wall time, and pins the reads to the given TSO.
     */
    public void initSnapshotSession(long tso) throws SQLException {
        execute("SET time_zone = '+00:00'", "SET SESSION tidb_snapshot = '" + tso + "'");
    }

    /**
     * @return the tables to be snapshotted, resolved from {@code information_schema} and reduced
     *         by the connector's table filter
     */
    public List<TableId> capturedTables(TableFilter tableFilter) throws SQLException {
        final List<TableId> tables = new ArrayList<>();
        query("SELECT table_schema, table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE'"
                + " ORDER BY table_schema, table_name", rs -> {
                    while (rs.next()) {
                        final TableId tableId = new TableId(rs.getString(1), null, rs.getString(2));
                        if (tableFilter.isIncluded(tableId)) {
                            tables.add(tableId);
                        }
                    }
                });
        return tables;
    }

    /**
     * @return the relational model of the given table, read from {@code information_schema}
     */
    public Table readTableStructure(TableId tableId) throws SQLException {
        final TableEditor editor = Table.editor().tableId(tableId);
        final List<String> primaryKeyNames = new ArrayList<>();
        prepareQuery("SELECT column_name, data_type, column_type, column_key, is_nullable,"
                + " numeric_precision, numeric_scale, datetime_precision"
                + " FROM information_schema.columns WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
                statement -> {
                    statement.setString(1, tableId.catalog());
                    statement.setString(2, tableId.table());
                },
                rs -> {
                    int position = 1;
                    while (rs.next()) {
                        final String name = rs.getString(1);
                        final String dataType = rs.getString(2).toLowerCase();
                        final String columnType = rs.getString(3);
                        final ColumnEditor column = Column.editor()
                                .name(name)
                                .type(columnType.toLowerCase())
                                .jdbcType(jdbcTypeFor(dataType))
                                .optional("YES".equalsIgnoreCase(rs.getString(5)))
                                .position(position++);
                        switch (dataType) {
                            case "bit":
                                column.length(rs.getInt(6));
                                break;
                            case "datetime":
                            case "timestamp":
                            case "time":
                                column.length(rs.getInt(8));
                                break;
                            case "float":
                                // numeric_scale is NULL for a plain FLOAT and set for FLOAT(M,D)
                                final int scale = rs.getInt(7);
                                if (!rs.wasNull()) {
                                    column.scale(scale);
                                }
                                break;
                            case "enum":
                            case "set":
                                column.enumValues(parseEnumValues(columnType));
                                break;
                            default:
                                break;
                        }
                        editor.addColumn(column.create());
                        if ("PRI".equalsIgnoreCase(rs.getString(4))) {
                            primaryKeyNames.add(name);
                        }
                    }
                });
        if (editor.columns().isEmpty()) {
            throw new DebeziumException("Table " + tableId + " has no columns in information_schema, it may have been dropped");
        }
        editor.setPrimaryKeyNames(primaryKeyNames);
        return editor.create();
    }

    /**
     * Runs the given snapshot query and hands every row to the consumer as an array of raw JDBC
     * values in column order.
     */
    public void fetchRows(String snapshotQuery, List<Column> columns, RowConsumer consumer) throws SQLException, InterruptedException {
        try {
            query(snapshotQuery, rs -> {
                try {
                    while (rs.next()) {
                        final Object[] row = new Object[columns.size()];
                        for (int i = 0; i < row.length; i++) {
                            row[i] = readColumnValue(rs, i + 1, columns.get(i));
                        }
                        consumer.accept(row);
                    }
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new SQLException(e);
                }
            });
        }
        catch (SQLException e) {
            if (e.getCause() instanceof InterruptedException interrupted) {
                throw interrupted;
            }
            throw e;
        }
    }

    private Object readColumnValue(ResultSet rs, int index, Column column) throws SQLException {
        switch (TiDbJdbcValueConverters.baseType(column)) {
            case "date":
                // Read as LocalDate to avoid time zone shifts of java.sql.Date
                return rs.getObject(index, java.time.LocalDate.class);
            case "datetime":
            case "timestamp":
                return rs.getObject(index, java.time.LocalDateTime.class);
            case "time":
                return rs.getString(index);
            default:
                return rs.getObject(index);
        }
    }

    /**
     * Extracts the labels of an ENUM or SET column type as reported by {@code information_schema},
     * e.g. {@code enum('a','it''s','x,y')} yields {@code a}, {@code it's} and {@code x,y}. Quotes
     * inside a label are doubled by the server.
     */
    static List<String> parseEnumValues(String columnType) {
        final List<String> values = new ArrayList<>();
        final int start = columnType.indexOf('(');
        final int end = columnType.lastIndexOf(')');
        if (start < 0 || end < start) {
            return values;
        }
        final String body = columnType.substring(start + 1, end);
        StringBuilder current = null;
        for (int i = 0; i < body.length(); i++) {
            final char c = body.charAt(i);
            if (current == null) {
                if (c == '\'') {
                    current = new StringBuilder();
                }
            }
            else if (c == '\'') {
                if (i + 1 < body.length() && body.charAt(i + 1) == '\'') {
                    current.append('\'');
                    i++;
                }
                else {
                    values.add(current.toString());
                    current = null;
                }
            }
            else {
                current.append(c);
            }
        }
        return values;
    }

    private static int jdbcTypeFor(String dataType) {
        switch (dataType) {
            case "tinyint":
                return Types.TINYINT;
            case "smallint":
                return Types.SMALLINT;
            case "mediumint":
            case "int":
            case "year":
                return Types.INTEGER;
            case "bigint":
                return Types.BIGINT;
            case "float":
                return Types.REAL;
            case "double":
                return Types.DOUBLE;
            case "decimal":
                return Types.DECIMAL;
            case "char":
            case "enum":
            case "set":
                return Types.CHAR;
            case "varchar":
                return Types.VARCHAR;
            case "tinytext":
            case "text":
            case "mediumtext":
            case "longtext":
            case "json":
                return Types.LONGVARCHAR;
            case "binary":
                return Types.BINARY;
            case "varbinary":
                return Types.VARBINARY;
            case "tinyblob":
            case "blob":
            case "mediumblob":
            case "longblob":
                return Types.BLOB;
            case "date":
                return Types.DATE;
            case "datetime":
            case "timestamp":
                return Types.TIMESTAMP;
            case "time":
                return Types.TIME;
            case "bit":
                return Types.BIT;
            default:
                return Types.OTHER;
        }
    }

    /**
     * Consumer of a single snapshotted row; the values are the raw JDBC values in column order.
     */
    @FunctionalInterface
    public interface RowConsumer {
        void accept(Object[] row) throws InterruptedException;
    }
}
