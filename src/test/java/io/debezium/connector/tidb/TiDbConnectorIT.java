/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.tidb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.data.Envelope;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;

/**
 * Integration tests running the connector against a TiDB cluster with a TiCDC changefeed
 * writing to Kafka.
 * <p>
 * The central check is that a row reads the same in a snapshot event and in a streamed event:
 * the same key and value schemas and the same values, for every data type, including the values
 * that only a real driver and a real TiCDC produce (YEAR, zero dates, dates outside the range of
 * nanosecond arithmetic, BIT, binary strings, defaults).
 *
 * @author Aviral Srivastava
 */
public class TiDbConnectorIT extends AbstractAsyncEngineConnectorTest {

    private static final AtomicInteger SEQUENCE = new AtomicInteger();

    private TiDbTestCluster cluster;
    private String database;
    private String topic;
    private String changefeed;

    @BeforeEach
    public void beforeEach() throws Exception {
        cluster = TiDbTestCluster.get();
        final int id = SEQUENCE.incrementAndGet();
        database = "it_db_" + id + "_" + System.currentTimeMillis() % 100_000;
        topic = "ticdc_" + database;
        changefeed = "cf-" + database.replace('_', '-');
        cluster.execute("CREATE DATABASE " + database);
        setConsumeTimeout(30, TimeUnit.SECONDS);
    }

    @AfterEach
    public void afterEach() throws Exception {
        stopConnector();
        if (cluster == null) {
            // The cluster failed to start
            return;
        }
        try {
            cluster.removeChangefeed(changefeed);
        }
        catch (IllegalStateException e) {
            // The test did not create a changefeed
        }
        cluster.execute("DROP DATABASE IF EXISTS " + database);
    }

    private static final String TYPES_TABLE = "CREATE TABLE %s.types ("
            + " id INT PRIMARY KEY,"
            + " c_tinyint TINYINT, c_tinyint_u TINYINT UNSIGNED, c_smallint SMALLINT, c_smallint_u SMALLINT UNSIGNED,"
            + " c_mediumint MEDIUMINT, c_int INT, c_int_u INT UNSIGNED, c_bigint BIGINT, c_bigint_u BIGINT UNSIGNED,"
            + " c_float FLOAT, c_float_md FLOAT(7,3), c_double DOUBLE, c_decimal DECIMAL(10,3),"
            + " c_bit1 BIT(1), c_bit10 BIT(10), c_bool BOOLEAN,"
            + " c_date DATE, c_datetime DATETIME, c_datetime3 DATETIME(3), c_datetime6 DATETIME(6),"
            + " c_timestamp TIMESTAMP NULL, c_timestamp6 TIMESTAMP(6) NULL, c_time TIME, c_time6 TIME(6), c_year YEAR,"
            + " c_char CHAR(5), c_varchar VARCHAR(20), c_text TEXT, c_binary BINARY(4), c_varbinary VARBINARY(10), c_blob BLOB,"
            + " c_enum ENUM('a','b,c','it''s'), c_set SET('x','y','z'), c_json JSON,"
            + " c_virtual INT AS (c_int * 2) VIRTUAL, c_stored INT AS (c_int * 3) STORED,"
            + " c_date_nn DATE NOT NULL, c_datetime_nn DATETIME NOT NULL,"
            + " c_timestamp_nn TIMESTAMP NOT NULL DEFAULT '2000-01-01 00:00:00')";

    private static final String DEFAULTS_TABLE = "CREATE TABLE %s.defaults ("
            + " id INT PRIMARY KEY, s VARCHAR(5) DEFAULT 'x', i INT DEFAULT 7, d DECIMAL(5,2) DEFAULT 1.50,"
            + " b BIT(3) DEFAULT b'101', e ENUM('p','q') DEFAULT 'q', dt DATETIME DEFAULT CURRENT_TIMESTAMP,"
            + " ts TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3), y YEAR DEFAULT 2001, t TIME DEFAULT '01:02:03',"
            + " bo TINYINT(1) DEFAULT 1, f FLOAT DEFAULT 2.5, dd DATE DEFAULT '2020-02-02')";

    private void createTablesAndRows() throws Exception {
        cluster.execute(
                String.format(TYPES_TABLE, database),
                String.format(DEFAULTS_TABLE, database),
                // A table without primary key whose unique NOT NULL index is TiCDC's handle key
                "CREATE TABLE " + database + ".uk (a INT NOT NULL, b VARCHAR(10), UNIQUE KEY u1 (a))",
                // The primary key order differs from the column order
                "CREATE TABLE " + database + ".ck (a INT, b INT, v VARCHAR(5), PRIMARY KEY (b, a))");
        cluster.execute(
                "SET SESSION sql_mode = ''",
                "INSERT INTO " + database + ".types (id, c_tinyint, c_tinyint_u, c_smallint, c_smallint_u, c_mediumint, c_int, c_int_u,"
                        + " c_bigint, c_bigint_u, c_float, c_float_md, c_double, c_decimal, c_bit1, c_bit10, c_bool,"
                        + " c_date, c_datetime, c_datetime3, c_datetime6, c_timestamp, c_timestamp6, c_time, c_time6, c_year,"
                        + " c_char, c_varchar, c_text, c_binary, c_varbinary, c_blob, c_enum, c_set, c_json,"
                        + " c_date_nn, c_datetime_nn, c_timestamp_nn) VALUES"
                        + " (1, -128, 255, -32768, 65535, -8388608, -2147483648, 4294967295, -9223372036854775808, 18446744073709551615,"
                        + " 1.5, 1234.567, 2.25, 1234567.891, b'1', b'1000000001', true,"
                        + " '9999-12-31', '9999-12-31 23:59:59', '1000-01-01 00:00:00.123', '2024-02-29 12:34:56.654321',"
                        + " '2038-01-19 03:14:07', '2024-02-29 12:34:56.000001', '-838:59:59', '12:34:56.500000', 2024,"
                        + " 'ab', 'héllo', 'text', 0x01020304, 0xDEADBEEF, 0x00FF, 'b,c', 'x,z', '{\"a\": [1, 2]}',"
                        + " '0000-00-00', '0000-00-00 00:00:00', '0000-00-00 00:00:00')",
                "INSERT INTO " + database + ".types (id, c_year, c_date_nn, c_datetime_nn) VALUES (2, 1999, '2020-01-01', '2020-01-01 00:00:00')",
                "INSERT INTO " + database + ".defaults (id, s, i, d, b, e, dt, ts, y, t, bo, f, dd)"
                        + " VALUES (1, 'v', 1, 2.25, b'010', 'p', '2021-01-01 01:01:01', '2022-02-02 02:02:02.123', 1999, '-01:00:00', 0, 0.5,"
                        + " '1999-09-09')",
                "INSERT INTO " + database + ".uk VALUES (1, 'x')",
                "INSERT INTO " + database + ".ck VALUES (1, 2, 'v')");
    }

    @Test
    public void shouldEmitIdenticalRecordsInSnapshotAndStreamingForAllTypes() throws Exception {
        cluster.createChangefeed(changefeed, database, topic);
        createTablesAndRows();

        // The snapshot reads the rows through the SQL endpoint
        start(TiDbConnector.class, cluster.connectorConfig(database, topic)
                .with(TiDbConnectorConfig.SNAPSHOT_MODE, "initial_only")
                .build());
        final Map<String, SourceRecord> snapshot = byTableAndKey(consumeRecordsByTopic(5).allRecordsInOrder());
        stopConnector();
        assertThat(snapshot).hasSize(5);
        snapshot.values().forEach(record -> assertThat(operation(record)).isEqualTo(Envelope.Operation.READ));

        // Streaming reads the same rows from the TiCDC changefeed
        cleanupOffsets();
        start(TiDbConnector.class, cluster.connectorConfig(database, topic)
                .with(TiDbConnectorConfig.SNAPSHOT_MODE, "no_data")
                .build());
        final Map<String, SourceRecord> streamed = byTableAndKey(consumeRecordsByTopic(5).allRecordsInOrder());
        assertThat(streamed).hasSize(5);
        streamed.values().forEach(record -> assertThat(operation(record)).isEqualTo(Envelope.Operation.CREATE));

        assertThat(streamed.keySet()).isEqualTo(snapshot.keySet());
        for (Map.Entry<String, SourceRecord> entry : snapshot.entrySet()) {
            final SourceRecord fromSnapshot = entry.getValue();
            final SourceRecord fromStream = streamed.get(entry.getKey());
            validate(fromSnapshot);
            validate(fromStream);

            assertThat(fromStream.topic()).isEqualTo(fromSnapshot.topic());
            assertThat(fromStream.keySchema()).as("key schema of %s", entry.getKey()).isEqualTo(fromSnapshot.keySchema());
            assertThat(fromStream.key()).as("key of %s", entry.getKey()).isEqualTo(fromSnapshot.key());

            final Struct snapshotAfter = after(fromSnapshot);
            final Struct streamedAfter = after(fromStream);
            for (Field field : snapshotAfter.schema().fields()) {
                final Field streamedField = streamedAfter.schema().field(field.name());
                assertThat(streamedField).as("field %s of %s", field.name(), entry.getKey()).isNotNull();
                assertSameFieldSchema(entry.getKey() + "." + field.name(), field.schema(), streamedField.schema());
                assertThat(streamedAfter.get(streamedField)).as("value of %s.%s", entry.getKey(), field.name())
                        .isEqualTo(snapshotAfter.get(field));
            }
            assertThat(streamedAfter.schema().fields()).extracting(Field::name).as("fields of %s", entry.getKey())
                    .isEqualTo(snapshotAfter.schema().fields().stream().map(Field::name).toList());
            assertThat(streamedAfter.schema()).as("value schema of %s", entry.getKey()).isEqualTo(snapshotAfter.schema());
        }

        // Spot checks of values that only the real driver produces
        final Struct types = after(snapshot.get("types:Struct{id=1}"));
        assertThat(types.get("c_year")).isEqualTo(2024);
        assertThat(types.get("c_date")).isEqualTo(2932896);
        assertThat(types.get("c_datetime")).isEqualTo(253402300799000L);
        assertThat(types.get("c_datetime3")).isEqualTo(-30610223999877L);
        assertThat(types.get("c_date_nn")).isEqualTo(0);
        assertThat(types.get("c_datetime_nn")).isEqualTo(0L);
        assertThat(types.get("c_timestamp_nn")).isEqualTo("1970-01-01T00:00:00Z");
        // Like TiCDC, virtual generated columns come last and are always null
        assertThat(types.schema().fields().get(types.schema().fields().size() - 1).name()).isEqualTo("c_virtual");
        assertThat(types.get("c_virtual")).isNull();
        assertThat(types.schema().field("c_stored")).isNotNull();
        assertThat(after(snapshot.get("types:Struct{id=2}")).get("c_year")).isEqualTo(1999);
        assertThat(snapshot).containsKey("uk:Struct{a=1}");
        assertThat(snapshot).containsKey("ck:Struct{a=1,b=2}");
    }

    @Test
    public void shouldNotRepeatCompletedInitialOnlySnapshotAfterRestart() throws Exception {
        cluster.execute("CREATE TABLE " + database + ".t (id INT PRIMARY KEY, v VARCHAR(10))",
                "INSERT INTO " + database + ".t VALUES (1, 'a'), (2, 'b')");
        final Configuration config = cluster.connectorConfig(database, topic)
                .with(TiDbConnectorConfig.SNAPSHOT_MODE, "initial_only")
                .build();

        start(TiDbConnector.class, config);
        assertThat(consumeRecordsByTopic(2).allRecordsInOrder()).hasSize(2);
        stopConnector();

        // The completed snapshot is in the offsets, so the restarted connector reads nothing
        start(TiDbConnector.class, config);
        assertThat(waitForAvailableRecords(10, TimeUnit.SECONDS)).isFalse();
    }

    @Test
    public void shouldHandOffFromSnapshotToStreamingWithOneSchema() throws Exception {
        cluster.execute("CREATE TABLE " + database + ".t (id INT PRIMARY KEY, v VARCHAR(10), secret VARCHAR(20), note VARCHAR(10))",
                "INSERT INTO " + database + ".t VALUES (1, 'a', 'hidden-1', 'n1')");
        cluster.createChangefeed(changefeed, database, topic);

        start(TiDbConnector.class, cluster.connectorConfig(database, topic)
                .with(TiDbConnectorConfig.SNAPSHOT_MODE, "initial")
                .with("column.mask.with.3.chars", database + "\\.t\\.secret")
                .with("column.exclude.list", database + "\\.t\\.note")
                .with("message.key.columns", database + ".t:id,v")
                .build());
        final SourceRecord snapshotRecord = consumeRecordsByTopic(1).allRecordsInOrder().get(0);

        cluster.execute("INSERT INTO " + database + ".t VALUES (2, 'b', 'hidden-2', 'n2')");
        final SourceRecord streamedRecord = consumeRecordsByTopic(1).allRecordsInOrder().get(0);

        assertThat(operation(snapshotRecord)).isEqualTo(Envelope.Operation.READ);
        assertThat(operation(streamedRecord)).isEqualTo(Envelope.Operation.CREATE);
        assertThat(streamedRecord.keySchema()).isEqualTo(snapshotRecord.keySchema());
        assertThat(streamedRecord.valueSchema()).isEqualTo(snapshotRecord.valueSchema());
        assertThat(((Struct) streamedRecord.key()).schema().fields()).extracting(Field::name).containsExactly("id", "v");
        for (SourceRecord record : List.of(snapshotRecord, streamedRecord)) {
            assertThat(after(record).getString("secret")).isEqualTo("***");
            assertThat(after(record).schema().field("note")).isNull();
        }
    }

    @Test
    public void shouldRunBlockingSnapshotWithAdditionalConditionsAndResumeStreaming() throws Exception {
        cluster.execute("CREATE TABLE " + database + ".t (id INT PRIMARY KEY, color VARCHAR(10))",
                "CREATE TABLE " + database + ".debezium_signal (id VARCHAR(42) PRIMARY KEY, type VARCHAR(32) NOT NULL, data VARCHAR(2048))",
                "INSERT INTO " + database + ".t VALUES (1, 'red'), (2, 'blue'), (3, 'blue')");
        cluster.createChangefeed(changefeed, database, topic);

        start(TiDbConnector.class, cluster.connectorConfig(database, topic)
                .with(TiDbConnectorConfig.SNAPSHOT_MODE, "initial")
                .with(CommonConnectorConfig.SIGNAL_DATA_COLLECTION, database + ".debezium_signal")
                .build());
        assertThat(recordsForTable(consumeRecordsByTopic(3).allRecordsInOrder(), "t")).hasSize(3);

        cluster.execute("INSERT INTO " + database + ".debezium_signal VALUES ('ad-hoc-1', 'execute-snapshot', "
                + "'{\"type\": \"blocking\", \"data-collections\": [\"" + database + ".t\"], \"additional-conditions\": ["
                + "{\"data-collection\": \"" + database + ".t\", \"filter\": \"SELECT `id`, `color` FROM `" + database
                + "`.`t` WHERE `color` = ''blue''\"}]}')");

        // The signal row itself is streamed, followed by the two rows of the blocking snapshot
        final List<SourceRecord> blocking = recordsForTable(consumeRecordsByTopic(3).allRecordsInOrder(), "t");
        assertThat(blocking).hasSize(2);
        blocking.forEach(record -> {
            assertThat(operation(record)).isEqualTo(Envelope.Operation.READ);
            assertThat(after(record).getString("color")).isEqualTo("blue");
        });

        // Streaming resumes after the blocking snapshot
        cluster.execute("INSERT INTO " + database + ".t VALUES (4, 'green')");
        final List<SourceRecord> streamed = recordsForTable(consumeRecordsByTopic(1).allRecordsInOrder(), "t");
        assertThat(streamed).hasSize(1);
        assertThat(operation(streamed.get(0))).isEqualTo(Envelope.Operation.CREATE);
        assertThat(after(streamed.get(0)).getString("color")).isEqualTo("green");
    }

    @Test
    public void shouldIgnoreSnapshotSelectStatementOverrides() throws Exception {
        cluster.execute("CREATE TABLE " + database + ".t (id INT PRIMARY KEY, v VARCHAR(10))",
                "INSERT INTO " + database + ".t VALUES (1, 'a'), (2, 'b'), (3, 'c')");

        // Overrides are not supported: the snapshot logs a warning and reads all rows
        start(TiDbConnector.class, cluster.connectorConfig(database, topic)
                .with(TiDbConnectorConfig.SNAPSHOT_MODE, "initial_only")
                .with("snapshot.select.statement.overrides", database + ".t")
                .with("snapshot.select.statement.overrides." + database + ".t", "SELECT `id`, `v` FROM `" + database + "`.`t` WHERE `id` > 1")
                .build());
        final List<SourceRecord> records = consumeRecordsByTopic(3).allRecordsInOrder();

        assertThat(records).extracting(record -> after(record).getInt32("id")).containsExactly(1, 2, 3);
    }

    @Test
    public void shouldStreamSnapshotRowsOfLargeTables() throws Exception {
        cluster.execute("CREATE TABLE " + database + ".big (id INT PRIMARY KEY, payload VARCHAR(200))");
        final int rows = 20_000;
        for (int start = 0; start < rows; start += 1_000) {
            final StringBuilder insert = new StringBuilder("INSERT INTO " + database + ".big VALUES ");
            for (int id = start; id < start + 1_000; id++) {
                insert.append(id == start ? "" : ",").append("(").append(id).append(", REPEAT('x', 200))");
            }
            cluster.execute(insert.toString());
        }

        // Rows are streamed from the result set, both with the default and with a cursor fetch size
        for (int fetchSize : new int[]{ Integer.MIN_VALUE, 500 }) {
            cleanupOffsets();
            final Configuration.Builder config = cluster.connectorConfig(database, topic)
                    .with(TiDbConnectorConfig.SNAPSHOT_MODE, "initial_only");
            if (fetchSize > 0) {
                config.with(CommonConnectorConfig.SNAPSHOT_FETCH_SIZE, fetchSize);
            }
            start(TiDbConnector.class, config.build());
            assertThat(consumeRecordsByTopic(rows).allRecordsInOrder()).hasSize(rows);
            stopConnector();
        }
    }

    private void cleanupOffsets() {
        io.debezium.util.Testing.Files.delete(OFFSET_STORE_PATH);
    }

    private static List<SourceRecord> recordsForTable(List<SourceRecord> records, String table) {
        final List<SourceRecord> result = new ArrayList<>();
        for (SourceRecord record : records) {
            if (record.value() != null && table.equals(((Struct) record.value()).getStruct(Envelope.FieldName.SOURCE).getString("table"))) {
                result.add(record);
            }
        }
        return result;
    }

    private static Map<String, SourceRecord> byTableAndKey(List<SourceRecord> records) {
        final Map<String, SourceRecord> result = new HashMap<>();
        for (SourceRecord record : records) {
            final String table = ((Struct) record.value()).getStruct(Envelope.FieldName.SOURCE).getString("table");
            result.put(table + ":" + record.key(), record);
        }
        return result;
    }

    private static Envelope.Operation operation(SourceRecord record) {
        return Envelope.Operation.forCode(((Struct) record.value()).getString(Envelope.FieldName.OPERATION));
    }

    private static Struct after(SourceRecord record) {
        return ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER);
    }

    private static void assertSameFieldSchema(String field, Schema expected, Schema actual) {
        assertThat(actual.type()).as("type of %s", field).isEqualTo(expected.type());
        assertThat(actual.name()).as("logical type of %s", field).isEqualTo(expected.name());
        assertThat(actual.isOptional()).as("optionality of %s", field).isEqualTo(expected.isOptional());
        assertThat(actual.parameters()).as("parameters of %s", field).isEqualTo(expected.parameters());
        assertThat(actual.defaultValue()).as("default of %s", field).isEqualTo(expected.defaultValue());
        assertThat(actual.version()).as("version of %s", field).isEqualTo(expected.version());
        assertThat(actual.doc()).as("doc of %s", field).isEqualTo(expected.doc());
        assertThat(actual).as("schema of %s", field).isEqualTo(expected);
    }
}
