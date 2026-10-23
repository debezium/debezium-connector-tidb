/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.tidb;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.ConfigDefinition;
import io.debezium.config.Configuration;
import io.debezium.config.ConfigurationNames;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.heartbeat.DatabaseHeartbeatImpl;
import io.debezium.relational.ColumnFilterMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.schema.DefaultTopicNamingStrategy;
import io.debezium.util.Collect;
import io.debezium.util.Strings;

/**
 * Configuration of the TiDB connector.
 * <p>
 * The connector consumes the Debezium-format output of a TiCDC changefeed
 * ({@code protocol=debezium}) from Kafka and layers the Debezium connector framework on top of
 * it: managed offsets, the standard event envelope with a TiDB-specific {@code source} block,
 * table filtering, and initial and on demand snapshots through TiDB's MySQL compatible SQL
 * endpoint.
 *
 * @author Aviral Srivastava
 */
public class TiDbConnectorConfig extends RelationalDatabaseConnectorConfig {

    /**
     * Stream snapshot rows one by one rather than reading whole tables into memory, as the MySQL
     * connector does; a positive {@code snapshot.fetch.size} fetches through a server side cursor.
     */
    protected static final int DEFAULT_SNAPSHOT_FETCH_SIZE = Integer.MIN_VALUE;

    protected static final String TICDC_CONSUMER_PREFIX = "ticdc.consumer.";

    private static final Set<String> BUILT_IN_DB_NAMES = Collect.unmodifiableSet(
            "information_schema", "mysql", "performance_schema", "sys", "metrics_schema", "inspection_schema", "lightning_task_info");

    /**
     * The set of predefined snapshot mode options. Only modes with a matching
     * {@code Snapshotter} SPI implementation in the Debezium framework may be listed here.
     */
    public enum SnapshotMode implements EnumeratedValue {

        /**
         * Take no data snapshot; table structure is learned from the inline schemas of the TiCDC
         * messages and changes are streamed from the TiCDC topics.
         */
        NO_DATA("no_data"),

        /**
         * Snapshot the data of all captured tables through TiDB's MySQL compatible SQL endpoint
         * on first startup, then stream changes from the TiCDC topics.
         */
        INITIAL("initial"),

        /**
         * Snapshot the data of all captured tables and stop, without streaming changes.
         */
        INITIAL_ONLY("initial_only");

        private final String value;

        SnapshotMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        public static SnapshotMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (SnapshotMode option : SnapshotMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }
    }

    /**
     * Where to start reading the TiCDC topics when no offsets have been stored yet.
     */
    public enum InitialOffset implements EnumeratedValue {

        EARLIEST("earliest"),

        LATEST("latest");

        private final String value;

        InitialOffset(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        public static InitialOffset parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim().toLowerCase(Locale.ROOT);
            for (InitialOffset option : InitialOffset.values()) {
                if (option.getValue().equals(value)) {
                    return option;
                }
            }
            return null;
        }
    }

    public static final Field TICDC_BOOTSTRAP_SERVERS = Field.create("ticdc.kafka.bootstrap.servers")
            .withDisplayName("TiCDC Kafka bootstrap servers")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 1))
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .required()
            .withDescription("A list of host/port pairs used to establish the connection to the Kafka cluster "
                    + "that the TiCDC changefeed (with protocol=debezium) writes to.");

    public static final Field TICDC_TOPICS = Field.create("ticdc.topics")
            .withDisplayName("TiCDC topics")
            .withType(Type.LIST)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 2))
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .required()
            .withDescription("A comma-separated list of Kafka topics written by the TiCDC changefeed "
                    + "that this connector consumes change events from.");

    public static final Field TICDC_POLL_TIMEOUT_MS = Field.create("ticdc.poll.timeout.ms")
            .withDisplayName("TiCDC poll timeout (ms)")
            .withType(Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 1))
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(500)
            .withValidation(Field::isPositiveInteger)
            .withDescription("The maximum time, in milliseconds, to block while polling the TiCDC topics for new change events.");

    public static final Field TICDC_INITIAL_OFFSET = Field.create("ticdc.initial.offset")
            .withDisplayName("TiCDC initial offset")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 2))
            .withEnum(InitialOffset.class, InitialOffset.EARLIEST)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withValidation(TiDbConnectorConfig::validateInitialOffset)
            .withDescription("The position in the TiCDC topics to start reading from when no offsets have been stored yet: "
                    + "'earliest' to read the topics from the beginning, 'latest' to read only changes arriving after the connector start.");

    public static final Field SNAPSHOT_MODE = Field.create(SNAPSHOT_MODE_PROPERTY_NAME)
            .withDisplayName("Snapshot mode")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 0))
            .withEnum(SnapshotMode.class, SnapshotMode.NO_DATA)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDescription("The criteria for running a snapshot upon startup of the connector. Select one of the following snapshot options: "
                    + "'no_data': The connector takes no data snapshot, learns the table structure from the TiCDC messages "
                    + "and immediately streams changes from the TiCDC topics; "
                    + "'initial': On first startup the connector snapshots the data of all captured tables through TiDB's SQL endpoint, "
                    + "then streams changes from the TiCDC topics; "
                    + "'initial_only': The connector snapshots the data of all captured tables and stops without streaming changes.");

    public static final Field JDBC_HOSTNAME = Field.create(ConfigurationNames.DATABASE_CONFIG_PREFIX + "hostname")
            .withDisplayName("TiDB hostname")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 3))
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withValidation(TiDbConnectorConfig::validateSqlEndpoint)
            .withDescription("Resolvable hostname or IP address of the TiDB SQL endpoint. "
                    + "Only required when the snapshot mode captures data.");

    public static final Field JDBC_PORT = Field.create(ConfigurationNames.DATABASE_CONFIG_PREFIX + "port")
            .withDisplayName("TiDB port")
            .withType(Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 4))
            .withDefault(4000)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withValidation(Field::isPositiveInteger)
            .withDescription("Port of the TiDB SQL endpoint.");

    public static final Field JDBC_USER = Field.create(ConfigurationNames.DATABASE_CONFIG_PREFIX + "user")
            .withDisplayName("TiDB user")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 5))
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("Name of the database user connecting to the TiDB SQL endpoint. "
                    + "Only required when the snapshot mode captures data.");

    public static final Field JDBC_PASSWORD = Field.create(ConfigurationNames.DATABASE_CONFIG_PREFIX + "password")
            .withDisplayName("TiDB password")
            .withType(Type.PASSWORD)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 6))
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("Password of the database user connecting to the TiDB SQL endpoint.");

    public static final Field SOURCE_INFO_STRUCT_MAKER = CommonConnectorConfig.SOURCE_INFO_STRUCT_MAKER
            .withDefault(TiDbSourceInfoStructMaker.class.getName());

    public static final Field TOPIC_NAMING_STRATEGY = CommonConnectorConfig.TOPIC_NAMING_STRATEGY
            .withDefault(DefaultTopicNamingStrategy.class.getName());

    // Options of the relational framework whose behavior is fixed by the TiCDC encoding, or which
    // the connector does not support. They are rejected rather than silently ignored.

    public static final Field DECIMAL_HANDLING_MODE = RelationalDatabaseConnectorConfig.DECIMAL_HANDLING_MODE
            .withValidation((config, field, problems) -> requireValue(config, field, problems, "double",
                    "TiCDC encodes DECIMAL values as double"));

    public static final Field TIME_PRECISION_MODE = RelationalDatabaseConnectorConfig.TIME_PRECISION_MODE
            .withValidation((config, field, problems) -> requireValue(config, field, problems, "adaptive_time_microseconds",
                    "TiCDC encodes temporal values with adaptive precision and TIME values in microseconds"));

    public static final Field BINARY_HANDLING_MODE = CommonConnectorConfig.BINARY_HANDLING_MODE
            .withValidation((config, field, problems) -> requireValue(config, field, problems, "base64",
                    "TiCDC encodes binary values as base64 strings"));

    public static final Field CUSTOM_CONVERTERS = CommonConnectorConfig.CUSTOM_CONVERTERS
            .withValidation((config, field, problems) -> rejectValue(config, field, problems,
                    "custom converters are not supported by the TiDB connector"));

    public static final Field INCLUDE_SCHEMA_COMMENTS = RelationalDatabaseConnectorConfig.INCLUDE_SCHEMA_COMMENTS
            .withValidation((config, field, problems) -> config.getBoolean(field)
                    ? reject(field, config.getString(field), problems, "column comments are not available in the TiCDC stream")
                    : 0);

    public static final Field HEARTBEAT_ACTION_QUERY = DatabaseHeartbeatImpl.HEARTBEAT_ACTION_QUERY
            .withValidation((config, field, problems) -> rejectValue(config, field, problems,
                    "heartbeat action queries are not supported by the TiDB connector"));

    private static final ConfigDefinition CONFIG_DEFINITION = RelationalDatabaseConnectorConfig.CONFIG_DEFINITION.edit()
            .name("TiDB")
            // The base relational connection fields are required fields; the connector replaces
            // them with optional variants because the SQL endpoint is only needed when the
            // snapshot mode captures data
            .excluding(HOSTNAME, PORT, USER, PASSWORD, DATABASE_NAME)
            .group(Field.Group.CONNECTION, TICDC_BOOTSTRAP_SERVERS, TICDC_TOPICS, JDBC_HOSTNAME, JDBC_PORT, JDBC_USER, JDBC_PASSWORD)
            .group(Field.Group.CONNECTION_ADVANCED, TICDC_POLL_TIMEOUT_MS, TICDC_INITIAL_OFFSET)
            .group(Field.Group.CONNECTOR_SNAPSHOT, SNAPSHOT_MODE)
            // Replaces the inherited definitions in place, adding the validators
            .group(Field.Group.CONNECTOR, DECIMAL_HANDLING_MODE, TIME_PRECISION_MODE, INCLUDE_SCHEMA_COMMENTS, BINARY_HANDLING_MODE)
            .group(Field.Group.CONNECTOR_ADVANCED, SOURCE_INFO_STRUCT_MAKER, TOPIC_NAMING_STRATEGY, CUSTOM_CONVERTERS)
            .group(Field.Group.ADVANCED_HEARTBEAT, HEARTBEAT_ACTION_QUERY)
            .create();

    /**
     * Validates that the SQL endpoint connection is configured when the selected snapshot mode
     * needs to read data from TiDB, or when column mappers need the source column metadata that
     * only the SQL endpoint provides.
     */
    private static int validateSqlEndpoint(Configuration config, Field field, Field.ValidationOutput problems) {
        final String modeValue = config.getString(SNAPSHOT_MODE);
        final SnapshotMode mode = SnapshotMode.parse(modeValue);
        if (mode == null) {
            // getString applies the field default when the key is absent, so a failed parse
            // means an explicitly supplied empty or unknown value
            problems.accept(SNAPSHOT_MODE, modeValue, "'" + SNAPSHOT_MODE_PROPERTY_NAME + "' must be one of "
                    + allowedValues(SnapshotMode.values()));
            return 1;
        }
        final String reason;
        if (mode != SnapshotMode.NO_DATA) {
            reason = "'" + SNAPSHOT_MODE_PROPERTY_NAME + "' is '" + mode.getValue() + "'";
        }
        else if (requiresSourceColumnMetadata(config)) {
            reason = "column masking, truncation or source type propagation is configured";
        }
        else {
            return 0;
        }
        int problemCount = 0;
        if (Strings.isNullOrEmpty(config.getString(JDBC_HOSTNAME))) {
            problems.accept(JDBC_HOSTNAME, null, "'" + JDBC_HOSTNAME.name() + "' is required when " + reason);
            problemCount++;
        }
        if (Strings.isNullOrEmpty(config.getString(JDBC_USER))) {
            problems.accept(JDBC_USER, null, "'" + JDBC_USER.name() + "' is required when " + reason);
            problemCount++;
        }
        return problemCount;
    }

    /**
     * @return {@code true} if column mappers are configured that depend on the source column
     *         metadata (type, length), which only the SQL endpoint provides
     */
    private static boolean requiresSourceColumnMetadata(Configuration config) {
        return config.keys().stream().anyMatch(key -> key.startsWith("column.mask.") || key.startsWith("column.truncate."))
                || !Strings.isNullOrBlank(config.getString(PROPAGATE_COLUMN_SOURCE_TYPE))
                || !Strings.isNullOrBlank(config.getString(PROPAGATE_DATATYPE_SOURCE_TYPE));
    }

    private static int requireValue(Configuration config, Field field, Field.ValidationOutput problems, String supported, String reason) {
        final String value = config.getString(field.name());
        if (value != null && !supported.equalsIgnoreCase(value.trim())) {
            return reject(field, value, problems, "only '" + supported + "' is supported because " + reason);
        }
        return 0;
    }

    private static int rejectValue(Configuration config, Field field, Field.ValidationOutput problems, String reason) {
        final String value = config.getString(field.name());
        if (!Strings.isNullOrBlank(value)) {
            return reject(field, value, problems, reason);
        }
        return 0;
    }

    private static int reject(Field field, String value, Field.ValidationOutput problems, String reason) {
        problems.accept(field, value, "'" + field.name() + "' is not supported: " + reason);
        return 1;
    }

    /**
     * Validates that an explicitly supplied initial offset is one of the known values.
     */
    private static int validateInitialOffset(Configuration config, Field field, Field.ValidationOutput problems) {
        final String value = config.getString(field);
        if (InitialOffset.parse(value) == null) {
            problems.accept(field, value, "'" + field.name() + "' must be one of " + allowedValues(InitialOffset.values()));
            return 1;
        }
        return 0;
    }

    private static String allowedValues(EnumeratedValue[] values) {
        return Arrays.stream(values)
                .map(value -> "'" + value.getValue() + "'")
                .collect(Collectors.joining(", "));
    }

    /**
     * The set of {@link Field}s defined as part of this configuration.
     */
    public static Field.Set ALL_FIELDS = Field.setOf(CONFIG_DEFINITION.all());

    public static ConfigDef configDef() {
        return CONFIG_DEFINITION.configDef();
    }

    private final SnapshotMode snapshotMode;
    private final InitialOffset initialOffset;
    private final String ticdcBootstrapServers;
    private final List<String> ticdcTopics;
    private final Duration ticdcPollTimeout;
    private final Properties ticdcConsumerProperties;
    private final boolean sqlEndpointConfigured;
    private final boolean requiresSourceColumnMetadata;

    public TiDbConnectorConfig(Configuration config) {
        super(
                config,
                TableFilter.fromPredicate(TiDbConnectorConfig::isNotBuiltInTable),
                TableId::toString,
                DEFAULT_SNAPSHOT_FETCH_SIZE,
                ColumnFilterMode.CATALOG,
                true);

        this.snapshotMode = SnapshotMode.parse(config.getString(SNAPSHOT_MODE));
        this.initialOffset = InitialOffset.parse(config.getString(TICDC_INITIAL_OFFSET));
        this.ticdcBootstrapServers = config.getString(TICDC_BOOTSTRAP_SERVERS);
        this.ticdcTopics = Strings.listOfTrimmed(config.getString(TICDC_TOPICS), topic -> topic.isEmpty() ? null : topic);
        this.ticdcPollTimeout = Duration.ofMillis(config.getInteger(TICDC_POLL_TIMEOUT_MS));
        this.ticdcConsumerProperties = config.subset(TICDC_CONSUMER_PREFIX, true).asProperties();
        if (!Strings.isNullOrEmpty(ticdcBootstrapServers)) {
            this.ticdcConsumerProperties.put("bootstrap.servers", ticdcBootstrapServers);
        }
        this.sqlEndpointConfigured = !Strings.isNullOrEmpty(config.getString(JDBC_HOSTNAME));
        this.requiresSourceColumnMetadata = requiresSourceColumnMetadata(config);
    }

    public static boolean isBuiltInDatabase(String databaseName) {
        if (Strings.isNullOrEmpty(databaseName)) {
            return false;
        }
        return BUILT_IN_DB_NAMES.contains(databaseName.toLowerCase(Locale.ROOT));
    }

    public static boolean isNotBuiltInTable(TableId tableId) {
        return !isBuiltInDatabase(tableId.catalog());
    }

    public String getTicdcBootstrapServers() {
        return ticdcBootstrapServers;
    }

    public List<String> getTicdcTopics() {
        return ticdcTopics;
    }

    public Duration getTicdcPollTimeout() {
        return ticdcPollTimeout;
    }

    public InitialOffset getTicdcInitialOffset() {
        return initialOffset;
    }

    /**
     * @return {@code true} if the TiDB SQL endpoint is configured
     */
    public boolean isSqlEndpointConfigured() {
        return sqlEndpointConfigured;
    }

    /**
     * @return {@code true} if column mappers are configured that depend on the source column
     *         metadata, which only the SQL endpoint provides
     */
    public boolean requiresSourceColumnMetadata() {
        return requiresSourceColumnMetadata;
    }

    /**
     * @return the properties for the Kafka consumer reading the TiCDC topics; any property
     *         prefixed with {@code ticdc.consumer.} is passed through verbatim
     */
    public Properties getTicdcConsumerProperties() {
        final Properties props = new Properties();
        props.putAll(ticdcConsumerProperties);
        return props;
    }

    @Override
    public String getContextName() {
        return Module.contextName();
    }

    @Override
    public String getConnectorName() {
        return Module.name();
    }

    @Override
    public SnapshotMode getSnapshotMode() {
        return snapshotMode;
    }

    @Override
    public Optional<? extends EnumeratedValue> getSnapshotLockingMode() {
        return Optional.empty();
    }

    @Override
    protected SourceInfoStructMaker<? extends AbstractSourceInfo> getSourceInfoStructMaker(Version version) {
        return getSourceInfoStructMaker(SOURCE_INFO_STRUCT_MAKER, Module.name(), Module.version(), this);
    }
}
