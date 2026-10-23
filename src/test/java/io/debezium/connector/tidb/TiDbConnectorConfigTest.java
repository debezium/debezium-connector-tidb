/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.tidb;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;

/**
 * Unit tests for {@link TiDbConnectorConfig}.
 *
 * @author Aviral Srivastava
 */
public class TiDbConnectorConfigTest {

    private static Configuration.Builder minimalConfig() {
        return Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "tidb_server")
                .with(TiDbConnectorConfig.TICDC_BOOTSTRAP_SERVERS, "localhost:9092")
                .with(TiDbConnectorConfig.TICDC_TOPICS, "ticdc-inventory");
    }

    @Test
    public void shouldValidateMinimalConfiguration() {
        final Configuration config = minimalConfig().build();
        assertThat(config.validateAndRecord(TiDbConnectorConfig.ALL_FIELDS, error -> {
        })).isTrue();
    }

    @Test
    public void shouldNotValidateWithoutTicdcBootstrapServers() {
        final Configuration config = Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "tidb_server")
                .with(TiDbConnectorConfig.TICDC_TOPICS, "ticdc-inventory")
                .build();
        assertThat(config.validateAndRecord(TiDbConnectorConfig.ALL_FIELDS, error -> {
        })).isFalse();
    }

    @Test
    public void shouldNotValidateWithoutTicdcTopics() {
        final Configuration config = Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "tidb_server")
                .with(TiDbConnectorConfig.TICDC_BOOTSTRAP_SERVERS, "localhost:9092")
                .build();
        assertThat(config.validateAndRecord(TiDbConnectorConfig.ALL_FIELDS, error -> {
        })).isFalse();
    }

    @Test
    public void shouldParseTopicList() {
        final TiDbConnectorConfig config = new TiDbConnectorConfig(
                minimalConfig().with(TiDbConnectorConfig.TICDC_TOPICS, "topic-a, topic-b ,topic-c").build());
        assertThat(config.getTicdcTopics()).containsExactly("topic-a", "topic-b", "topic-c");
    }

    @Test
    public void shouldDefaultToNoDataSnapshotModeAndEarliestOffset() {
        final TiDbConnectorConfig config = new TiDbConnectorConfig(minimalConfig().build());
        assertThat(config.getSnapshotMode()).isEqualTo(TiDbConnectorConfig.SnapshotMode.NO_DATA);
        assertThat(config.getTicdcInitialOffset()).isEqualTo(TiDbConnectorConfig.InitialOffset.EARLIEST);
    }

    @Test
    public void shouldNotValidateExplicitlyEmptyOrUnknownSnapshotMode() {
        assertThat(minimalConfig().with(TiDbConnectorConfig.SNAPSHOT_MODE, "")
                .build().validateAndRecord(TiDbConnectorConfig.ALL_FIELDS, error -> {
                })).isFalse();
        assertThat(minimalConfig().with(TiDbConnectorConfig.SNAPSHOT_MODE, "always")
                .build().validateAndRecord(TiDbConnectorConfig.ALL_FIELDS, error -> {
                })).isFalse();
    }

    @Test
    public void shouldNotValidateUnknownInitialOffset() {
        assertThat(minimalConfig().with(TiDbConnectorConfig.TICDC_INITIAL_OFFSET, "somewhere")
                .build().validateAndRecord(TiDbConnectorConfig.ALL_FIELDS, error -> {
                })).isFalse();
    }

    @Test
    public void shouldNotValidateDataSnapshotModeWithoutSqlEndpoint() {
        final Configuration config = minimalConfig()
                .with(TiDbConnectorConfig.SNAPSHOT_MODE, "initial")
                .build();
        assertThat(config.validateAndRecord(TiDbConnectorConfig.ALL_FIELDS, error -> {
        })).isFalse();
    }

    @Test
    public void shouldValidateDataSnapshotModeWithSqlEndpoint() {
        final Configuration config = minimalConfig()
                .with(TiDbConnectorConfig.SNAPSHOT_MODE, "initial")
                .with(TiDbConnectorConfig.JDBC_HOSTNAME, "localhost")
                .with(TiDbConnectorConfig.JDBC_USER, "root")
                .build();
        assertThat(config.validateAndRecord(TiDbConnectorConfig.ALL_FIELDS, error -> {
        })).isTrue();

        final TiDbConnectorConfig connectorConfig = new TiDbConnectorConfig(config);
        assertThat(connectorConfig.getSnapshotMode()).isEqualTo(TiDbConnectorConfig.SnapshotMode.INITIAL);
        assertThat(config.getInteger(TiDbConnectorConfig.JDBC_PORT)).isEqualTo(4000);
    }

    @Test
    public void shouldPassThroughConsumerProperties() {
        final TiDbConnectorConfig config = new TiDbConnectorConfig(
                minimalConfig()
                        .with("ticdc.consumer.security.protocol", "SASL_SSL")
                        .with("ticdc.consumer.fetch.max.bytes", "1048576")
                        .build());

        assertThat(config.getTicdcConsumerProperties())
                .containsEntry("bootstrap.servers", "localhost:9092")
                .containsEntry("security.protocol", "SASL_SSL")
                .containsEntry("fetch.max.bytes", "1048576");
    }

    @Test
    public void shouldFilterBuiltInDatabases() {
        assertThat(TiDbConnectorConfig.isBuiltInDatabase("mysql")).isTrue();
        assertThat(TiDbConnectorConfig.isBuiltInDatabase("INFORMATION_SCHEMA")).isTrue();
        assertThat(TiDbConnectorConfig.isBuiltInDatabase("metrics_schema")).isTrue();
        assertThat(TiDbConnectorConfig.isBuiltInDatabase("inventory")).isFalse();
    }

    private static boolean isValid(Configuration.Builder config) {
        return config.build().validateAndRecord(TiDbConnectorConfig.ALL_FIELDS, error -> {
        });
    }

    @Test
    public void shouldRejectOptionsTheTiCdcEncodingFixes() {
        assertThat(isValid(minimalConfig().with("decimal.handling.mode", "precise"))).isFalse();
        assertThat(isValid(minimalConfig().with("decimal.handling.mode", "double"))).isTrue();
        assertThat(isValid(minimalConfig().with("time.precision.mode", "connect"))).isFalse();
        assertThat(isValid(minimalConfig().with("time.precision.mode", "adaptive_time_microseconds"))).isTrue();
        assertThat(isValid(minimalConfig().with("binary.handling.mode", "bytes"))).isFalse();
        assertThat(isValid(minimalConfig().with("binary.handling.mode", "base64"))).isTrue();
    }

    @Test
    public void shouldRejectUnsupportedOptions() {
        assertThat(isValid(minimalConfig().with("converters", "custom"))).isFalse();
        assertThat(isValid(minimalConfig().with("heartbeat.action.query", "SELECT 1"))).isFalse();
        assertThat(isValid(minimalConfig().with("include.schema.comments", "true"))).isFalse();
    }

    @Test
    public void shouldRequireSqlEndpointForColumnMappers() {
        assertThat(isValid(minimalConfig().with("column.mask.with.5.chars", "inventory\\.customers\\.email"))).isFalse();
        assertThat(isValid(minimalConfig().with("column.truncate.to.10.chars", "inventory\\.customers\\.name"))).isFalse();
        assertThat(isValid(minimalConfig().with("column.propagate.source.type", ".*"))).isFalse();
        assertThat(isValid(minimalConfig()
                .with("column.mask.with.5.chars", "inventory\\.customers\\.email")
                .with(TiDbConnectorConfig.JDBC_HOSTNAME, "localhost")
                .with(TiDbConnectorConfig.JDBC_USER, "root"))).isTrue();
        // Column filters and key columns only need the column names, which TiCDC provides
        assertThat(isValid(minimalConfig()
                .with("column.exclude.list", "inventory\\.customers\\.email")
                .with("message.key.columns", "inventory.customers:id"))).isTrue();
    }

    @Test
    public void shouldStreamSnapshotRowsByDefault() {
        final TiDbConnectorConfig config = new TiDbConnectorConfig(minimalConfig().build());
        assertThat(config.getSnapshotFetchSize()).isEqualTo(Integer.MIN_VALUE);
    }
}
