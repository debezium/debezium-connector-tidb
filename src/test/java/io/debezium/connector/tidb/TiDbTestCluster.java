/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.tidb;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;

/**
 * A TiDB cluster (PD, TiKV, TiDB), a TiCDC server and a Kafka broker in containers, shared by
 * all integration tests of a JVM. TiCDC needs a real TiKV store, so the cluster cannot use TiDB's
 * embedded storage.
 *
 * @author Aviral Srivastava
 */
public final class TiDbTestCluster {

    private static final Logger LOGGER = LoggerFactory.getLogger(TiDbTestCluster.class);

    private static final String TIDB_VERSION = System.getProperty("tidb.version", "v8.5.1");
    private static final String KAFKA_IMAGE = "apache/kafka:3.9.0";
    private static final String KAFKA_INTERNAL_LISTENER = "kafka:19092";
    private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(3);

    private static TiDbTestCluster instance;

    private final Network network = Network.newNetwork();
    private final GenericContainer<?> pd;
    private final GenericContainer<?> tikv;
    private final GenericContainer<?> tidb;
    private final GenericContainer<?> ticdc;
    private final GenericContainer<?> kafka;
    private final int kafkaHostPort;

    private TiDbTestCluster() {
        pd = new GenericContainer<>("pingcap/pd:" + TIDB_VERSION)
                .withNetwork(network)
                .withNetworkAliases("pd")
                .withCommand("--name=pd", "--client-urls=http://0.0.0.0:2379", "--peer-urls=http://0.0.0.0:2380",
                        "--advertise-client-urls=http://pd:2379", "--advertise-peer-urls=http://pd:2380",
                        "--initial-cluster=pd=http://pd:2380", "--data-dir=/data/pd")
                .waitingFor(Wait.forLogMessage(".*PD leader is ready to serve.*", 1).withStartupTimeout(STARTUP_TIMEOUT));
        tikv = new GenericContainer<>("pingcap/tikv:" + TIDB_VERSION)
                .withNetwork(network)
                .withNetworkAliases("tikv")
                .withCommand("--addr=0.0.0.0:20160", "--advertise-addr=tikv:20160", "--status-addr=0.0.0.0:20180",
                        "--pd=pd:2379", "--data-dir=/data/tikv")
                .waitingFor(Wait.forLogMessage(".*TiKV is ready to serve.*", 1).withStartupTimeout(STARTUP_TIMEOUT));
        tidb = new GenericContainer<>("pingcap/tidb:" + TIDB_VERSION)
                .withNetwork(network)
                .withNetworkAliases("tidb")
                .withExposedPorts(4000)
                .withCommand("--store=tikv", "--path=pd:2379", "--advertise-address=tidb")
                .waitingFor(Wait.forLogMessage(".*server is running MySQL protocol.*", 1).withStartupTimeout(STARTUP_TIMEOUT));
        // A fixed host port, so that the broker can advertise it to the connector running on the
        // host while TiCDC reaches the broker through the internal listener
        kafkaHostPort = freePort();
        kafka = new FixedHostPortGenericContainer<>(KAFKA_IMAGE)
                .withFixedExposedPort(kafkaHostPort, kafkaHostPort)
                .withNetwork(network)
                .withNetworkAliases("kafka")
                .withEnv("KAFKA_NODE_ID", "1")
                .withEnv("KAFKA_PROCESS_ROLES", "broker,controller")
                .withEnv("KAFKA_LISTENERS", "INTERNAL://:19092,EXTERNAL://:" + kafkaHostPort + ",CONTROLLER://:9093")
                .withEnv("KAFKA_ADVERTISED_LISTENERS", "INTERNAL://" + KAFKA_INTERNAL_LISTENER + ",EXTERNAL://localhost:" + kafkaHostPort)
                .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT")
                .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "INTERNAL")
                .withEnv("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
                .withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@kafka:9093")
                .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
                .waitingFor(Wait.forLogMessage(".*Kafka Server started.*", 1).withStartupTimeout(STARTUP_TIMEOUT));
        ticdc = new GenericContainer<>("pingcap/ticdc:" + TIDB_VERSION)
                .withNetwork(network)
                .withNetworkAliases("ticdc")
                .withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint("/cdc"))
                .withCommand("server", "--addr=0.0.0.0:8300", "--advertise-addr=ticdc:8300", "--pd=http://pd:2379")
                .waitingFor(Wait.forLogMessage(".*campaign owner successfully.*", 1).withStartupTimeout(STARTUP_TIMEOUT));
    }

    /**
     * @return the running cluster, started on first use and stopped when the JVM exits
     */
    public static synchronized TiDbTestCluster get() {
        if (instance == null) {
            final TiDbTestCluster cluster = new TiDbTestCluster();
            cluster.start();
            Runtime.getRuntime().addShutdownHook(new Thread(cluster::stop));
            instance = cluster;
        }
        return instance;
    }

    private void start() {
        LOGGER.info("Starting TiDB {} cluster with TiCDC and Kafka", TIDB_VERSION);
        pd.start();
        tikv.start();
        tidb.start();
        kafka.start();
        ticdc.start();
        tidb.followOutput(new Slf4jLogConsumer(LoggerFactory.getLogger("tidb")).withPrefix("tidb"));
    }

    private void stop() {
        for (GenericContainer<?> container : new GenericContainer<?>[]{ ticdc, kafka, tidb, tikv, pd }) {
            container.stop();
        }
        network.close();
    }

    public String tidbHost() {
        return tidb.getHost();
    }

    public int tidbPort() {
        return tidb.getMappedPort(4000);
    }

    public String kafkaBootstrapServers() {
        return "localhost:" + kafkaHostPort;
    }

    private static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public Connection connect() throws SQLException {
        return DriverManager.getConnection("jdbc:mysql://" + tidbHost() + ":" + tidbPort() + "/?user=root", "root", "");
    }

    /**
     * Runs the given statements in one session.
     */
    public void execute(String... statements) throws SQLException {
        try (Connection connection = connect(); Statement statement = connection.createStatement()) {
            for (String sql : statements) {
                statement.execute(sql);
            }
        }
    }

    /**
     * @return the current TSO of the cluster
     */
    public long currentTso() throws SQLException {
        try (Connection connection = connect();
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("SHOW MASTER STATUS")) {
            rs.next();
            return rs.getLong("Position");
        }
    }

    /**
     * Creates a changefeed writing the changes of one database in Debezium format to the given
     * topic.
     */
    public void createChangefeed(String changefeedId, String database, String topic) throws Exception {
        final String configFile = "/tmp/" + changefeedId + ".toml";
        ticdc.copyFileToContainer(Transferable.of("[filter]\nrules = ['" + database + ".*']\n"), configFile);
        cdc("changefeed", "create", "--server=http://127.0.0.1:8300", "--changefeed-id=" + changefeedId,
                "--config=" + configFile,
                "--sink-uri=kafka://" + KAFKA_INTERNAL_LISTENER + "/" + topic
                        + "?protocol=debezium&kafka-version=3.9.0&replication-factor=1&partition-num=1");
    }

    public void removeChangefeed(String changefeedId) throws Exception {
        cdc("changefeed", "remove", "--server=http://127.0.0.1:8300", "--changefeed-id=" + changefeedId);
    }

    private void cdc(String... args) throws Exception {
        final String[] command = new String[args.length + 2];
        command[0] = "/cdc";
        command[1] = "cli";
        System.arraycopy(args, 0, command, 2, args.length);
        final ExecResult result = ticdc.execInContainer(command);
        if (result.getExitCode() != 0) {
            throw new IllegalStateException("TiCDC command failed: " + result.getStdout() + result.getStderr());
        }
    }

    /**
     * @return a connector configuration consuming the given TiCDC topic and reading snapshots
     *         through the SQL endpoint of the cluster
     */
    public Configuration.Builder connectorConfig(String database, String topic) {
        return Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "tidb_it")
                .with(TiDbConnectorConfig.TICDC_BOOTSTRAP_SERVERS, kafkaBootstrapServers())
                .with(TiDbConnectorConfig.TICDC_TOPICS, topic)
                .with(TiDbConnectorConfig.JDBC_HOSTNAME, tidbHost())
                .with(TiDbConnectorConfig.JDBC_PORT, tidbPort())
                .with(TiDbConnectorConfig.JDBC_USER, "root")
                .with("database.include.list", database)
                .with(CommonConnectorConfig.POLL_INTERVAL_MS, 100);
    }
}
