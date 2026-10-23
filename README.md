# Debezium Connector for TiDB

This repository contains an incubating Debezium connector for [TiDB](https://www.pingcap.com/tidb/),
the first phase of TiDB support in Debezium, as discussed in
[DBZ-6269](https://issues.redhat.com/browse/DBZ-6269) /
[debezium/dbz#789](https://github.com/debezium/dbz/issues/789).

## Building the connector

The connector builds against the current Debezium core development version. Install the required
core modules into your local Maven repository first:

```
git clone https://github.com/debezium/debezium.git core
./mvnw clean install -f core/pom.xml -pl debezium-bom,debezium-connector-common -am -DskipTests -DskipITs
```

Then build the connector (add `-Passembly` to also produce the connector plugin archive):

```
./mvnw clean verify
```

The integration tests start a TiDB cluster (PD, TiKV, TiDB), a TiCDC server and a Kafka broker
with Testcontainers, so they need Docker. Skip them with `-DskipITs`. The TiDB version under test
can be changed with `-Dtidb.version=v8.5.1`.

## Architecture (phase 1)

TiDB does not expose a MySQL binlog and the only consumer of TiKV's raw `EventFeed` gRPC API is
TiCDC itself, which resolves raw key-values into rows using its schema snapshots. Rather than
re-implementing that translation layer, this connector consumes the Kafka output of a TiCDC
changefeed running in its native Debezium protocol mode (TiDB v8.0+):

```
cdc cli changefeed create \
  --sink-uri="kafka://<broker>:9092/<topic>?protocol=debezium"
```

```
TiDB/TiKV ──> TiCDC (changefeed, protocol=debezium) ──> Kafka topic(s)
                                                            │
                                                            ▼
                                          Debezium TiDB connector (this module)
                                                            │
                                                            ▼
                                       Kafka Connect topics (standard Debezium envelope)
```

On top of the raw TiCDC stream, the connector provides what a native Debezium connector is
expected to provide:

* **Connector lifecycle and offsets** — progress is stored in the Kafka Connect offset storage
  as the TiCDC topic/partition/offset positions plus the TSO `commit_ts` of the last processed
  transaction. Restarts resume exactly where the connector left off, no consumer group offsets
  are used.
* **A TiDB-specific `source` block** — TiCDC's Debezium output fills the MySQL-specific source
  fields with dummies (`server_id=0`, `gtid=null`, `file=""`, `pos=0`). The connector re-wraps
  the typed row images into its own envelope whose `source` struct carries the real position:
  `db`, `table`, `commit_ts` (TSO) and `cluster_id`.
* **Table filtering** — the standard `table.include.list`/`table.exclude.list` options are
  applied to the captured tables; TiDB system schemas are excluded by default.
* **Standard topic naming** — events are re-routed to `<topic.prefix>.<db>.<table>` topics using
  the configured topic naming strategy, independently of how the TiCDC changefeed partitions its
  output.

Schemas are learned from the typed TiCDC messages (the changefeed must produce JSON with inline
schemas, which is the default for `protocol=debezium`) and refreshed automatically when the
advertised row schema changes.

## Initial snapshots

With `snapshot.mode=initial` the connector captures the existing data of all captured tables
through TiDB's MySQL compatible SQL endpoint before streaming starts. The snapshot reads the
current TSO once, pins the session to it with `tidb_snapshot`, and reads every table at that one
consistent point. No locks are taken.

The snapshot TSO is recorded in the offsets. During streaming the connector drops any event whose
`commit_ts` is not newer than the snapshot TSO, so a changefeed whose `start-ts` lies at or
before the snapshot TSO hands off to streaming without duplicates. Create the changefeed with a
`start-ts` at or before the snapshot TSO so no change is missed; a changefeed created after the
snapshot with a later `start-ts` leaves a gap between the snapshot TSO and the `start-ts`.

The snapshot session must stay within the GC lifetime of the cluster (`tidb_gc_life_time`): TiDB
rejects `tidb_snapshot` reads older than the GC safe point, so very long snapshots need a longer
GC lifetime for their duration.

Rows are streamed from the server one by one rather than read into memory a table at a time.
Setting `snapshot.fetch.size` to a positive value fetches them through a server side cursor in
batches of that size instead.

Snapshot rows use the same encoding as TiCDC's Debezium output, so a table has one schema across
the hand off from snapshot to streaming. This includes the TiCDC conventions for zero dates
(`0000-00-00` is the epoch in NOT NULL columns and null otherwise), for column defaults, for
virtual generated columns (listed after all other columns and always null) and for the message
key of tables without a primary key (the unique index on NOT NULL columns that TiCDC uses).
TiCDC does not stream tables that have neither a primary key nor such an index unless the
changefeed sets `force-replicate`; the snapshot logs a warning for them.

`snapshot.include.collection.list` limits the initial snapshot to some of the captured tables.
`snapshot.select.statement.overrides` is not supported: the snapshot logs a warning and reads all
rows of the tables.

An on demand snapshot is started with an `execute-snapshot` signal of type `blocking`, for
example through a signal table named in `signal.data.collection` that the changefeed captures.
Streaming pauses while it runs. The `additional-conditions` of the signal give a full select
statement per table, which has to return all columns of the table in table order.

Encryption of the snapshot connection follows the MySQL driver default: TLS is negotiated when
the server supports it. Additional driver settings can be passed through with the `database.`
prefix, for example `database.sslMode=VERIFY_CA` to require a verified TLS connection. The
snapshot session runs with its time zone pinned to UTC so that TIMESTAMP values are read
consistently regardless of the cluster's global time zone.

## Configuration

Minimal example:

```json
{
  "connector.class": "io.debezium.connector.tidb.TiDbConnector",
  "topic.prefix": "tidb_server",
  "ticdc.kafka.bootstrap.servers": "broker:9092",
  "ticdc.topics": "ticdc-changefeed-topic"
}
```

| Option | Default | Description |
| --- | --- | --- |
| `ticdc.kafka.bootstrap.servers` | — | Kafka cluster the TiCDC changefeed writes to. |
| `ticdc.topics` | — | Comma-separated list of topics written by the changefeed. |
| `ticdc.initial.offset` | `earliest` | Where to start reading when no offsets are stored (`earliest`/`latest`). |
| `ticdc.poll.timeout.ms` | `500` | Poll timeout of the internal consumer. |
| `ticdc.consumer.*` | — | Pass-through properties for the internal Kafka consumer (e.g. security settings). |
| `snapshot.mode` | `no_data` | `no_data` (no data snapshot, structure learned from the TiCDC messages), `initial` (snapshot all captured tables through the SQL endpoint on first start, then stream) or `initial_only` (snapshot and stop). |
| `database.hostname` | — | Hostname of the TiDB SQL endpoint. Required for `initial` and `initial_only`. |
| `database.port` | `4000` | Port of the TiDB SQL endpoint. |
| `database.user` | — | User for the TiDB SQL endpoint. Required for `initial` and `initial_only`. |
| `database.password` | — | Password for the TiDB SQL endpoint. |

The common Debezium options apply to snapshot and streaming events alike: table and column
filters, `message.key.columns`, `column.mask.*`, `column.truncate.*`, `column.propagate.source.type`,
`datatype.propagate.source.type`, `tombstones.on.delete`, `skipped.operations`,
`skip.messages.without.change`, `event.processing.failure.handling.mode`, topic naming, signals,
notifications and SMTs. Streamed rows go through the same table schema as snapshot rows, so these
options give the same result in both. Column masking, truncation and source type propagation need
the source column types, which TiCDC's messages do not carry, so they require `database.hostname`
and `database.user` even with `snapshot.mode=no_data`.

Some options are rejected at validation because the TiCDC encoding fixes their behavior or the
connector does not support them: `decimal.handling.mode` other than `double`,
`time.precision.mode` other than `adaptive_time_microseconds`, `binary.handling.mode` other than
`base64`, `converters`, `heartbeat.action.query` and `include.schema.comments=true`.
`snapshot.max.threads` above 1 and `snapshot.tables.order.by.row.count` are accepted but have no
effect; the snapshot logs a warning for them.

The snapshot connection sets `connectTimeout=30000` unless `database.connectTimeout` is given.

## Roadmap

Per the maintainer guidance in DBZ-6269:

1. **(this module)** Connector consuming TiCDC's Debezium-format Kafka output with Debezium
   lifecycle management.
2. **Managed snapshots** — initial snapshots are implemented (`snapshot.mode=initial`);
   incremental snapshots through the signal mechanism follow.
3. **Direct TiKV streaming** — replace the TiCDC/Kafka dependency with a client of TiKV's
   `EventFeed` gRPC API; the rest of the connector (offsets keyed by `commit_ts`, envelope,
   snapshots) remains unchanged.

## Known limitations

* Incremental snapshots are not implemented yet; an initial snapshot is all or nothing and
  restarts from the beginning when interrupted.
* The TiCDC changefeed must emit JSON with inline schemas (`protocol=debezium` default).
* Decimal values arrive as `float64` from TiCDC's Debezium output; precise decimal encoding
  requires phase 2/3. Snapshot events use the same encodings as TiCDC so both describe a table
  the same way: binary strings are base64 encoded strings, and `BIGINT UNSIGNED` values above
  `9223372036854775807` wrap into negative `int64` values exactly as TiCDC encodes them.
* An on demand (blocking) snapshot through the `execute-snapshot` signal snapshots only the
  requested tables and does not move the streaming position; streamed changes of those tables
  that occurred while streaming was paused are emitted again after the snapshot.
* TiCDC replaces an explicit NULL with the column default when it encodes a row. The snapshot
  emits the NULL that is stored, so for nullable columns that have a default, a NULL value reads
  differently in snapshot and streamed events.
* DDL and watermark events (TiCDC v9+) are skipped; schema changes are picked up lazily from the
  data messages.
* Single task; parallelism is bounded by the TiCDC topic partitioning for now.
