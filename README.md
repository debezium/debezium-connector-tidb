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

All common Debezium options (`table.include.list`, `topic.naming.strategy`, `tombstones.on.delete`,
SMTs, ...) apply as usual.

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
  requires phase 2/3.
* DDL and watermark events (TiCDC v9+) are skipped; schema changes are picked up lazily from the
  data messages.
* Single task; parallelism is bounded by the TiCDC topic partitioning for now.
