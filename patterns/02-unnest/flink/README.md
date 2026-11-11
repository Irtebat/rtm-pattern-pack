# 02 Array unnest: Flink

This directory holds the Apache Flink implementation of pattern 02-unnest for the RTM Pattern Pack. The job reads orders from `rtmpack.02-unnest.input`, emits one record per element of the `items` array, and writes the result to `rtmpack.02-unnest.out.flink`. It runs as an Amazon Managed Service for Apache Flink application named `rtmpack-flink-02-unnest` on `FLINK-2_3`. The pattern is described in [../README.md](../README.md). The input and output formats are defined in sections 2 and 3 of [docs/CONTRACT.md](../../../docs/CONTRACT.md). The Spark Real-Time Mode side is in [../rtm/README.md](../rtm/README.md).

The project follows the layout and conventions of [01-dedup/flink](../../01-dedup/flink/README.md).

## Layout

```
patterns/02-unnest/flink/
  pom.xml                                   Maven project, Java 17, shaded jar
  sql/unnest.sql                            readable copy of the SQL, for docs
  src/main/resources/unnest.sql             the SQL the job executes
  src/main/java/com/rtmpack/unnest/
    UnnestJob.java                          entry point; renders and runs the SQL
    UnnestSql.java                          loads, renders and splits the script
    JobConfig.java                          parameter resolution
  src/test/java/com/rtmpack/unnest/
    UnnestSqlTest.java                      runs the unnest statements on bounded input
```

The build does not check that `sql/unnest.sql` matches `src/main/resources/unnest.sql`. Compare the two with `cmp` after any edit.

## Query

```sql
INSERT INTO unnest_output
SELECT
  CONCAT(o.order_id, ':', CAST(t.ord - 1 AS STRING)) AS kafka_key,
  o.order_id, o.`key`, o.event_ts,
  t.ord - 1 AS item_idx,
  t.sku, t.qty, t.price,
  <epoch-ms conversion of o.in_ts_ltz> AS in_ts,
  'flink' AS engine
FROM unnest_input AS o
CROSS JOIN UNNEST(o.items) WITH ORDINALITY AS t(sku, qty, price, ord);
```

`WITH ORDINALITY` numbers elements from 1. The contract defines `item_idx` as the 0-based position. The query therefore uses `ord - 1` for both the `item_idx` column and the key suffix.

The source table `unnest_input` reads JSON over SASL_SSL with SCRAM-SHA-512. Its schema is `order_id`, `key`, `event_ts`, `items ARRAY<ROW<sku STRING, qty INT, price DOUBLE>>` and `payload`. The Kafka broker timestamp is read with `METADATA FROM 'timestamp'` into `in_ts_ltz`. `scan.startup.mode = 'latest-offset'` starts a fresh run at the end of the topic. The `payload` field is declared so the schema matches the input. It is not copied to the output, because the contract output fields do not include it.

`in_ts` uses the same conversion as 01-dedup. The `TIMESTAMP_LTZ(3)` value is cast to a string, parsed with `UNIX_TIMESTAMP`, and the millisecond part is added back. `table.local-time-zone` is pinned to `UTC`, so the result does not depend on the host. All output rows from one order carry the same `in_ts`. Latency per output record is measured against the order's input timestamp, as the contract requires.

### Sink key and value

The Kafka key is `<order_id>:<item_idx>`. The sink table declares a physical STRING column `kafka_key` and lists it in `key.fields`, with `key.format = 'raw'`. Computed columns are not part of a sink's writable schema. So the key is computed in the SELECT and written to the physical column.

`value.fields-include = 'EXCEPT_KEY'` removes the columns named in `key.fields` from the value. The JSON value therefore holds exactly the contract fields: `order_id`, `key`, `event_ts`, `item_idx`, `sku`, `qty`, `price`, `in_ts` and `engine`. Here `key` is the customer key column, not the Kafka record key. Because `key.fields` names only `kafka_key`, the customer `key` column stays in the value. The test plans the INSERT against the real Kafka sink DDL, which validates these options.

## No shuffle

The statement has no `GROUP BY`, join on a key, window or deduplication, so no operator needs records partitioned by key. The plan is the Kafka source, a correlate (the UNNEST), a calc, and the Kafka sink. Flink chains these operators into one task per parallel instance. A record moves from the source to the sink inside one thread, without serialization and without a network buffer. The unit test asserts that the plan for the INSERT contains no `Exchange` node.

Managed Flink flushes network buffers between operators every `execution.buffer-timeout.interval` (default 100 ms), and application code cannot change it. A keyed job such as [01-dedup](../../01-dedup/flink/README.md) has a hash exchange, where records wait in such a buffer. This plan has no exchange, so the buffer timeout does not apply. The only buffering left is the Kafka producer batch, which is why the sink sets `linger.ms = 0`. The source partitions map one to one to the chained tasks. With parallelism equal to the partition count, each task reads one partition and writes to the sink without a rebalance.

## Configuration

Parameters come from three sources. Managed Flink runtime properties take precedence, then command-line arguments, then environment variables, then defaults.

| Managed Flink property | Flat name | Argument or env var | Required | Default |
|---|---|---|---|---|
| group `kafka`, `bootstrap.servers` | `kafka.bootstrap.servers` | `--kafka.bootstrap.servers` / `RTMPACK_KAFKA_BOOTSTRAP_SERVERS` | yes | none |
| group `kafka`, `username` | `kafka.username` | `--kafka.username` / `RTMPACK_KAFKA_USERNAME` | yes | none |
| group `kafka`, `password` | `kafka.password` | `--kafka.password` / `RTMPACK_KAFKA_PASSWORD` | yes | none |
| group `app`, `input.topic` | `app.input.topic` | `--app.input.topic` / `RTMPACK_APP_INPUT_TOPIC` | no | `rtmpack.02-unnest.input` |
| group `app`, `output.topic` | `app.output.topic` | `--app.output.topic` / `RTMPACK_APP_OUTPUT_TOPIC` | no | `rtmpack.02-unnest.out.flink` |

The pattern is stateless, so there is no state TTL. If the Terraform module passes `state.ttl.ms` in the `app` group, the job accepts it and ignores it. The SQL does not set `table.exec.state.ttl`.

The password is never logged. The job builds the SASL JAAS string in code from the username and password, and escapes single quotes in values for SQL. The SASL mechanism and protocol are fixed in the SQL, as in 01-dedup.

## Configuration choices

The sink is at-least-once: `sink.delivery-guarantee = 'at-least-once'`. This matches the Kafka sink of the Spark Real-Time Mode side. After a failure and restore, the job can write some records again. The collector reports these as `sink_duplicates`, separate from `missing`. The output key is `<order_id>:<item_idx>`, so a repeated record has the same key as the original.

The producer linger is `'properties.linger.ms' = '0'`. Section 8 of the contract requires this for both engines. The Kafka producer then sends each record without waiting to fill a batch.

Checkpoints run every 60 seconds. The Terraform module sets this. There is no keyed state, so checkpoints only record the source offsets.

A fresh run starts at `latest-offset`. After a restore from a snapshot, the saved offsets are used.

The network buffer timeout is not changed. See "No shuffle" above.

The JSON format uses its default behavior, so a record that does not parse fails the job. This keeps a benchmark run from silently dropping events. An order with an empty `items` array produces no output rows, which matches the contract (1 to 5 items per order).

## Parallelism and KPUs

This is the same as 01-dedup. Parallelism equals the partition count (default 8) with `ParallelismPerKPU = 1`, which gives 8 KPUs plus 1 orchestration KPU. Autoscaling is off. Output volume is about three times the input rate, so the sink does more work per task than in 01-dedup. The KPU count is the same, for a fair comparison with the other patterns.

## Build and test

Requirements: JDK 17 and Maven 3.9 or later.

```sh
export JAVA_HOME=<path to a JDK 17>
infra/scripts/build-flink.sh 02-unnest   # compiles, runs UnnestSqlTest, writes the shaded jar under patterns/02-unnest/flink/target/
```

`build-flink.sh` downloads from Maven Central, or from whatever your own `~/.m2/settings.xml` configures. To route downloads through a mirror, set `RTMPACK_MAVEN_MIRROR`. To use another settings file, set `RTMPACK_MAVEN_SETTINGS`. The full local test run for the pattern is `infra/scripts/test-pattern.sh 02-unnest`.

The shaded jar contains the Kafka connector and the service descriptors Flink needs. Flink, the Table API, the planner loader and the runtime properties library are `provided`, because Managed Flink supplies them. The surefire configuration excludes `flink-table-planner-loader` from the test classpath, because the tests use the full planner. `flink-connector-base` and `flink-json` are test-scope only. The Flink distribution provides them at runtime, but the sink planning in the test needs them on the classpath.

`UnnestSqlTest` runs the statements from `unnest.sql` on a bounded in-memory source. The source Kafka DDL is replaced by a view with the same columns. The sink DDL is executed, which needs no broker, and the INSERT is planned against it. The test checks:

- the output row count equals the sum of the item counts
- `item_idx` is 0-based and dense, and `sku`, `qty` and `price` match their elements
- the key is `<order_id>:<item_idx>` and keys are distinct
- `in_ts` is the epoch-millisecond value of the input timestamp
- the plan has no `Exchange`
- the sink options contain `key.fields`, `EXCEPT_KEY`, `linger.ms = 0` and the at-least-once setting

## Deploy

The `infra/terraform/modules/flink_app` module uploads the jar and creates the application. It sets the runtime properties listed above. See [infra/README.md](../../../infra/README.md). Set `start_application = true` only after the input topic exists and the MSK network path is in place.
