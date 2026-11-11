# 08 Join: Flink side

This directory holds the Apache Flink implementation of the stream-stream join pattern for the RTM Pattern Pack. The job reads the topics `rtmpack.08-join.input.orders` and `rtmpack.08-join.input.payments`, joins each payment to its order, and writes the matches to `rtmpack.08-join.out.flink`. It runs as an Amazon Managed Service for Apache Flink application named `rtmpack-flink-08-join`. The pattern is described in [../README.md](../README.md), and the RTM side is in [../rtm/README.md](../rtm/README.md).

The benchmark contract is in [../../../docs/CONTRACT.md](../../../docs/CONTRACT.md). Section 9 and the rules of this pattern (in [../README.md](../README.md), Benchmark setup) apply. This README covers the choices that matter for the comparison with Spark Real-Time Mode.

## Layout

```
patterns/08-join/flink/
  pom.xml                                   Maven project, Java 17, shaded jar
  sql/join.sql                              readable copy of the SQL, for docs
  src/main/resources/join.sql               the SQL the job executes
  src/main/java/com/rtmpack/join/
    JoinJob.java                            entry point; renders and runs the SQL
    JoinSql.java                            loads, renders and splits the script
    JobConfig.java                          parameter resolution
  src/test/java/com/rtmpack/join/
    JoinSqlTest.java                        plan shape and bounded join result
```

The build does not check that `sql/join.sql` matches `src/main/resources/join.sql`. Compare the two with `cmp` after any edit.

## How it works

The job is a Table API application. It defines three Kafka tables in Flink SQL, and runs one `INSERT` statement:

1. The two source tables read JSON from `<app.input.topic>.orders` and `<app.input.topic>.payments` over SASL_SSL with SCRAM-SHA-512. Each reads the broker append timestamp with `METADATA FROM 'timestamp'` into `in_ts_ltz`.
2. Each source has a rowtime attribute, `event_time AS TO_TIMESTAMP_LTZ(event_ts, 3)`, and a watermark of `event_time - INTERVAL '5' SECOND`.
3. The statement is an interval join:

```sql
FROM orders AS o
JOIN payments AS p
  ON o.order_id = p.order_id
 AND p.event_time BETWEEN o.event_time AND o.event_time + INTERVAL '30' SECOND
```

4. `in_ts` is the `GREATEST` of the two input timestamps, each converted to epoch milliseconds in UTC. The output has `order_id`, `payment_id`, `key`, `amount`, `method`, `order_ts`, `payment_ts`, `in_ts` and `engine = 'flink'`.
5. The sink table writes JSON to `rtmpack.08-join.out.flink`. The Kafka record key is `order_id` (raw format).

`method` is a reserved word in Flink SQL, so it is quoted with backticks in the DDL and the query.

The script sets two session options at the top. `pipeline.name` names the job. `table.local-time-zone` is pinned to UTC, so the timestamp conversion does not depend on the host.

`JoinJob` splits the script into statements. It runs the SET and CREATE statements in order, then waits on the INSERT. Statements are split on semicolons outside single-quoted literals. Whole-line `--` comments are removed first.

## Configuration

Parameters come from three sources. Managed Flink runtime properties take precedence, then command-line arguments, then environment variables, then defaults.

| Managed Flink property | Flat name | Argument or env var | Required | Default |
|---|---|---|---|---|
| group `kafka`, `bootstrap.servers` | `kafka.bootstrap.servers` | `--kafka.bootstrap.servers` / `RTMPACK_KAFKA_BOOTSTRAP_SERVERS` | yes | none |
| group `kafka`, `username` | `kafka.username` | `--kafka.username` / `RTMPACK_KAFKA_USERNAME` | yes | none |
| group `kafka`, `password` | `kafka.password` | `--kafka.password` / `RTMPACK_KAFKA_PASSWORD` | yes | none |
| group `app`, `state.ttl.ms` | `app.state.ttl.ms` | `--app.state.ttl.ms` / `RTMPACK_APP_STATE_TTL_MS` | no | `60000` |
| group `app`, `input.topic` | `app.input.topic` | `--app.input.topic` / `RTMPACK_APP_INPUT_TOPIC` | no | `rtmpack.08-join.input` (base) |
| group `app`, `output.topic` | `app.output.topic` | `--app.output.topic` / `RTMPACK_APP_OUTPUT_TOPIC` | no | `rtmpack.08-join.out.flink` |

`app.input.topic` is a base. The job appends `.orders` and `.payments`. `app.output.topic` is used as is.

Outside Managed Flink, `KinesisAnalyticsRuntime` finds no property file, so the job uses arguments and environment variables. The password is never logged. The job builds the SASL JAAS string in code from the username and password, and escapes single quotes in values for SQL. The SASL mechanism and protocol are fixed in the SQL, as the contract requires.

## Configuration choices

The sink uses `sink.delivery-guarantee = 'at-least-once'` and `linger.ms = 0`, as the contract requires. The comparison with RTM therefore holds. After a failure and restore, the job can write a record again. The collector reports these as `sink_duplicates`.

The interval join drops buffered rows as the watermark advances. An order is dropped once the watermark passes its event time plus 30 seconds. A payment is dropped once the watermark passes its event time. A payment can only match orders up to 30 seconds earlier, so the retention follows the same bounds. `table.exec.state.ttl` does not apply to this operator, so `app.state.ttl.ms` has no effect on this pattern. The script still sets it, so every pattern resolves the same parameters. The state size follows the input rate and the 30-second window plus the 5-second watermark delay, not a TTL.

The watermark is 5 seconds behind the largest event time on each source, as the contract states. The join output for a pair is emitted when both rows have arrived, not when the watermark passes. The watermark therefore adds no latency for matches. It only controls when state is cleaned.

Checkpoints run every `checkpoint_interval_ms`, which the Terraform module sets to 60000. Snapshots are enabled, so the application can be restored after a stop.

Both sources use `scan.startup.mode = 'latest-offset'`.

The join shuffles both inputs by `order_id`. The default 100 ms `execution.buffer-timeout.interval` therefore applies, as in the deduplication pattern ([../../01-dedup/flink/README.md](../../01-dedup/flink/README.md)). Managed Flink does not allow application code to change it.

The JSON format uses its default behavior, so a record that does not parse fails the job.

## Parallelism and KPUs

The contract uses parallelism 8 with `ParallelismPerKPU = 1`. That gives 8 KPUs for the job and 1 orchestration KPU, 9 KPUs in total. The source reads 8 partitions per topic, so each source subtask reads one partition.

## Runtime

| Item | Value |
|---|---|
| Runtime | Flink 2.3.0, `RuntimeEnvironment = FLINK-2_3` |
| Java | 17 |
| Kafka connector | `flink-connector-kafka` 4.0.1-2.0 |
| Runtime properties | `com.amazonaws:aws-kinesisanalytics-runtime` 1.2.0 |

## Build and test

Requirements: JDK 17 and Maven 3.9 or later.

```sh
infra/scripts/build-flink.sh 08-join   # from the repository root; compiles, runs JoinSqlTest, writes the shaded jar under target/
```

`infra/scripts/test-pattern.sh 08-join` runs this build together with the RTM and harness tests.

`JoinSqlTest` runs the statements from `join.sql` on two bounded in-memory sources. The sources have the same columns, rowtime expression and watermark as the Kafka tables. The three Kafka DDL statements are replaced, so the test needs no broker. It checks:

- The optimized plan contains `IntervalJoin` and no regular `Join` node.
- A payment at +0 s and +30 s after the order matches. A payment 1 ms before the order does not match, and neither does a payment 30.001 s after it. A payment with an unrelated order id does not match, and an order without a payment produces no match.
- The output fields, `engine = 'flink'`, and `in_ts` as the larger of the two input timestamps, in epoch milliseconds.

`SET` statements in `join.sql` are applied to the table configuration by `JoinSql.applyIfSet`, because the Table API does not accept `SET` through `executeSql`.

The shaded jar contains the Kafka connector and the service descriptors Flink needs. Flink, Table API, the planner loader and the runtime properties library are `provided`, because Managed Flink supplies them.

## Deploy

The `infra/terraform/modules/flink_app` module uploads the jar and creates the application, its execution role and its log group. It sets the runtime properties from the names listed above. Set `app.input.topic` to the base `rtmpack.08-join.input`, and create both input topics (`.orders` and `.payments`) before starting the application.
