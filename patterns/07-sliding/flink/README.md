# 07 Sliding: Flink side

This directory holds the Apache Flink implementation of the sliding window pattern for the RTM Pattern Pack. The job reads the input topic `rtmpack.07-sliding.input`, keeps a running `count`, `sum(amount)` and `max(in_ts)` per key and 60-second window that slides every 20 seconds, and writes every update to `rtmpack.07-sliding.out.flink`. It runs as an Amazon Managed Service for Apache Flink application named `rtmpack-flink-07-sliding`. The pattern is described in [../README.md](../README.md), and the RTM side is in [../rtm/README.md](../rtm/README.md).

The benchmark contract is in [../../../docs/CONTRACT.md](../../../docs/CONTRACT.md). The rules for this pattern are in [../README.md](../README.md), Benchmark setup. This README covers the choices that matter for the comparison with Spark Real-Time Mode.

## Layout

```
patterns/07-sliding/flink/
  pom.xml                                   Maven project, Java 17, shaded jar
  sql/sliding.sql                           readable copy of the SQL, for docs
  src/main/resources/sliding.sql            the SQL the job executes
  src/main/java/com/rtmpack/sliding/
    SlidingJob.java                         entry point; renders and runs the SQL
    SlidingSql.java                         loads, renders and splits the script
    JobConfig.java                          parameter resolution
  src/test/java/com/rtmpack/sliding/
    SlidingSqlTest.java                     plan shape and bounded-input result tests
```

The build does not check that `sql/sliding.sql` matches `src/main/resources/sliding.sql`. Compare the two with `cmp` after any edit.

## How it works

The job is a Table API application. It defines a Kafka source table and an `upsert-kafka` sink table in Flink SQL, and runs one `INSERT` statement:

1. The source table reads JSON records from `rtmpack.07-sliding.input` over SASL_SSL with SCRAM-SHA-512. The Kafka broker append timestamp is read with `METADATA FROM 'timestamp'` into `in_ts_ltz`.
2. The query computes `ws = event_ts - MOD(event_ts, 20000)` and expands each row with `CROSS JOIN UNNEST(ARRAY[ws, ws - 20000, ws - 40000])`. The three values are the window starts. Each start `s` satisfies `s <= event_ts < s + 60000`. These are the same windows as Spark's `window(event_time, "60 seconds", "20 seconds")`. The `HOP` window table-valued function is not used.
3. The statement uses a regular `GROUP BY` aggregation. It does not use a window table-valued function, because `TUMBLE` and `HOP` emit a result only when the window closes. This pattern needs the running count after every input. So the window is carried as a grouping column, and the aggregation emits an update for every input row. Mini-batch is off, which is the default.
4. The sink table is `upsert-kafka` with `PRIMARY KEY (kafka_key) NOT ENFORCED`. Its key format is `raw` on the `kafka_key` string, and its value format is `json`, with `value.fields-include = EXCEPT_KEY`. The Kafka record key is `<key>:<window_start_ms>`. The value holds `key`, `window_start_ms`, `window_end_ms`, `count`, `sum_amount`, `in_ts` and `engine = 'flink'`. `in_ts` is the largest input broker timestamp in the group, in epoch milliseconds, computed in UTC.

The script sets three session options at the top. `pipeline.name` names the job. `table.local-time-zone` is pinned to UTC, so the timestamp conversion does not depend on the host. `table.exec.state.ttl` sets the state retention.

The query uses `GROUP BY kafka_key`, where `kafka_key = key || ':' || window_start`. `key` and `window_start` are carried with `MAX`. This defines the same groups as `GROUP BY key, window_start`. The sink's primary key is `kafka_key`. Grouping on it makes the aggregate's upsert key equal to the primary key, so no materializer is planned. The cost is two extra `MAX` values in each group's aggregate state.

`SlidingJob` splits the script into statements. It runs the SET and CREATE statements in order, then waits on the INSERT. Statements are split on semicolons outside single-quoted literals. Whole-line `--` comments are removed first.

## Plan shape and changelog mode

The plan, checked by `SlidingSqlTest`, is `TableSourceScan` -> `Calc` -> `Exchange(hash[kafka_key])` -> `GroupAggregate` -> `Calc` -> `Sink`. There is exactly one `GroupAggregate`, after one `Exchange`. The plan has no window operator, no `LocalGroupAggregate`, no mini-batch assigner and no `SinkUpsertMaterializer`.

The changelog mode of the aggregate is `[I,UA]`. That means inserts and update-after records, with no `UPDATE_BEFORE`. The `upsert-kafka` sink declares an upsert changelog (insert, update-after, delete), so the planner does not ask the aggregate for retractions. Each input therefore produces one Kafka record per window. The first input of a group is an insert, and every later input is an update-after. Both are written as a keyed JSON value. The query never produces a delete, so no tombstone records are written.

The `UNNEST` turns each input into three rows before the exchange. The shuffle therefore carries three times the input rate, about 30,000 rows per second at 10,000 events per second. The sink receives about three records per input. The state holds three groups per key per 20-second step.

## Configuration

Parameters come from three sources. Managed Flink runtime properties take precedence, then command-line arguments, then environment variables, then defaults.

| Managed Flink property | Flat name | Argument or env var | Required | Default |
|---|---|---|---|---|
| group `kafka`, `bootstrap.servers` | `kafka.bootstrap.servers` | `--kafka.bootstrap.servers` / `RTMPACK_KAFKA_BOOTSTRAP_SERVERS` | yes | none |
| group `kafka`, `username` | `kafka.username` | `--kafka.username` / `RTMPACK_KAFKA_USERNAME` | yes | none |
| group `kafka`, `password` | `kafka.password` | `--kafka.password` / `RTMPACK_KAFKA_PASSWORD` | yes | none |
| group `app`, `state.ttl.ms` | `app.state.ttl.ms` | `--app.state.ttl.ms` / `RTMPACK_APP_STATE_TTL_MS` | no | `120000` |
| group `app`, `input.topic` | `app.input.topic` | `--app.input.topic` / `RTMPACK_APP_INPUT_TOPIC` | no | `rtmpack.07-sliding.input` |
| group `app`, `output.topic` | `app.output.topic` | `--app.output.topic` / `RTMPACK_APP_OUTPUT_TOPIC` | no | `rtmpack.07-sliding.out.flink` |

Outside Managed Flink, `KinesisAnalyticsRuntime` finds no property file, so the job uses arguments and environment variables. The password is never logged. The job builds the SASL JAAS string in code from the username and password, and escapes single quotes in values for SQL.

The SASL mechanism and protocol (`SCRAM-SHA-512` over `SASL_SSL`) are fixed in the SQL, as the contract requires.

## Configuration choices

The job does not enable Kafka transactions on the `upsert-kafka` sink. The guarantee is therefore at-least-once, which matches the RTM Kafka sink (contract section 5). After a failure and restore, the job can write an update again. The output is keyed, and each record carries the full running aggregate. A repeated update is therefore idempotent for the final value.

`table.exec.state.ttl` is set from `app.state.ttl.ms`. The default is 120000 (120 seconds, see [../README.md](../README.md), Benchmark setup). Flink drops the state of a group when it has had no update for that long. The window is 60 seconds, so a group stays for at least one window length after its last event. If an event arrives for a group whose state has expired, Flink starts a new group, and the count restarts from 1.

There is no watermark, so Flink never drops a late event. RTM drops events older than its 10-second watermark.

Checkpoints run every `checkpoint_interval_ms`. The benchmark sets this to 60000 (contract section 8). Snapshots are enabled, so the application can be restored from its last snapshot after a stop.

The source uses `scan.startup.mode = 'latest-offset'`, so a fresh run starts at the current end of the topic. After a restore from a snapshot, the saved offsets are used.

The sink sets `properties.linger.ms = 0` (contract section 8). `upsert-kafka` has its own sink buffer, controlled by `sink.buffer-flush.max-rows` and `sink.buffer-flush.interval`. That buffer is disabled by default. The pattern leaves it at the default, so records are written without waiting.

The aggregation shuffles by `kafka_key`, so network buffers are flushed every `execution.buffer-timeout.interval` (100 ms default). The setting is not on the list of properties customers can modify, so the benchmark uses Managed Flink as delivered. See [../../01-dedup/flink/README.md](../../01-dedup/flink/README.md).

The JSON format uses its default behavior, so a record that does not parse fails the job. Rows with a null `key` or `event_ts` are filtered out in the SQL, as the RTM job does.

## Parallelism and KPUs

Managed Flink allocates KPUs as `Parallelism / ParallelismPerKPU`, and adds one KPU for orchestration. The contract (section 9) sets parallelism 8 and `ParallelismPerKPU = 1`. That gives 8 KPUs for the job plus 1 orchestration KPU, 9 KPUs in total. Autoscaling is off. The runtime and connector versions are the same as for the deduplication pattern ([../../01-dedup/flink/README.md](../../01-dedup/flink/README.md)).

## Build and test

Requirements: JDK 17 and Maven 3.9 or later.

```sh
infra/scripts/build-flink.sh 07-sliding   # from the repository root; compiles, runs SlidingSqlTest, writes the shaded jar under target/
```

`infra/scripts/test-pattern.sh 07-sliding` runs this build together with the RTM and harness tests.

`SET` statements in `sliding.sql` are applied to the table configuration by `SlidingSql.applyIfSet`, because the Table API does not accept `SET` through `executeSql`.

The shaded jar contains the Kafka connector, which includes the `upsert-kafka` factory, and the service descriptors Flink needs. Flink, Table API, the planner loader and the runtime properties library are `provided`, because Managed Flink supplies them. `flink-connector-base` and `flink-json` are `test` dependencies only. The tests plan the real Kafka DDL, which needs them on the classpath. Managed Flink provides them at runtime.

`SlidingSqlTest` has three tests:

1. Defaults: state TTL 120000 ms and the topic names.
2. Plan shape: the full script's DDL is planned with the real Kafka and `upsert-kafka` tables (no broker needed), and the `INSERT` is explained with the changelog mode. It checks one `GroupAggregate` after one `Exchange` on `kafka_key`, no window operator, no materializer, no mini-batch, and changelog mode `[I,UA]`.
3. Result: the Kafka source is replaced by a bounded in-memory table with the same columns. The `INSERT` query runs as a `SELECT`, and its changelog is collected. It checks that there is one insert or update-after per input and window (three per input), and that counts grow with each update. It also checks that the final `count`, `sum_amount`, `window_start_ms`, `window_end_ms` and `in_ts` (maximum) of every group equal a brute-force oracle. Each event is in exactly three windows.

## Deploy

The `infra/terraform/modules/flink_app` module uploads the jar to the artifact bucket. It also creates the application, its execution role and its log group, and sets the runtime properties from the same names listed above. Set `start_application = true` only after the input topic exists and the MSK network path is in place. The `app` group should set `state.ttl.ms = 120000`, `input.topic` and `output.topic`.

## Runtime

| Item | Value |
|---|---|
| Runtime | Flink 2.3.0, `RuntimeEnvironment = FLINK-2_3` |
| Java | 17 |
| Kafka connector | `flink-connector-kafka` 4.0.1-2.0 |
| Runtime properties | `com.amazonaws:aws-kinesisanalytics-runtime` 1.2.0 |
