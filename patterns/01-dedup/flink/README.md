# 01 Deduplication: Flink

This directory holds the Apache Flink implementation of pattern 01-dedup for the RTM Pattern Pack. The job reads the input topic `rtmpack.01-dedup.input`, keeps the first record for each `event_id`, and writes the result to `rtmpack.01-dedup.out.flink`. It runs as an Amazon Managed Service for Apache Flink application named `rtmpack-flink-01-dedup`. The pattern is described in [../README.md](../README.md). The Spark Real-Time Mode side is in [../rtm/README.md](../rtm/README.md). The benchmark contract is [docs/CONTRACT.md](../../../docs/CONTRACT.md).

## Layout

```
patterns/01-dedup/flink/
  pom.xml                                   Maven project, Java 17, shaded jar
  sql/dedup.sql                             readable copy of the SQL, for docs
  src/main/resources/dedup.sql              the SQL the job executes
  src/main/java/com/rtmpack/dedup/
    DedupJob.java                           entry point; renders and runs the SQL
    DedupSql.java                           loads, renders and splits the script
    JobConfig.java                          parameter resolution
  src/test/java/com/rtmpack/dedup/
    DedupSqlTest.java                       runs the dedup statements on bounded input
```

The build does not check that `sql/dedup.sql` matches `src/main/resources/dedup.sql`. Compare the two with `cmp` after any edit.

## How it works

The job is a Table API application. It defines two Kafka tables in Flink SQL and runs one `INSERT` statement:

1. The source table reads JSON records from `rtmpack.01-dedup.input` over SASL_SSL with SCRAM-SHA-512. The Kafka broker append timestamp is read with `METADATA FROM 'timestamp'` into `in_ts_ltz`. The column `proc_time AS PROCTIME()` adds a processing-time attribute.
2. The statement deduplicates with `ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY proc_time ASC) = 1`. Ordering by processing time, ascending, keeps the first arrival of each `event_id`. Because this is first-row deduplication, the output is append-only.
3. The sink table writes JSON to `rtmpack.01-dedup.out.flink`. The Kafka record key is `event_id` (raw format). The value holds the input fields, `in_ts` and `engine = 'flink'`. `in_ts` is the input record's broker timestamp in epoch milliseconds, computed in UTC.

The script sets three session options at the top. `pipeline.name` names the job. `table.local-time-zone` is pinned to UTC, so the timestamp conversion does not depend on the host. `table.exec.state.ttl` sets the state horizon.

`DedupJob` splits the script into statements. It runs the SET and CREATE statements in order, then waits on the INSERT. Statements are split on semicolons outside single-quoted literals. Whole-line `--` comments are removed first. The Table API does not accept `SET` through `executeSql`, so `DedupSql.applyIfSet` applies the `SET` statements to the table configuration.

## Configuration

Parameters come from three sources. Managed Flink runtime properties take precedence, then command-line arguments, then environment variables, then defaults.

| Managed Flink property | Flat name | Argument or env var | Required | Default |
|---|---|---|---|---|
| group `kafka`, `bootstrap.servers` | `kafka.bootstrap.servers` | `--kafka.bootstrap.servers` / `RTMPACK_KAFKA_BOOTSTRAP_SERVERS` | yes | none |
| group `kafka`, `username` | `kafka.username` | `--kafka.username` / `RTMPACK_KAFKA_USERNAME` | yes | none |
| group `kafka`, `password` | `kafka.password` | `--kafka.password` / `RTMPACK_KAFKA_PASSWORD` | yes | none |
| group `app`, `state.ttl.ms` | `app.state.ttl.ms` | `--app.state.ttl.ms` / `RTMPACK_APP_STATE_TTL_MS` | no | `60000` |
| group `app`, `input.topic` | `app.input.topic` | `--app.input.topic` / `RTMPACK_APP_INPUT_TOPIC` | no | `rtmpack.01-dedup.input` |
| group `app`, `output.topic` | `app.output.topic` | `--app.output.topic` / `RTMPACK_APP_OUTPUT_TOPIC` | no | `rtmpack.01-dedup.out.flink` |

Outside Managed Flink, `KinesisAnalyticsRuntime` finds no property file, so the job uses arguments and environment variables. The password is never logged. The job builds the SASL JAAS string in code from the username and password, and escapes single quotes in values for SQL. The SASL mechanism and protocol (`SCRAM-SHA-512` over `SASL_SSL`) are fixed in the SQL. They are not runtime properties.

## Configuration choices

The sink is at-least-once: `sink.delivery-guarantee = 'at-least-once'`. This matches RTM, whose Kafka sink is also at-least-once. After a failure and restore, the job can write a record again. The collector reports these as `sink_duplicates`, separate from `missing`.

The state horizon is 60 seconds. It must be longer than the maximum duplicate delay of 30 seconds, so a duplicate always finds the state of its original. `table.exec.state.ttl` is set from `app.state.ttl.ms`. RTM uses `withWatermark("event_time", "60 seconds")` with `dropDuplicates(["event_id", "event_time"])`, so both engines use the same horizon. If the TTL is shorter than the duplicate delay, late duplicates are emitted again and count as `missing` in the correctness check. Do not lower the TTL without updating the contract.

Checkpoints run every 60 seconds. The Terraform module sets this. Snapshots are enabled, so the application can be restored from its last snapshot after a stop. Checkpoints protect the state of the dedup operator.

The source uses `scan.startup.mode = 'latest-offset'`. A fresh run starts at the current end of the topic. After a restore from a snapshot, the saved offsets are used.

This pipeline shuffles by `event_id`. Keyed pipelines flush network buffers between operators every `execution.buffer-timeout.interval`, which defaults to 100 ms. The setting is not on the [list of properties customers can modify](https://docs.aws.amazon.com/managed-flink/latest/java/reference-modifiable-settings.html). Application code cannot change it, and a change needs an AWS support case. The pack runs Managed Flink as delivered. Self-managed Flink can lower this setting.

The JSON format uses its default behavior, so a record that does not parse fails the job. This keeps a benchmark run from silently dropping events.

## Parallelism and KPUs

Managed Flink allocates KPUs as `Parallelism / ParallelismPerKPU`, and bills one more KPU for orchestration. Each KPU gives 1 vCPU and 4 GB of memory.

The benchmark sets parallelism equal to the partition count, which defaults to 8, with `ParallelismPerKPU = 1`. The job gets 8 KPUs, plus 1 orchestration KPU, for 9 KPUs in total. The Terraform module turns autoscaling off, so parallelism stays fixed for the run.

## Runtime and pricing

| Item | Value | Source |
|---|---|---|
| Runtime | Flink 2.3.0, `RuntimeEnvironment = FLINK-2_3` | [release versions](https://docs.aws.amazon.com/managed-flink/latest/java/release-version-list.html), [Flink 2.3](https://docs.aws.amazon.com/managed-flink/latest/java/flink-2-3.html) |
| Java | 17 (recommended for Flink 2.3) | Flink 2.3 page |
| Kafka connector | `flink-connector-kafka` 4.0.1-2.0 | [connectors](https://docs.aws.amazon.com/managed-flink/latest/java/how-flink-connectors.html) |
| Runtime properties | `com.amazonaws:aws-kinesisanalytics-runtime` 1.2.0, `KinesisAnalyticsRuntime.getApplicationProperties()` | [runtime properties](https://docs.aws.amazon.com/managed-flink/latest/java/how-properties.html) |
| KPU price, us-west-2 | $0.11 per KPU-hour | AWS price list, [pricing](https://docs.aws.amazon.com/managed-flink/latest/java/how-pricing.html) |
| Billing | per second, 10-minute minimum per application start | pricing page |

At 9 KPUs, the compute rate is 9 x $0.11 = $0.99 per hour, plus running storage.

## Build and test

Requirements: JDK 17 and Maven 3.9 or later.

```sh
export JAVA_HOME=<path to a JDK 17>
infra/scripts/build-flink.sh 01-dedup   # compiles, runs DedupSqlTest, writes the shaded jar under patterns/01-dedup/flink/target/
```

`build-flink.sh` downloads from Maven Central, or from whatever your own `~/.m2/settings.xml` configures. To route downloads through a mirror, set `RTMPACK_MAVEN_MIRROR`. To use another settings file, set `RTMPACK_MAVEN_SETTINGS`. For the full local test run for the pattern, use `infra/scripts/test-pattern.sh 01-dedup`.

The shaded jar contains the Kafka connector and the service descriptors Flink needs. Flink, the Table API, the planner loader and the runtime properties library are `provided`, because Managed Flink supplies them.

`DedupSqlTest` runs the statements from `dedup.sql` on a bounded in-memory source. The two Kafka DDL statements are replaced by a view with the same columns, so the test needs no broker. It checks one output row per `event_id`, the `engine` value, and the epoch-millisecond `in_ts` conversion.

Local run against a broker, with the provided jars on the classpath:

```sh
mvn -q dependency:build-classpath -Dmdep.outputFile=target/classpath.txt
java -cp "target/<built jar>.jar:$(cat target/classpath.txt)" com.rtmpack.dedup.DedupJob \
  --kafka.bootstrap.servers b-1.example:9096 \
  --kafka.username rtmpack --kafka.password "$RTMPACK_SECRET"
```

## Deploy

The `infra/terraform/modules/flink_app` module uploads the jar to the artifact bucket. It also creates the application, its execution role and its log group, and sets the runtime properties listed above. The application references the S3 object version of the jar, so a rebuilt jar updates the application. The execution role needs `s3:GetObjectVersion`. Set `start_application = true` only after the input topic exists and the MSK network path is in place. See [infra/README.md](../../../infra/README.md).
