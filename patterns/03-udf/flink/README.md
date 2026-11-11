# 03 UDF: Flink

This directory holds the Apache Flink implementation of the 03-udf pattern for the RTM Pattern Pack. The job reads events from `rtmpack.03-udf.input`, scores each one with a Java scalar function, and writes the result to `rtmpack.03-udf.out.flink`. It runs as an Amazon Managed Service for Apache Flink application on `FLINK-2_3`. The scoring rule is in [../README.md](../README.md). The formats are defined in sections 2 and 3 of [docs/CONTRACT.md](../../../docs/CONTRACT.md).

The project follows the layout and conventions of [../../02-unnest/flink/README.md](../../02-unnest/flink/README.md).

## Layout

```
patterns/03-udf/
  score_vectors.csv             shared test vector, also used by the RTM tests
patterns/03-udf/flink/
  pom.xml                       Maven project, Java 17, shaded jar
  sql/                          readable copy of the SQL, for docs
  src/main/resources/           the SQL the job executes
  src/main/java/com/rtmpack/udf/
    job entry point             registers the function, renders and runs the SQL
    scalar function class       the scoring function
    SQL loader                  loads, renders and splits the script
    job configuration           parameter resolution
  src/test/java/com/rtmpack/udf/
    SQL test                    vector, thresholds and the SQL on bounded input
```

The build does not check that the readable SQL under `sql/` matches the copy under `src/main/resources/`. Compare the two with `cmp` after any edit.

## The function

The function class extends `ScalarFunction` and implements the rule in [../README.md](../README.md), Benchmark setup:

```
h        = SHA-256(UTF-8(event_id + "|" + key))
score    = (first 4 bytes of h as an unsigned big-endian integer) mod 1000
decision = "flag" if score >= 950, "review" if score >= 800, else "approve"
```

It returns one row, `ROW<score INT, decision STRING>`, so the SQL calls it once per record. A null argument gives a null result. The `MessageDigest` is created in `open()`, once per task.

The job registers the function with `tableEnv.createTemporarySystemFunction("score_event", <scoring function class>)` before any statement runs.

### Non-deterministic flag

`isDeterministic()` returns `false`, although the function is pure. The reason is the SQL, which reads two fields of the returned row. For a deterministic function, the planner inlines the call into both field accesses, so SHA-256 runs twice per record. A non-deterministic call is evaluated once, and its row is reused. The input is an append-only stream, so the flag has no other effect on the plan. A unit test asserts one call per record.

## Query

```sql
INSERT INTO udf_output
SELECT
  e.event_id AS kafka_key, e.event_id, e.`key`, e.seq, e.event_ts,
  e.r.score AS score, e.r.decision AS decision,
  <epoch-ms conversion of e.in_ts_ltz> AS in_ts,
  'flink' AS engine
FROM (
  SELECT event_id, `key`, seq, event_ts, in_ts_ltz, score_event(event_id, `key`) AS r
  FROM udf_input
) AS e;
```

The source table `udf_input` reads JSON over SASL_SSL with SCRAM-SHA-512. Its schema is `event_id`, `key`, `seq`, `event_ts`, `is_dup` and `payload`. The Kafka broker timestamp is read with `METADATA FROM 'timestamp'` into `in_ts_ltz`. `scan.startup.mode = 'latest-offset'` starts a fresh run at the end of the topic. The `payload` field is declared so the schema matches the input. It is not copied to the output, because the contract output fields do not include it.

`in_ts` uses the same conversion as the 01-dedup and 02-unnest patterns. The `TIMESTAMP_LTZ(3)` value is cast to a string, parsed with `UNIX_TIMESTAMP`, and the millisecond part is added back. `table.local-time-zone` is pinned to `UTC`, so the result does not depend on the host.

### Sink key and value

The Kafka key is `event_id`. The sink table declares a physical STRING column `kafka_key`, lists it in `key.fields`, and sets `key.format = 'raw'`. `value.fields-include = 'EXCEPT_KEY'` removes `kafka_key` from the value. `event_id` is a separate column, so it stays in the value. The JSON value contains exactly `event_id`, `key`, `seq`, `event_ts`, `score`, `decision`, `in_ts` and `engine`. The customer `key` column is not the Kafka record key, so it stays in the value too.

## No shuffle

The statement has no `GROUP BY`, key join, window or deduplication. The plan is the Kafka source, a calc that calls the function, and the Kafka sink. Flink chains these into one task per parallel instance. The unit test asserts that the plan for the INSERT contains no `Exchange` node. The function runs in the task thread, with no serialization between the source and the function.

## Configuration

Parameters come from three sources. Managed Flink runtime properties take precedence, then command-line arguments, then environment variables, then defaults.

| Managed Flink property | Flat name | Argument or env var | Required | Default |
|---|---|---|---|---|
| group `kafka`, `bootstrap.servers` | `kafka.bootstrap.servers` | `--kafka.bootstrap.servers` / `RTMPACK_KAFKA_BOOTSTRAP_SERVERS` | yes | none |
| group `kafka`, `username` | `kafka.username` | `--kafka.username` / `RTMPACK_KAFKA_USERNAME` | yes | none |
| group `kafka`, `password` | `kafka.password` | `--kafka.password` / `RTMPACK_KAFKA_PASSWORD` | yes | none |
| group `app`, `input.topic` | `app.input.topic` | `--app.input.topic` / `RTMPACK_APP_INPUT_TOPIC` | no | `rtmpack.03-udf.input` |
| group `app`, `output.topic` | `app.output.topic` | `--app.output.topic` / `RTMPACK_APP_OUTPUT_TOPIC` | no | `rtmpack.03-udf.out.flink` |

The pattern is stateless, so there is no state TTL. If the Terraform module passes `state.ttl.ms` in the `app` group, the job accepts it and ignores it.

The password is never logged. The job builds the SASL JAAS string in code from the username and password, and escapes single quotes in values for SQL.

## Configuration choices

The sink is at-least-once: `sink.delivery-guarantee = 'at-least-once'`. This matches the Kafka sink of the Spark Real-Time Mode side. After a failure and restore, the job can write some records again, and the collector reports them as `sink_duplicates`.

The producer linger is `'properties.linger.ms' = '0'`. The benchmark profile requires this for both engines (section 8 of the contract).

Checkpoints run every 60 seconds. The Terraform module sets this. There is no keyed state, so checkpoints only record the source offsets.

A fresh run starts at `latest-offset`. After a restore from a snapshot, the saved offsets are used.

The network buffer timeout is not changed. Managed Flink does not allow application code to change it, and the chained plan has no exchange that it would affect.

The JSON format uses its default behavior, so a record that does not parse fails the job. This keeps a benchmark run from silently dropping events.

## Parallelism and KPUs

Parallelism equals the partition count (default 8) with `ParallelismPerKPU = 1`, which gives 8 KPUs plus 1 orchestration KPU. Autoscaling is off.

## Build and test

Requirements: JDK 17 and Maven 3.9 or later.

```sh
export JAVA_HOME=<path to a JDK 17>
infra/scripts/build-flink.sh 03-udf   # compiles, runs the unit tests, writes the shaded jar under patterns/03-udf/flink/target/
```

The shaded jar contains the Kafka connector and the service descriptors Flink needs. Flink, the Table API, the planner loader and the runtime properties library are `provided`. The surefire configuration excludes `flink-table-planner-loader` from the test classpath, because the tests use the full planner.

The SQL test reads `../score_vectors.csv` relative to the module directory, which is the Maven working directory. It checks:

- the five vector rows give the expected score and decision, both from the function and through the SQL
- the vector covers all three decisions, and the thresholds 799/800 and 949/950 are classified correctly
- null arguments give null
- the function is registered the same way as in the job and runs once per record
- the output row has `kafka_key = event_id`, the contract fields, no `payload`, and `in_ts` equal to the epoch milliseconds of the input timestamp
- the plan has no `Exchange`
- the sink options contain `key.fields`, `EXCEPT_KEY`, `linger.ms = 0` and the at-least-once setting

## Deploy

The `infra/terraform/modules/flink_app` module uploads the jar and creates the application. It sets the runtime properties listed above. Set `start_application = true` only after the input topic exists and the MSK network path is in place.
