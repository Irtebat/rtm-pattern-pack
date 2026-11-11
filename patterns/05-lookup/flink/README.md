# 05 Lookup: Flink

This directory holds the Apache Flink implementation of the lookup pattern. The job reads events from `rtmpack.05-lookup.input`, enriches each event with the attributes of its merchant, and writes the result to `rtmpack.05-lookup.out.flink`. It runs as the Amazon Managed Service for Apache Flink application `rtmpack-flink-05-lookup` on `FLINK-2_3`. The input, the reference table rule and the output format are in sections 2 and 3 of [../../../docs/CONTRACT.md](../../../docs/CONTRACT.md), and in [../README.md](../README.md), Benchmark setup. The pattern overview is in [../README.md](../README.md). The Spark Real-Time Mode side is in [../rtm/README.md](../rtm/README.md).

## Layout

The Java package is `com.rtmpack.lookup`. The project follows the layout of the other Flink projects in the pack.

| Role | Purpose |
|---|---|
| `pom.xml` | Maven project, Java 17, shaded jar. |
| `sql/` and `src/main/resources/` | Two copies of the lookup SQL: a readable copy for docs, and the one the job executes. The build does not check that they match, so compare them with `cmp` after any edit. |
| Job entry point class | Renders and runs the SQL. |
| SQL loader class | Loads, renders and splits the script. |
| Job configuration class | Parameter resolution. |
| Merchant lookup class | The table function that holds the merchant map. |
| `src/test/resources/` | A 100-row fixture of the reference table for the unit tests. |
| SQL test class | Runs the lookup statements on bounded input. |

The reference table itself is `patterns/05-lookup/data/merchants.csv`. The harness generates it. It is not part of this directory.

## Query

```sql
CREATE TEMPORARY SYSTEM FUNCTION lookup_merchant AS 'com.rtmpack.lookup.MerchantLookup' LANGUAGE JAVA;

INSERT INTO lookup_output
SELECT
  e.event_id AS kafka_key,
  e.event_id, e.`key`, e.event_ts, e.merchant_id,
  m.risk_tier, m.category, m.country,
  <epoch-ms conversion of e.in_ts_ltz> AS in_ts,
  'flink' AS engine
FROM lookup_input AS e
CROSS JOIN LATERAL TABLE(lookup_merchant(e.merchant_id)) AS m(risk_tier, category, country);
```

The source table reads JSON over SASL_SSL with SCRAM-SHA-512. The Kafka broker timestamp is read with `METADATA FROM 'timestamp'` into `in_ts_ltz`. `scan.startup.mode = 'latest-offset'` starts a fresh run at the end of the topic. `in_ts` is the epoch-millisecond value of that timestamp, with `table.local-time-zone` pinned to `UTC`. The fields `seq`, `is_dup` and `payload` are declared so the schema matches the input. They are not copied to the output.

### Sink key and value

The Kafka key is `event_id`. The sink table declares a physical STRING column `kafka_key`, listed in `key.fields`, with `key.format = 'raw'`. `value.fields-include = 'EXCEPT_KEY'` keeps it out of the JSON value. The `event_id` column stays in the value, because it is a separate column. The value therefore contains exactly the contract fields: `event_id`, `key`, `event_ts`, `merchant_id`, `risk_tier`, `category`, `country`, `in_ts` and `engine`.

## The lookup function

The merchant lookup class is a Java `TableFunction<Row>` with output type `ROW<risk_tier STRING, category STRING, country STRING>`. Its `open()` method reads `/merchants.csv` from the classpath, which is the jar. It fills a `HashMap<String, String[]>` keyed by `merchant_id`. Each parallel task reads the file once, so every task holds the full table. That is about 10,000 entries, a few megabytes on the heap. `eval(String)` emits one row for a known id, and nothing for an unknown or null id.

### Why a table function and not a scalar function

Both forms keep the plan free of an exchange. The table function was chosen for two reasons.

Inner semantics come without a filter. `CROSS JOIN LATERAL` with a function that emits no row for an unknown id drops that event. A scalar function returning `ROW<...>` would return null for unknown ids. The query would then need a `WHERE m IS NOT NULL` filter on a subquery.

The lookup runs once per record. The planner may inline a scalar call into both the filter and the projections, which can evaluate the lookup more than once per record. A table function is called exactly once per input row.

The plan is the Kafka source, a correlate that calls the function, a calc, and the Kafka sink. The unit test asserts that it contains a `Correlate` node and no `Exchange`.

### How the table gets into the jar

The pom lists `../data/merchants.csv` as an extra resource directory. The file is copied to `target/classes/merchants.csv` and shaded into the jar as `/merchants.csv`. If the file is missing, `mvn package` fails in the `prepare-package` phase. The error says that `patterns/05-lookup/data/merchants.csv` is missing. The check runs after the tests, so the unit tests do not need the file.

In the unit tests, `src/test/resources/merchants.csv` has the same name. It comes earlier on the test classpath than the packaged file, so the tests use the 100-row fixture. The fixture was generated from the contract rule, for merchants `m-00000` to `m-00099`.

## No shuffle

The statement has no `GROUP BY`, keyed join, window or deduplication. The lookup is a local map access, so no operator needs records partitioned by key. Flink chains the source, correlate, calc and sink into one task per parallel instance. A record moves from the source to the sink inside one thread, without serialization and without a network buffer. So `execution.buffer-timeout.interval` adds no latency here. The unit test asserts that the plan for the INSERT contains no `Exchange` node. The only buffering left is the Kafka producer batch, which is why the sink sets `linger.ms = 0`.

## Configuration

Parameters come from three sources. Managed Flink runtime properties take precedence, then command-line arguments, then environment variables, then defaults.

| Managed Flink property | Flat name | Argument or env var | Required | Default |
|---|---|---|---|---|
| group `kafka`, `bootstrap.servers` | `kafka.bootstrap.servers` | `--kafka.bootstrap.servers` / `RTMPACK_KAFKA_BOOTSTRAP_SERVERS` | yes | none |
| group `kafka`, `username` | `kafka.username` | `--kafka.username` / `RTMPACK_KAFKA_USERNAME` | yes | none |
| group `kafka`, `password` | `kafka.password` | `--kafka.password` / `RTMPACK_KAFKA_PASSWORD` | yes | none |
| group `app`, `input.topic` | `app.input.topic` | `--app.input.topic` / `RTMPACK_APP_INPUT_TOPIC` | no | `rtmpack.05-lookup.input` |
| group `app`, `output.topic` | `app.output.topic` | `--app.output.topic` / `RTMPACK_APP_OUTPUT_TOPIC` | no | `rtmpack.05-lookup.out.flink` |

The pattern is stateless, so there is no state TTL. If the Terraform module passes `state.ttl.ms` in the `app` group, the job accepts it and ignores it. There is no setting for the reference table. It is part of the jar.

The password is never logged. The job builds the SASL JAAS string in code from the username and password, and escapes single quotes in values for SQL.

## Configuration choices

The sink is at-least-once: `sink.delivery-guarantee = 'at-least-once'`. This matches the Kafka sink of the Spark Real-Time Mode side. Repeated records carry the same `event_id` key.

The producer linger is `'properties.linger.ms' = '0'`. Section 8 of the contract requires it for both engines.

Checkpoints run every 60 seconds. The Terraform module sets this. There is no keyed state, so checkpoints only record the source offsets. The lookup map is rebuilt from the jar on every start, and it is not checkpointed.

A fresh run starts at `latest-offset`.

The reference table is fixed at build time. Changing it requires a new jar and an application update. The RTM job reads the file from a volume when the query starts.

The JSON format uses its default behavior, so a record that does not parse fails the job. A malformed reference file also fails the job, in `open()`. The error message includes the offending line.

## Parallelism and KPUs

Parallelism equals the partition count (default 8) with `ParallelismPerKPU = 1`, which gives 8 KPUs plus 1 orchestration KPU. Autoscaling is off. Each task keeps its own copy of the table. Heap use therefore scales with parallelism, but at about 10,000 rows it is small.

## Build and test

Requirements: JDK 17 and Maven 3.9 or later. From the repository root:

```bash
infra/scripts/build-flink.sh 05-lookup   # compiles, runs the SQL test, writes the shaded jar under patterns/05-lookup/flink/target/
```

`build-flink.sh` downloads from Maven Central, or from whatever your own `~/.m2/settings.xml` configures. To route downloads through a mirror, set `RTMPACK_MAVEN_MIRROR`. To use another settings file, set `RTMPACK_MAVEN_SETTINGS`. Generate `patterns/05-lookup/data/merchants.csv` first, or packaging fails as described above. To run all local tests for the pattern (RTM, Flink and harness), use `infra/scripts/test-pattern.sh 05-lookup`.

The shaded jar contains the Kafka connector, the service descriptors Flink needs, the lookup class and the reference table. Flink, the Table API, the planner loader and the runtime properties library are `provided`. The surefire configuration excludes `flink-table-planner-loader` from the test classpath, because the tests use the full planner. `flink-connector-base` and `flink-json` are test-scope only, for sink planning.

The SQL test runs the statements from the lookup SQL on a bounded in-memory source. The source Kafka DDL is replaced by a view with the same columns. The function and sink DDL are executed, and the INSERT is planned against them. The test checks:

- known merchants get the `risk_tier`, `category` and `country` of the fixture rows
- events with an unknown or null `merchant_id` produce no output
- the Kafka key equals `event_id`, and `engine` is `flink`
- `in_ts` is the epoch-millisecond value of the input timestamp
- the plan has a `Correlate` node and no `Exchange`
- the fixture loads completely (100 entries)
- the sink options contain `key.fields`, `EXCEPT_KEY`, `linger.ms = 0` and the at-least-once setting

## Deploy

The `infra/terraform/modules/flink_app` module uploads the jar and creates the application. It sets the runtime properties listed above. Set `start_application = true` only after the input topic exists and the MSK network path is in place.
