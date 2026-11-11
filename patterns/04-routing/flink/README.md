# 04 Routing: Flink

This directory holds the Apache Flink implementation of the 04-routing pattern, which covers demux by type and fan-out to a second consumer, for the RTM Pattern Pack. The job reads events from `rtmpack.04-routing.input` and writes each event to its type topic and to an all-events topic. It runs as an Amazon Managed Service for Apache Flink application on `FLINK-2_3`. The formats are defined in sections 2 and 3 of [docs/CONTRACT.md](../../../docs/CONTRACT.md).

The project follows the layout and conventions of [../../02-unnest/flink/README.md](../../02-unnest/flink/README.md).

## Layout

```
patterns/04-routing/flink/
  pom.xml                       Maven project, Java 17, shaded jar
  sql/                          readable copy of the SQL, for docs
  src/main/resources/           the SQL the job executes
  src/main/java/com/rtmpack/routing/
    job entry point             renders the SQL, runs one statement set
    SQL loader                  loads, renders and splits the script
    job configuration           parameter resolution
  src/test/java/com/rtmpack/routing/
    SQL test                    runs the routing statements on bounded input
```

The build does not check that the readable SQL under `sql/` matches the copy under `src/main/resources/`. Compare the two with `cmp` after any edit.

## Topics

`app.output.topic` is the base. By default it is `rtmpack.04-routing.out.flink`.

| Sink table | Topic | Filter |
|---|---|---|
| `routing_out_payment` | `<base>.payment` | `type = 'payment'` |
| `routing_out_order` | `<base>.order` | `type = 'order'` |
| `routing_out_click` | `<base>.click` | `type = 'click'` |
| `routing_out_login` | `<base>.login` | `type = 'login'` |
| `routing_out_all` | `<base>.all` | none |

All five topics must exist before the job starts. The Kafka key is `event_id`. The value has `event_id`, `key`, `seq`, `event_ts`, `is_dup`, `type`, `in_ts` and `engine="flink"`. The `payload` field is read but not written.

## Query

There is one source table, `routing_input`, and five sink tables with the same columns. The script has five `INSERT` statements over the source:

```sql
INSERT INTO routing_out_payment
SELECT event_id AS kafka_key, event_id, `key`, seq, event_ts, is_dup, `type`,
       <epoch-ms conversion of in_ts_ltz> AS in_ts, 'flink' AS engine
FROM routing_input
WHERE `type` = 'payment';
-- the same for order, click and login, and without the WHERE clause for routing_out_all
```

The job runs the DDL and `SET` statements in order. It adds the five `INSERT` statements to one `StatementSet` with `addInsertSql`, then runs it with `execute()`. This is the Table API form of `EXECUTE STATEMENT SET BEGIN ... END`. The semicolons inside that block would break the script splitter, so the job groups the inserts itself. The result is one Flink job with one source and five sinks.

The source reads JSON over SASL_SSL with SCRAM-SHA-512. The Kafka broker timestamp is read with `METADATA FROM 'timestamp'` into `in_ts_ltz`. `scan.startup.mode = 'latest-offset'` starts a fresh run at the end of the topic. `in_ts` uses the same conversion as the 01-dedup and 02-unnest patterns, with `table.local-time-zone` pinned to `UTC`. Both copies of an event carry the same `in_ts`.

### Sink key and value

Each sink declares a physical STRING column `kafka_key`, listed in `key.fields` with `key.format = 'raw'`. `value.fields-include = 'EXCEPT_KEY'` removes it from the value. `event_id` is a separate column, so it stays in the value. The customer `key` column is not the Kafka record key, so it stays in the value too.

## No shuffle

Each `INSERT` is a filter and a projection over the source. The planner reads the source once, so the optimized plan has a single `TableSourceScan` feeding five calc and sink chains. No operator needs records partitioned by key, so the plan has no `Exchange`. The unit test asserts both.

The source operator forwards each record to the five chained calc and sink operators in the same thread. A record that matches a type filter is written twice: once to its type topic, and once to the all-events topic. A record that matches no type filter is written once, to the all-events topic.

## Configuration

Parameters come from three sources. Managed Flink runtime properties take precedence, then command-line arguments, then environment variables, then defaults.

| Managed Flink property | Flat name | Argument or env var | Required | Default |
|---|---|---|---|---|
| group `kafka`, `bootstrap.servers` | `kafka.bootstrap.servers` | `--kafka.bootstrap.servers` / `RTMPACK_KAFKA_BOOTSTRAP_SERVERS` | yes | none |
| group `kafka`, `username` | `kafka.username` | `--kafka.username` / `RTMPACK_KAFKA_USERNAME` | yes | none |
| group `kafka`, `password` | `kafka.password` | `--kafka.password` / `RTMPACK_KAFKA_PASSWORD` | yes | none |
| group `app`, `input.topic` | `app.input.topic` | `--app.input.topic` / `RTMPACK_APP_INPUT_TOPIC` | no | `rtmpack.04-routing.input` |
| group `app`, `output.topic` | `app.output.topic` | `--app.output.topic` / `RTMPACK_APP_OUTPUT_TOPIC` | no | `rtmpack.04-routing.out.flink` |

The pattern is stateless, so there is no state TTL. If the Terraform module passes `state.ttl.ms` in the `app` group, the job accepts it and ignores it.

The password is never logged. The job builds the SASL JAAS string in code from the username and password, and escapes single quotes in values for SQL.

## Configuration choices

The sinks are at-least-once: `sink.delivery-guarantee = 'at-least-once'` on all five sinks. This matches the Kafka sink of the Spark Real-Time Mode side.

The producer linger is `'properties.linger.ms' = '0'` on all five sinks. The benchmark profile requires this for both engines (section 8 of the contract). Each sink has its own producer.

Checkpoints run every 60 seconds. The Terraform module sets this. There is no keyed state, so checkpoints only record the source offsets.

A fresh run starts at `latest-offset`. After a restore from a snapshot, the saved offsets are used.

The network buffer timeout is not changed. Managed Flink does not allow application code to change it, and the chained plan has no exchange that it would affect.

The JSON format uses its default behavior, so a record that does not parse fails the job.

## Parallelism and KPUs

Parallelism equals the partition count (default 8) with `ParallelismPerKPU = 1`, which gives 8 KPUs plus 1 orchestration KPU. Autoscaling is off. The output rate is twice the input rate, and each task holds five Kafka producers.

## Build and test

Requirements: JDK 17 and Maven 3.9 or later.

```sh
export JAVA_HOME=<path to a JDK 17>
infra/scripts/build-flink.sh 04-routing   # compiles, runs the unit tests, writes the shaded jar under patterns/04-routing/flink/target/
```

The shaded jar contains the Kafka connector and the service descriptors Flink needs. Flink, the Table API, the planner loader and the runtime properties library are `provided`. The surefire configuration excludes `flink-table-planner-loader` from the test classpath, because the tests use the full planner.

The SQL test runs the statements from `routing.sql` on a bounded in-memory source. The source Kafka DDL is replaced by a view with the same columns. The five sink DDL statements are executed, which needs no broker, and the five `INSERT` statements are planned together as one statement set. The test checks:

- the script has five sink tables and five `INSERT` statements
- the statement set plan has no `Exchange`, writes to all five sinks, and has one source scan
- with 4 payment, 3 order, 2 click and 1 login events, the type tables receive 4, 3, 2 and 1 rows, the all table receives 10, and the total is twice the input
- every row in a type table has that type, the key equals `event_id`, there is no `payload`, and `in_ts` is the epoch milliseconds of the input timestamp
- the sink topics are `<base>.<type>` and `<base>.all`, and the default topics follow the contract
- the sink options contain `key.fields`, `EXCEPT_KEY`, `linger.ms = 0` and the at-least-once setting on all five sinks

## Deploy

The `infra/terraform/modules/flink_app` module uploads the jar and creates the application. It sets the runtime properties listed above. Set `start_application = true` only after the input topic and all five output topics exist and the MSK network path is in place.
