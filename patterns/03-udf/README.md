# 03 UDF

## About the pattern

The input is one JSON event per Kafka record on `rtmpack.03-udf.input`. Each event has `event_id`, `key`, `seq`, `event_ts`, `is_dup` and `payload`. There are no duplicates.

The output is one record per input event, on `rtmpack.03-udf.out.<engine>`, where the engine is `rtm` or `flink`. The Kafka key is `event_id`. The value has `event_id`, `key`, `seq`, `event_ts`, `score`, `decision`, `in_ts` and `engine`. The `payload` field is dropped.

The rule is a scalar function that both engines implement the same way. The formula is in Benchmark setup.

For example, the event `{"event_id":"c9e9c616-612e-4696-a6ce-cc1b78e51061","key":"user-27519", ...}` produces `score = 975` and `decision = "flag"`. The test vectors in [score_vectors.csv](score_vectors.csv) list more pairs and cover all three decisions.

The query is stateless. Each record is scored on its own, with no shuffle and no state.

## Why sub-second processing matters for this pattern

Rule engines and scoring services decide per event (approve, flag, route) while a user or a transaction is waiting. A card authorization, a checkout step or a login check has a response deadline of a few hundred milliseconds in total. The scoring step gets only part of that budget. The decision has to leave the stream processor fast enough for the calling service to answer in time. These services read the decision from a Kafka topic and act on every record, so the output stays in Kafka.

If nobody acts on the decision within seconds, for example when the scores feed a daily report or a model training table, near-real-time Structured Streaming into Delta is the better fit. A Delta copy for analytics can also be written separately from the same topic.

## How each engine implements it

| Engine | Key construct | Details |
|---|---|---|
| Spark Real-Time Mode (RTM) | Arrow-optimized Python scalar UDF, `udf(..., useArrow=True)`, returning a struct of `score` and `decision` | [rtm/README.md](rtm/README.md) |
| Managed Service for Apache Flink | Java `ScalarFunction` registered with `createTemporarySystemFunction` and called from Flink SQL | [flink/README.md](flink/README.md) |

Each engine uses its usual UDF form. The constraints below are specific to each engine.

- On RTM, the Python function runs in a separate Python worker process, with Arrow serialization between the JVM and Python. On DBR 18 and below, the RTM reference recommends `spark.sql.execution.arrow.maxRecordsPerBatch = 1`, so a record is not held back while an Arrow batch fills. The job sets it by default. Unity Catalog UDFs are not supported in RTM. On DBR 19 and later, rows are batched by time instead. See the [RTM documentation](https://docs.databricks.com/aws/en/structured-streaming/real-time) for the current rules.
- The RTM variant can write the same function with built-in SQL functions (`sha2`, `substring`, `conv`, `pmod`). These run in the JVM and need no Python worker. This is the `--scoring builtin` option.
- On Flink, the function runs inside the Flink task thread. The query is a source, a calc and a sink, chained into one task, so there is no network shuffle.

## Benchmark setup

Shared rules (topics, event schema, latency, run parameters, tuned profiles, value check) are in sections 1 to 9 of [docs/CONTRACT.md](../../docs/CONTRACT.md). The rules below apply to this pattern.

| Item | Value |
|---|---|
| Input topic | `rtmpack.03-udf.input`, section 2 event, no duplicates |
| Input rate | 10,000 events per second |
| Record size | 1,024 bytes |
| Partitions | 8 (input and output topics) |
| Duplicates | none |
| Output topic | `rtmpack.03-udf.out.<engine>`, Kafka key `event_id`. Value: `event_id`, `key`, `seq`, `event_ts`, `score`, `decision`, `in_ts`, `engine`. No `payload` |
| Duration | 1,800 seconds, with the first 300 seconds excluded from latency statistics |
| Correctness rule | distinct `event_id`s as in section 6 (no missing records), plus `value_errors` for any output whose `score` or `decision` differs from the function. Value errors must be 0 |
| Latency | output record broker append time minus input record broker append time |
| Delivery | at-least-once on both sinks, producer `linger.ms = 0` |

The scoring function is identical in both engines:

```
h        = SHA-256(UTF-8(event_id + "|" + key))
score    = (first 4 bytes of h as an unsigned big-endian integer) mod 1000
decision = "flag" if score >= 950, "review" if score >= 800, else "approve"
```

RTM uses an Arrow-optimized Python scalar UDF with `spark.sql.execution.arrow.maxRecordsPerBatch = 1`, as the RTM reference recommends for DBR 18 and below. Flink uses a Java `ScalarFunction`. The section on how each engine implements it has the details.

RTM runs on DBR 18 LTS (`18.x-scala2.13`), Photon off, on 1 driver and 3 workers of m5.xlarge (12 slots). The query is one stage, so it runs 8 tasks, which is 67 percent of the slots. The trigger is `realTime="5 minutes"`.

Managed Flink runs `FLINK-2_3` with parallelism 8 and `ParallelismPerKPU = 1`, plus 1 orchestration KPU. It checkpoints every 60 seconds, and autoscaling is off.

## How to run

Local tests cover the RTM job, the Flink build and tests, and the harness. Nothing in the cloud is touched.

```bash
infra/scripts/test-pattern.sh 03-udf
```

The full benchmark run starts both engines on the same input topic:

```bash
infra/scripts/run-benchmark.sh --pattern 03-udf
```

The RTM job has two variant flags. Pass them with `rtm_extra_params`. Each variant gets its own checkpoint location, because a checkpoint cannot be shared between different queries. A repeated argument takes its last value, so the extra `--checkpoint-location` replaces the default one.

| Flag | Values | What it changes |
|---|---|---|
| `--scoring` | `arrow-udf` (default), `builtin` | `builtin` computes the same score and decision with built-in SQL functions instead of the Python UDF. |
| `--arrow-max-records-per-batch` | integer, default `1` | Sets `spark.sql.execution.arrow.maxRecordsPerBatch`. `0` leaves the runtime default, which is the setting to use on DBR 19 and later. |

To run the built-in functions variant:

```bash
infra/scripts/run-benchmark.sh --pattern 03-udf \
  --tf-var 'rtm_extra_params={"03-udf"=["--scoring","builtin","--checkpoint-location","/Volumes/<catalog>/<schema>/<volume>/checkpoints/03-udf-rtm-builtin-run1"]}'
```

To use the runtime default batching on a newer runtime, set both the job argument and the runtime override. Both variables are needed:

```bash
infra/scripts/run-benchmark.sh --pattern 03-udf \
  --tf-var 'rtm_spark_version_overrides={"03-udf"="19.x-scala2.13"}' \
  --tf-var 'rtm_extra_params={"03-udf"=["--arrow-max-records-per-batch","0","--checkpoint-location","/Volumes/<catalog>/<schema>/<volume>/checkpoints/03-udf-rtm-dbr19-run1"]}'
```

Replace the runtime string with one that is available in your workspace. The infra README describes all three override variables: `rtm_extra_params`, `rtm_spark_version_overrides` and `rtm_spark_conf_overrides` (see [../../infra/README.md](../../infra/README.md)).

