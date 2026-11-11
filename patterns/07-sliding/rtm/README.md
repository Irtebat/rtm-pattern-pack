# 07 Sliding: Spark Real-Time Mode

This directory holds the Spark Real-Time Mode (RTM) side of the sliding window pattern. The job reads the input topic from Amazon MSK. It computes `count`, `sum(amount)` and `max(in_ts)` per key and per 60-second window that slides every 20 seconds, on event time. It writes the running aggregate to the RTM output topic. The pattern is described in [../README.md](../README.md). The Flink side is in [../flink/README.md](../flink/README.md).

| Path | Purpose |
|---|---|
| `sliding_rtm.py` | PySpark job. The transformation `aggregate_sliding` is a pure function. |
| `tests/test_sliding_rtm.py` | Local pytest suite. Runs the transformation in batch mode and as a finite streaming query. |

## How the job works

1. The job reads `rtmpack.07-sliding.input` with `startingOffsets=latest`. It uses `SASL_SSL` with `SCRAM-SHA-512`. Credentials and the bootstrap address come from the secret scope `rtmpack` (keys `kafka-bootstrap`, `kafka-username`, `kafka-password`).
2. The Kafka value is parsed as the input event (`event_id`, `key`, `seq`, `event_ts`, `amount`, `payload`). `event_ts` is epoch milliseconds and becomes the Spark timestamp `event_time`. Rows without a `key` or a valid `event_ts` are dropped.
3. `in_ts` is the Kafka record `timestamp` (broker LogAppendTime), converted to epoch milliseconds. For an aggregate, the output `in_ts` is the `max(in_ts)` of the inputs in the window so far (contract section 9).
4. The aggregation runs on `event_time` with a 10-second watermark:

```python
parsed.withWatermark("event_time", "10 seconds")
      .groupBy(window(event_time, "60 seconds", "20 seconds"), key)
      .agg(count(1) AS count, sum(amount) AS sum_amount, max(in_ts) AS in_ts)
```

   A 60-second window with a 20-second slide assigns each event to exactly three windows. One input therefore changes three groups.
5. The output record has Kafka key `<key>:<window_start_ms>` and a JSON value with `key`, `window_start_ms`, `window_end_ms`, `count`, `sum_amount`, `in_ts` and `engine="rtm"`. It is written to `rtmpack.07-sliding.out.rtm` in update output mode, with the real-time trigger `trigger(realTime=<--checkpoint-interval>)`.

Windows are aligned to the epoch. `window_start_ms` and `window_end_ms` are `window.start` and `window.end` in epoch milliseconds. They match the Flink side exactly.

The collector reports the number of updates it reads. This lets you compare the update rate with the input rate.

## Arguments

The parameters are command-line arguments of the Python task.

| Argument | Default | Meaning |
|---|---|---|
| `--secret-scope` | `rtmpack` | Databricks secret scope that holds the MSK credentials. |
| `--input-topic` | `rtmpack.07-sliding.input` | Input topic. |
| `--output-topic` | `rtmpack.07-sliding.out.rtm` | Output topic. |
| `--partitions` | `4` | Sets `spark.sql.shuffle.partitions`, which is the number of aggregation tasks. The input topic has 8 partitions, which sets the number of source tasks. |
| `--state-horizon` | `10 seconds` | Watermark delay. It limits window state and sets how late an event can arrive and still be counted. |
| `--checkpoint-interval` | `60` (seconds) | Real-time trigger interval. Progress is checkpointed at this interval. A plain number means seconds. Any Spark interval string also works. The benchmark passes `300 seconds`. |
| `--kafka-linger-ms` | unset | Kafka producer `linger.ms` for the sink. Unset keeps the connector default. The benchmark passes `0`. |
| `--checkpoint-location` | none (required) | Checkpoint path, such as `/Volumes/<catalog>/<schema>/<volume>/checkpoints/07-sliding-rtm`. |

## Cluster configuration

- Dedicated access mode, Photon off, autoscaling off, on-demand instances.
- `spark.databricks.streaming.realTimeMode.enabled=true`.
- DBR 18 LTS (`18.x-scala2.13`).
- `m5.xlarge` workers with four slots each.

RTM schedules every stage at once. The total task count must not exceed the available slots. This query has two stages. The source stage has one task per Kafka input partition, which is 8 tasks. The shuffle stage for the aggregation on window and `key` uses `spark.sql.shuffle.partitions`, which is 4 tasks. That is 12 tasks. Four `m5.xlarge` workers give 16 slots, so 75 percent are used. With `--partitions 8`, the job would need 16 slots on 4 workers (100 percent), or 5 workers.

## Mapping to the Flink aggregation

| Concern | Flink | RTM (this job) |
|---|---|---|
| Window assignment | `CROSS JOIN UNNEST(ARRAY[ws, ws - 20000, ws - 40000])` with `ws = event_ts - MOD(event_ts, 20000)` | `window(event_time, "60 seconds", "20 seconds")` |
| Group | `key`, `window_start` | `key`, `window` |
| Aggregates | `COUNT(*)`, `SUM(amount)`, `MAX(in_ts)` | `count`, `sum(amount)`, `max(in_ts)` |
| Emission | An update for every input (regular aggregation, `upsert-kafka` sink) | Update output mode |
| State lifetime | `table.exec.state.ttl` = 120 s, from the last update of a group | Watermark of 10 s behind the maximum `event_time` seen |
| Late events | Always counted, because there is no watermark | Dropped when older than the watermark |
| Output key and value | Kafka key `<key>:<window_start_ms>`, JSON | Kafka key `<key>:<window_start_ms>`, JSON |
| `engine` | `flink` | `rtm` |
| Delivery | at-least-once (`upsert-kafka`) | at-least-once (the Kafka sink cannot be exactly-once in RTM) |

The two engines evict state differently. The Flink TTL depends on processing time since the last update. The RTM watermark depends on event time. An idle or skewed partition can hold the watermark back, which keeps window state longer.

## Running the local tests

The tests need Java 17 and PySpark 4.1, the Spark version of DBR 18. Install `patterns/requirements-test.txt` into a Python environment once. Set `RTMPACK_TEST_PYTHON` to that environment's interpreter. The script uses it for the Spark driver and the workers, so their versions match. Run these commands from the repository root:

```sh
python3 -m venv /tmp/rtm-venv && /tmp/rtm-venv/bin/pip install -r patterns/requirements-test.txt   # once
RTMPACK_TEST_PYTHON=/tmp/rtm-venv/bin/python infra/scripts/test-pattern.sh 07-sliding
```

This runs the RTM tests, the Flink build and tests, and the harness tests. To run only the RTM tests, add `--skip-flink --skip-harness`.

The tests compare every output against a brute-force oracle. The oracle covers counts, sums, `max(in_ts)`, window start and end, and key format. The tests check the JSON fields and `engine="rtm"`. They check that every event lands in exactly three windows: starts `ws`, `ws - 20 s` and `ws - 40 s`, each with `start <= event_ts < start + 60 s`. They also cover the slide-boundary cases, the dropping of rows without a key or event time, a finite streaming run of the watermarked query, the shaded SCRAM JAAS string, and the argument defaults.

The local tests run the same query in batch mode and as an `availableNow` streaming query, on open-source Spark. The real-time trigger, the Kafka source and the Kafka sink need Databricks, so the tests do not run them.
