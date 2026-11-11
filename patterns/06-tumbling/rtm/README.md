# 06 Tumbling: Spark Real-Time Mode

This directory holds the Spark Real-Time Mode (RTM) job for the tumbling window pattern. The job reads `rtmpack.06-tumbling.input` from Amazon MSK. It computes `count`, `sum(amount)` and `max(in_ts)` per key and per 60-second tumbling window, on event time. It writes the running aggregate to `rtmpack.06-tumbling.out.rtm`. The rules are in sections 6 and 9 of [../../../docs/CONTRACT.md](../../../docs/CONTRACT.md). The pattern overview is in [../README.md](../README.md). The Flink side is in [../flink/README.md](../flink/README.md).

| Role | Purpose |
|---|---|
| PySpark job (`*_rtm.py`) | The job. The transformation `aggregate_tumbling` is a pure function. |
| `tests/` | Local pytest suite. Runs the transformation in batch mode and as a finite streaming query. |

The Databricks job `rtmpack-06-tumbling-rtm` runs this job. The Terraform module `../../../infra/terraform/modules/databricks_rtm_job/` defines it.

## Data flow

1. The job reads `rtmpack.06-tumbling.input` with `startingOffsets=latest`. It uses `SASL_SSL` with `SCRAM-SHA-512`. Credentials and the bootstrap address come from the secret scope `rtmpack` (keys `kafka-bootstrap`, `kafka-username`, `kafka-password`).
2. The Kafka value is parsed as the contract input event (`event_id`, `key`, `seq`, `event_ts`, `amount`, `payload`). `event_ts` is epoch milliseconds and becomes the Spark timestamp `event_time`. Rows without a `key` or a valid `event_ts` are dropped.
3. `in_ts` is the Kafka record `timestamp` (broker LogAppendTime), converted to epoch milliseconds. Latency is measured against this value. For an aggregate, the output `in_ts` is the `max(in_ts)` of the inputs in the window so far.
4. The aggregation runs on `event_time` with a 10-second watermark:

   ```python
   parsed.withWatermark("event_time", "10 seconds")
         .groupBy(window(event_time, "60 seconds"), key)
         .agg(count(1) AS count, sum(amount) AS sum_amount, max(in_ts) AS in_ts)
   ```

   A tumbling window assigns each event to exactly one window.
5. The output record has Kafka key `<key>:<window_start_ms>` and a JSON value with `key`, `window_start_ms`, `window_end_ms`, `count`, `sum_amount`, `in_ts` and `engine="rtm"`. It is written to `rtmpack.06-tumbling.out.rtm` in update output mode, with the real-time trigger `trigger(realTime=<--checkpoint-interval>)`.

Windows are aligned to the epoch. `window_start_ms` and `window_end_ms` are `window.start` and `window.end` in epoch milliseconds. They match the Flink side exactly.

## Job arguments

The parameters are command-line arguments of the Python task.

| Argument | Default | Meaning |
|---|---|---|
| `--secret-scope` | `rtmpack` | Secret scope that holds the MSK credentials. The job definition never contains credentials. |
| `--input-topic` | `rtmpack.06-tumbling.input` | Input topic. |
| `--output-topic` | `rtmpack.06-tumbling.out.rtm` | Output topic. |
| `--partitions` | `4` | Sets `spark.sql.shuffle.partitions`, which is the number of aggregation tasks. The Kafka topic has 8 partitions, which sets the number of source tasks. |
| `--state-horizon` | `10 seconds` | Watermark delay. It limits window state and sets how late an event can arrive and still be counted. |
| `--checkpoint-interval` | `60` (seconds) | Real-time trigger interval. The tuned profile passes `5 minutes` (section 8 of the contract). A bare number is read as seconds. |
| `--kafka-linger-ms` | unset | Producer `linger.ms` for the sink. The tuned profile passes `0`. |
| `--checkpoint-location` | required | Checkpoint path on a Unity Catalog volume, such as `/Volumes/<catalog>/<schema>/<volume>/checkpoints/06-tumbling-rtm`. |

## Cluster configuration

The cluster uses dedicated access mode, Photon off, autoscaling off and on-demand instances. It sets `spark.databricks.streaming.realTimeMode.enabled=true` and runs on DBR 18 LTS (`18.x-scala2.13`). The workers are `m5.xlarge`, with four slots each. The other RTM jobs in the pack have the same requirements. See [../../01-dedup/rtm/README.md](../../01-dedup/rtm/README.md).

Extra cluster Spark settings for a single run can be passed with the `rtm_spark_conf_overrides` Terraform variable. See [../README.md](../README.md).

## Slot sizing

RTM schedules every stage at once. The total task count must not exceed the available slots. This query has two stages:

- Source stage: one task per Kafka input partition, 8 tasks.
- Shuffle stage for the aggregation on window and `key`: `spark.sql.shuffle.partitions` tasks, 4 tasks.

That is 12 tasks. Four `m5.xlarge` workers give 16 slots, so 75 percent are used. This is the limit in section 8 of the contract. The recommended settings are `--partitions 4` and 4 workers. Using 8 shuffle partitions would need 16 slots on 4 workers (100 percent), or 5 workers.

## Mapping to the Flink aggregation

| Concern | Flink | RTM (this job) |
|---|---|---|
| Window assignment | `window_start = event_ts - MOD(event_ts, 60000)` | `window(event_time, "60 seconds")` |
| Group | `key`, `window_start` | `key`, `window` |
| Aggregates | `COUNT(*)`, `SUM(amount)`, `MAX(in_ts)` | `count`, `sum(amount)`, `max(in_ts)` |
| Emission | An update for every input (regular aggregation, `upsert-kafka` sink) | Update output mode |
| State lifetime | `table.exec.state.ttl` = 120 s, from the last update of a group | Watermark of 10 s behind the maximum `event_time` seen |
| Late events | Always counted, since there is no watermark. They restart a group whose state has expired | Dropped when older than the watermark |
| Output key and value | Kafka key `<key>:<window_start_ms>`, JSON | Kafka key `<key>:<window_start_ms>`, JSON |
| `engine` | `flink` | `rtm` |
| Delivery | at-least-once (`upsert-kafka`) | at-least-once (the Kafka sink in RTM is at-least-once) |

The two engines evict state differently. The Flink TTL depends on processing time since the last update. The RTM watermark depends on event time.

## Run the tests

The tests need Java 17 and PySpark 4.1, the Spark version of DBR 18. Install `patterns/requirements-test.txt` into a Python environment once. Set `RTMPACK_TEST_PYTHON` to that environment's interpreter. The script uses it for the Spark driver and the workers, so their versions match. Run these commands from the repository root:

```sh
python3 -m venv /tmp/rtm-venv && /tmp/rtm-venv/bin/pip install -r patterns/requirements-test.txt   # once
RTMPACK_TEST_PYTHON=/tmp/rtm-venv/bin/python infra/scripts/test-pattern.sh 06-tumbling
```

This runs the RTM tests, the Flink build and tests, and the harness tests. To run only the RTM tests, add `--skip-flink --skip-harness`.

The tests compare every output against a brute-force oracle. The oracle computes counts, sums, `max(in_ts)`, window start and end, and key format. The tests check the contract JSON fields and `engine="rtm"`. Every event must be counted in exactly one window. Events on both sides of a window boundary must go to different windows. Rows without a key or event time must be dropped. The tests also run the watermarked query as a finite streaming query, and cover the SCRAM JAAS string and the argument defaults. They run on open-source Spark. They do not use the real-time trigger or the Kafka source and sink.
