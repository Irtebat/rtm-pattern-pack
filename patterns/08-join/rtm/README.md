# 08 Join: Spark Real-Time Mode

This directory holds the Spark Real-Time Mode (RTM) side of the stream-stream join pattern. The job reads two input topics from Amazon MSK. It joins each payment to its order on `order_id`, when the payment happens within 30 seconds after the order. It writes the matches to the RTM output topic. The pattern is described in [../README.md](../README.md). The Flink side is in [../flink/README.md](../flink/README.md).

| Path | Purpose |
|---|---|
| `join_rtm.py` | PySpark job. The transformation `join_orders_payments` is a pure function. |
| `tests/test_join_rtm.py` | Local pytest suite. Runs the transformation in batch mode and as a finite streaming query. |

## How the job works

1. The job reads `<input-topic>.orders` and `<input-topic>.payments` (default base `rtmpack.08-join.input`) with `startingOffsets=latest`. It uses `SASL_SSL` with `SCRAM-SHA-512`. Credentials and the bootstrap address come from the secret scope `rtmpack` (keys `kafka-bootstrap`, `kafka-username`, `kafka-password`).
2. Each Kafka value is parsed as the input event. The format is in the Benchmark setup section of [../README.md](../README.md). `event_ts` is epoch milliseconds and becomes the Spark timestamp `event_time`. Rows that fail to parse, or that have no `order_id` or `event_time`, are dropped.
3. `in_ts` of each side is the Kafka record `timestamp` (broker LogAppendTime), in epoch milliseconds.
4. Both sides get `withWatermark(<event time>, "30 seconds")`. The inner join condition is:

```python
(o.order_id == p.order_id)
& (p.event_time >= o.event_time)
& (p.event_time <= o.event_time + interval 30 seconds)
```

5. The output record has Kafka key `order_id` and a JSON value with `order_id`, `payment_id`, `key`, `amount`, `method`, `order_ts` (order `event_ts`), `payment_ts` (payment `event_ts`), `in_ts` and `engine="rtm"`. `in_ts` is `greatest(order in_ts, payment in_ts)`, so latency is measured from the newer input. The sink uses the real-time trigger `trigger(realTime=<--checkpoint-interval>)`. Its output mode is set by `--output-mode`, which defaults to `update`.

Together, the watermark and the time bound on the join condition let Spark drop an order's state once no payment can still match it.

## Stream-stream join settings

RTM stream-stream joins need five settings from the [Real-Time Mode setup page](https://docs.databricks.com/aws/en/structured-streaming/real-time/). At start, the job sets them on the session (`apply_join_confs`). For any setting the session rejects, it prints a warning to stderr.

| Setting | Value |
|---|---|
| `spark.databricks.streaming.realTimeMode.streamStreamJoin.enabled` | `true` |
| `spark.sql.streaming.join.stateFormatVersion` | `4` |
| `spark.sql.streaming.join.stateFormatV4.enabled` | `true` |
| `spark.sql.streaming.stateStore.rocksdb.mergeOperatorVersion` | `2` |
| `spark.sql.streaming.realTimeMode.controlMessage.enabled` | `true` |

The Terraform job definition also sets all five in the cluster `spark_conf`, with the same values.

Cluster `spark_conf` for this job:

```
spark.databricks.streaming.realTimeMode.enabled                       true
spark.sql.shuffle.partitions                                          4
spark.databricks.streaming.realTimeMode.streamStreamJoin.enabled      true
spark.sql.streaming.join.stateFormatVersion                           4
spark.sql.streaming.join.stateFormatV4.enabled                        true
spark.sql.streaming.stateStore.rocksdb.mergeOperatorVersion           2
spark.sql.streaming.realTimeMode.controlMessage.enabled               true
```

The local tests do not set these. They run on open-source PySpark 4.1, which does not provide the Databricks real-time settings, and they check only the join logic.

## Arguments

The parameters are command-line arguments of the Python task.

| Argument | Default | Meaning |
|---|---|---|
| `--secret-scope` | `rtmpack` | Databricks secret scope that holds the MSK credentials. |
| `--input-topic` | `rtmpack.08-join.input` | Topic base. The job appends `.orders` and `.payments`. |
| `--output-topic` | `rtmpack.08-join.out.rtm` | Output topic, used as is with no suffix. |
| `--partitions` | `4` | Sets `spark.sql.shuffle.partitions`, the size of the join stage. Source tasks follow the 8 partitions of each topic. |
| `--watermark-delay` | `30 seconds` | Watermark delay on both inputs. |
| `--join-window` | `30 seconds` | Upper bound of the payment time after the order time. |
| `--output-mode` | `update` | Sink output mode, `update` or `append`. |
| `--checkpoint-interval` | `60` (seconds) | Real-time trigger interval. A plain number means seconds. Any Spark interval string also works. The benchmark passes `300 seconds`. |
| `--kafka-linger-ms` | unset | Kafka producer `linger.ms` for the sink. The benchmark passes `0`. |
| `--checkpoint-location` | none (required) | Checkpoint path, such as `/Volumes/<catalog>/<schema>/<volume>/checkpoints/08-join-rtm`. |

## Cluster configuration

- Dedicated access mode, Photon off, autoscaling off, on-demand instances.
- `spark.databricks.streaming.realTimeMode.enabled=true`.
- DBR 18 LTS (`18.x-scala2.13`).
- `m5.xlarge` workers with four slots each.

RTM schedules every stage at once. The available task slots must be at least the number of tasks across all stages:
- Orders source stage: 8 tasks (one per partition).
- Payments source stage: 8 tasks.
- Join shuffle stage: 4 tasks (`--partitions`).

The requirement is 20 slots. Seven `m5.xlarge` workers give 28 slots, or 71 percent use. That is inside the 50 to 75 percent target in contract section 8. Whether Spark runs the two source stages separately or as one stage with 16 tasks, the total stays at 20.

## Mapping to the Flink interval join

| Concern | Flink | RTM (this job) |
|---|---|---|
| Join form | Interval join on rowtime: `p.event_time BETWEEN o.event_time AND o.event_time + INTERVAL '30' SECOND` | Inner join with the same time bounds in the condition |
| Watermark | `event_time - 5 s` on each source | `withWatermark` 30 seconds on each source |
| State cleanup | By watermark, per side | By watermark, per side |
| `in_ts` | `GREATEST` of the two Kafka timestamps | `greatest` of the two Kafka timestamps |
| Output key and value | Kafka key `order_id`, JSON value | Kafka key `order_id`, JSON value |
| Delivery | at-least-once | at-least-once (the Kafka sink cannot be exactly-once in RTM) |

The watermark delays differ, 5 s for Flink and 30 s for RTM, because the contract sets them per engine. A longer watermark delay keeps state longer in RTM.

## Running the local tests

The tests need Java 17 and PySpark 4.1, the Spark version of DBR 18. Install `patterns/requirements-test.txt` into a Python environment once. Set `RTMPACK_TEST_PYTHON` to that environment's interpreter. The script uses it for the Spark driver and the workers, so their versions match. Run these commands from the repository root:

```sh
python3 -m venv /tmp/rtm-venv && /tmp/rtm-venv/bin/pip install -r patterns/requirements-test.txt   # once
RTMPACK_TEST_PYTHON=/tmp/rtm-venv/bin/python infra/scripts/test-pattern.sh 08-join
```

This runs the RTM tests, the Flink build and tests, and the harness tests. To run only the RTM tests, add `--skip-flink --skip-harness`.

The tests cover:
- The join condition boundaries. A payment at +0 s and at +30 s is included. A payment at +30.001 s, at +31 s, or 1 ms before the order is excluded.
- Orders without payments and payments without orders produce no output.
- Output fields, the Kafka key, and `engine="rtm"`.
- `in_ts` as the larger input timestamp, in both directions, in epoch milliseconds.
- Dropping malformed rows.
- A finite streaming join with watermarks (file sources, memory sink, `availableNow`).
- That the job declares the five join settings with the values above, the argument defaults, and the SASL options.

The real-time trigger, the effect of the join settings, the Kafka source and sink, and the secret handling run only on Databricks. Local tests do not cover them.
