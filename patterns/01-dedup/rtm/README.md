# 01 Deduplication: Spark Real-Time Mode

This directory holds the Spark Real-Time Mode (RTM) side of pattern 01-dedup. The job reads the contract input topic `rtmpack.01-dedup.input` from Amazon MSK. It keeps the first record per `event_id` within a 60-second state horizon and writes the result to `rtmpack.01-dedup.out.rtm`. The pattern is described in [../README.md](../README.md). The Flink side is in [../flink/README.md](../flink/README.md).

| Path | Purpose |
|---|---|
| `dedup_rtm.py` | PySpark job. The transformation `dedup_first_per_event` is a pure function. |
| `tests/test_dedup_rtm.py` | Local pytest suite. Runs the transformation in batch mode and as a finite streaming query. |

Terraform deploys the job as the Databricks job `rtmpack-01-dedup-rtm`. Two modules do this work. `infra/terraform/modules/databricks_rtm_job/` uploads the workspace file and creates the job with the RTM cluster. `infra/terraform/modules/databricks_shared/` creates the secret scope and the Kafka secrets.

## How the job works

1. The job reads `rtmpack.01-dedup.input` with `startingOffsets=latest`. It uses `SASL_SSL` with `SCRAM-SHA-512`. Credentials and the bootstrap address come from the secret scope `rtmpack` (keys `kafka-bootstrap`, `kafka-username`, `kafka-password`).
2. The Kafka value is parsed as the contract input event (`event_id`, `key`, `seq`, `event_ts`, `is_dup`, `payload`). `event_ts` is epoch milliseconds and becomes the Spark timestamp `event_time`.
3. `in_ts` is the Kafka record `timestamp` (broker `LogAppendTime`) converted to epoch milliseconds. Latency is measured against this value.
4. Deduplication runs on `event_time`. The watermark equals the state horizon, and the query calls `dropDuplicates(["event_id", "event_time"])`. A duplicate has the same `event_ts` as its original, so this keeps the first arrival of each `event_id`. The event-time column must be part of the key so the watermark can evict state.
5. The output record has Kafka key `event_id` and a JSON value with the input fields, `in_ts` and `engine="rtm"`. It is written in update output mode with `trigger(realTime=<--checkpoint-interval>)`.

```python
parsed  = from_json(value) + in_ts + event_time, rows without event_id or event_time dropped
deduped = parsed.withWatermark("event_time", "60 seconds").dropDuplicates(["event_id", "event_time"])
```

`dropDuplicatesWithinWatermark(["event_id"])` is the more direct operator. DBR 18 LTS accepts it, but DBR 18.2 rejects it. The job uses the event-time form by default so that it runs on both. Pass `--dedup-operator within-watermark` to use the direct operator.

For a finite batch DataFrame, the function uses `dropDuplicates(["event_id"])`. This branch exists only for unit tests. Production always runs the streaming branch.

## Arguments

The arguments are command-line arguments of the Python task. Terraform passes them.

| Argument | Default | Meaning |
|---|---|---|
| `--secret-scope` | `rtmpack` | Secret scope that holds the MSK credentials. The job definition never contains credentials. |
| `--input-topic` | `rtmpack.01-dedup.input` | Input topic. |
| `--output-topic` | `rtmpack.01-dedup.out.rtm` | Output topic. |
| `--partitions` | `8` | Sets `spark.sql.shuffle.partitions`. This is the task count of the shuffle stage. Terraform passes `4`. |
| `--state-horizon` | `60 seconds` | Watermark delay for dedup state. It is longer than the maximum duplicate delay (30 s). |
| `--checkpoint-interval` | `60` | Real-time trigger interval. A bare number is read as seconds. Terraform passes `300 seconds`, the documented default. |
| `--kafka-linger-ms` | unset | When set, adds the sink option `kafka.linger.ms`. Terraform passes `0`. |
| `--dedup-operator` | `drop-duplicates` | `drop-duplicates` calls `dropDuplicates` on `event_id` and `event_time`. `within-watermark` calls `dropDuplicatesWithinWatermark(["event_id"])`. |
| `--checkpoint-location` | required | Checkpoint path on a Unity Catalog Volume, such as `/Volumes/<catalog>/<schema>/<volume>/checkpoints/01-dedup-rtm`. |

## Cluster configuration

Terraform creates a job cluster that meets the RTM requirements in the [setup documentation](https://docs.databricks.com/aws/en/structured-streaming/real-time/setup).

| Setting | Value | Reason |
|---|---|---|
| `spark_version` | `18.x-scala2.13` (DBR 18 LTS) | RTM needs 16.4 LTS or later. |
| `data_security_mode` | `DATA_SECURITY_MODE_DEDICATED` | Dedicated access mode. Standard mode works for Python only. |
| `runtime_engine` | `STANDARD` | Photon must be off. |
| `num_workers` | `4` (fixed) | Autoscaling must be off. The count comes from the slot check below. |
| `aws_attributes.availability` | `ON_DEMAND` | Spot must be off. |
| `spark.databricks.streaming.realTimeMode.enabled` | `true` | Enables RTM on the cluster. |
| `spark.sql.shuffle.partitions` | `4` | Matches the slot calculation. |
| `node_type_id` | `m5.xlarge` | Four vCPUs per worker, so four task slots per worker. |

The job is continuous, with `max_concurrent_runs = 1`. It is created paused, so no cluster starts until a benchmark unpauses it. When the engines are stopped, `run-benchmark.sh` also cancels active runs. The benchmark waits for probe records on the output topic before the load starts.

## Slot sizing

RTM schedules every stage at once. The documentation says that "the total number of available task slots must be greater than or equal to the number of tasks across all query stages." This query has two stages:

- Source stage: one task per Kafka input partition, so 8 tasks.
- Shuffle stage for `dropDuplicates`: `spark.sql.shuffle.partitions` tasks, so 4 tasks.

The requirement is 12 slots. Slots equal `num_workers * worker_vcpus`. The driver runs no tasks, so it adds no slots. Four `m5.xlarge` workers give 16 slots, so 12 tasks use 75 percent. The Terraform module checks this with a `precondition` on `databricks_job`. It outputs `slots_required` and `slots_available`.

## Comparison with the Flink query

| Concern | Flink | RTM (this job) |
|---|---|---|
| Keep first per key | `ROW_NUMBER() ... WHERE rn = 1` | `dropDuplicates(["event_id", "event_time"])` under a watermark keeps the first arrival |
| Key | `PARTITION BY event_id` | `event_id` |
| Order | processing-time order | arrival order |
| State lifetime | TTL of 60 s on state access | watermark of 60 s behind the maximum `event_time` seen |
| Delivery | at-least-once | at-least-once (the Kafka sink cannot be exactly-once in RTM) |

## RTM facts this job relies on

These facts were checked against the public documentation.

| Fact | Source |
|---|---|
| Cluster flag `spark.databricks.streaming.realTimeMode.enabled` set to `true` | [setup](https://docs.databricks.com/aws/en/structured-streaming/real-time/setup) |
| Classic compute with dedicated or standard access mode (standard is Python only). Photon, autoscaling and spot must be off | setup and [reference](https://docs.databricks.com/aws/en/structured-streaming/real-time/reference) |
| Minimum runtime is Databricks Runtime 16.4 LTS | setup |
| Real-time triggers work only in update output mode. The trigger is `.trigger(realTime="5 minutes")`. In PySpark the interval is required | setup, reference |
| Kafka is a source and a sink | reference |
| Exactly-once sinks are not supported, so the Kafka sink is at-least-once | error class `EXACTLY_ONCE_SINK_NOT_SUPPORTED` |
| The Kafka SASL login class is `kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule` | [Kafka authentication](https://docs.databricks.com/aws/en/connect/streaming/kafka/authentication) |
| One real-time job runs per compute resource, provided slots are sufficient | setup |

## Local tests

The tests need Java 17 and PySpark 4.1, the Spark version of DBR 18. Install `patterns/requirements-test.txt` into a Python environment once. Set `RTMPACK_TEST_PYTHON` to that environment's interpreter. The script uses it for the Spark driver and the workers, so their versions match. Run these commands from the repository root:

```sh
python3 -m venv /tmp/rtm-venv && /tmp/rtm-venv/bin/pip install -r patterns/requirements-test.txt   # once
RTMPACK_TEST_PYTHON=/tmp/rtm-venv/bin/python infra/scripts/test-pattern.sh 01-dedup
```

This runs the RTM tests, the Flink build and tests, and the harness tests. To run only the RTM tests, add `--skip-flink --skip-harness`.

The tests cover duplicate collapse, the contract output fields, `engine="rtm"`, `in_ts` in epoch milliseconds, and dropping rows without an `event_id`. They also cover the shaded SCRAM JAAS string, the argument defaults, and the watermarked streaming path. That path runs `dropDuplicates` on `event_id` and `event_time` with `availableNow`, on open-source Spark. The real-time trigger, Kafka, and secret handling run only on Databricks, so local tests do not cover them.

## Deployment

Terraform deploys the job from `infra/terraform/envs/sandbox-usw2`. The checkpoint root must be an existing Unity Catalog Volume path. See [infra/README.md](../../../infra/README.md).
