# 04 Routing: Spark Real-Time Mode

This directory holds the Spark Real-Time Mode (RTM) job for the 04-routing pattern. The pattern covers demux by type and fan-out to a second consumer. One query reads `rtmpack.04-routing.input` and writes every event twice: once to its type topic and once to an all-events topic. The pattern overview is in [../README.md](../README.md). The formats are in sections 2 and 3 of [../../../docs/CONTRACT.md](../../../docs/CONTRACT.md).

| Path | Purpose |
|---|---|
| PySpark job | The job. The transformation `route_events` is a pure function. |
| `tests/` | Local pytest suite. Runs the transformation in batch mode. |

## Output topics

`--output-topic` is the base name. Its default is `rtmpack.04-routing.out.rtm`.

| Topic | Content |
|---|---|
| `<base>.payment`, `<base>.order`, `<base>.click`, `<base>.login` | Events of that type (demux). |
| `<base>.all` | Every event (fan-out). |

All five topics must exist before the job starts. The Kafka key is `event_id`. The value has `event_id`, `key`, `seq`, `event_ts`, `is_dup`, `type`, `in_ts` and `engine="rtm"`. The job reads `payload` but does not write it.

## Data flow

1. The job reads `rtmpack.04-routing.input` with `startingOffsets=latest`. It uses `SASL_SSL` with `SCRAM-SHA-512`. The credentials and the bootstrap address come from the secret scope `rtmpack` (keys `kafka-bootstrap`, `kafka-username`, `kafka-password`).
2. The Kafka value is parsed with `from_json`. Rows with a null `event_id` are dropped, including malformed JSON.
3. `in_ts` is the Kafka record timestamp in epoch milliseconds. Both copies of an event carry the same `in_ts`.
4. The job builds two topic names for each event, `<base>.<type>` and `<base>.all`. `explode(array(type_topic, all_topic))` turns the event into two rows, with a string column `topic`.
5. The Kafka sink starts without the `topic` option, so each row goes to the topic named in its `topic` column. The output mode is update, with `trigger(realTime=...)`.

An event whose `type` is not one of the four contract types goes only to the `.all` topic. This way, a topic name is never built from an unexpected value. The generator produces only the four types.

The per-row `topic` column is a feature of the open-source Spark Kafka sink. When no `topic` option is set, the sink reads the topic from each row. The local tests run in batch mode and do not use the Kafka sink, so they do not exercise this path.

## Job parameters

| Argument | Default | Meaning |
|---|---|---|
| `--secret-scope` | `rtmpack` | Secret scope that holds the MSK credentials. |
| `--input-topic` | `rtmpack.04-routing.input` | Input topic. |
| `--output-topic` | `rtmpack.04-routing.out.rtm` | Base for the five output topics. |
| `--partitions` | unset | Accepted for the common job interface. Not used, because the query has no shuffle. |
| `--checkpoint-interval` | `5 minutes` | Real-time trigger interval, which is also the checkpoint interval. A bare number is read as seconds. |
| `--checkpoint-location` | required | Checkpoint path on a Unity Catalog volume, such as `/Volumes/<catalog>/<schema>/<volume>/checkpoints/04-routing-rtm`. |
| `--kafka-linger-ms` | unset | When set, adds the sink option `kafka.linger.ms`. The benchmark profile uses `0`. |
| `--mode` | `demux-fanout` | `demux-fanout` is the pattern: a type topic plus the all-events topic. `all-only` is a diagnostic variant. It writes each event once, to the all-events topic, through the same `topic` column. |

The Terraform module sets the cluster configuration: `spark.databricks.streaming.realTimeMode.enabled=true`, Photon off, dedicated access mode, on-demand nodes and a fixed worker count.

## Flink equivalent

Flink uses one source table, five sink tables, and one statement set. The set has five `INSERT` statements. Four of them filter with `WHERE type = '<type>'`, and one has no filter. Both engines read the input once and write each event twice. See [../flink/README.md](../flink/README.md).

## Slot sizing

RTM schedules all stages at once, so every task needs a slot. The query has only map-side operators: `from_json`, `explode` and `to_json`. It has one stage, so the number of tasks equals the number of input topic partitions. There is no shuffle, so `tasks_required = partitions`.

| Partitions | Minimum slots at 75 percent | Example cluster (m5.xlarge, 4 vCPU) |
|---|---|---|
| 8 | 11 | 3 workers (12 slots) |
| 16 | 22 | 6 workers (24 slots) |

The output rate is twice the input rate. This does not change the task count.

## Local tests

The tests need Java 17 and PySpark 4.1, the Spark version of DBR 18. Install `patterns/requirements-test.txt` into a Python environment once. Set `RTMPACK_TEST_PYTHON` to that environment's interpreter. The script uses it for the Spark driver and the workers, so their versions match. Run these commands from the repository root:

```sh
python3 -m venv /tmp/rtm-venv && /tmp/rtm-venv/bin/pip install -r patterns/requirements-test.txt   # once
RTMPACK_TEST_PYTHON=/tmp/rtm-venv/bin/python infra/scripts/test-pattern.sh 04-routing
```

This runs the RTM tests, the Flink build and tests, and the harness tests. To run only the RTM tests, add `--skip-flink --skip-harness`.

The tests check that each event produces two rows with the right topics, and that every type routes to its own topic. They also check the output fields (no `payload`) and `in_ts` on both copies. Each event gives two outputs, with distinct (topic, key) pairs. The tests also cover unknown and malformed input, and argument parsing.
