# 02 Array unnest: Spark Real-Time Mode

This directory holds the Spark Real-Time Mode (RTM) side of pattern 02-unnest. The job reads orders from `rtmpack.02-unnest.input`. It emits one record per element of the `items` array and writes the result to `rtmpack.02-unnest.out.rtm`. The pattern is described in [../README.md](../README.md). The input and output formats are in the Benchmark setup section of [../README.md](../README.md). The Flink side is in [../flink/README.md](../flink/README.md).

| Path | Purpose |
|---|---|
| `unnest_rtm.py` | PySpark job. The transformation `unnest_items` is a pure function. |
| `tests/test_unnest_rtm.py` | Local pytest suite. Runs the transformation in batch mode. |

Terraform deploys the job as the Databricks job `rtmpack-02-unnest-rtm`. Two modules do this work. `infra/terraform/modules/databricks_rtm_job/` uploads the workspace file and creates the job with the RTM cluster. `infra/terraform/modules/databricks_shared/` creates the secret scope and the Kafka secrets.

## How the job works

1. The job reads `rtmpack.02-unnest.input` with `startingOffsets=latest`. It uses `SASL_SSL` with `SCRAM-SHA-512`. Credentials and the bootstrap address come from the secret scope `rtmpack` (keys `kafka-bootstrap`, `kafka-username`, `kafka-password`).
2. The Kafka value is parsed into `order_id`, `key`, `event_ts` and `items`. The items field is an array of structs with `sku`, `qty` and `price`. By default (`--parse from-json`), the job uses `from_json` with the contract schema. With `--parse variant`, the value is parsed with `try_parse_json` into a VARIANT. Each field is then read with `try_variant_get`, with `items` typed as `array<struct<sku,qty,price>>`. Rows with a null `order_id` are dropped. This also drops malformed JSON.
3. `in_ts` is the Kafka record `timestamp` converted to epoch milliseconds. It is the same for every record produced from one order, and latency is measured against it.
4. `posexplode(items)` produces one row per element, with its 0-based position `item_idx`. An order with a null or empty `items` array produces no rows.
5. The output record has Kafka key `<order_id>:<item_idx>` and a JSON value with `order_id`, `key`, `event_ts`, `item_idx`, `sku`, `qty`, `price`, `in_ts` and `engine="rtm"`. It is written in update output mode with `trigger(realTime=...)`.

The `payload` field is not copied to the output, because the contract output has no `payload` field.

## Arguments

The arguments are command-line arguments of the Python task. Terraform passes them.

| Argument | Default | Meaning |
|---|---|---|
| `--secret-scope` | `rtmpack` | Secret scope that holds the MSK credentials. |
| `--input-topic` | `rtmpack.02-unnest.input` | Input topic. |
| `--output-topic` | `rtmpack.02-unnest.out.rtm` | Output topic. |
| `--checkpoint-interval` | `5 minutes` | Real-time trigger interval (the documented default). A bare number is read as seconds. |
| `--checkpoint-location` | required | Checkpoint path on a Unity Catalog Volume, such as `/Volumes/<catalog>/<schema>/<volume>/checkpoints/02-unnest-rtm`. |
| `--kafka-linger-ms` | unset | When set, adds the sink option `kafka.linger.ms`. The benchmark uses `0`. |
| `--parse` | `from-json` | `from-json` or `variant`. `variant` parses the value with `try_parse_json` and reads fields with `try_variant_get`. It then applies the same `posexplode`. The output is the same. |

The Terraform module sets the cluster configuration: `spark.databricks.streaming.realTimeMode.enabled=true`, Photon off, dedicated access mode, on-demand nodes and a fixed worker count. The job does not set `spark.sql.shuffle.partitions` itself. The module sets it on the cluster. This pattern has no shuffle stage, so the value does not affect the query.

## Flink equivalent

The Flink SQL form of the same transformation is a lateral join against the array, with an ordinal:

```sql
SELECT o.order_id, o.key, o.event_ts, t.idx - 1 AS item_idx, t.sku, t.qty, t.price
FROM orders AS o
CROSS JOIN UNNEST(o.items) WITH ORDINALITY AS t(sku, qty, price, idx)
```

`WITH ORDINALITY` is 1-based. The Flink side therefore subtracts 1 to match the 0-based `item_idx` from `posexplode`. Both forms are stateless and work per record.

## Slot sizing

RTM schedules all stages at once, so every task needs a slot. The query reads from Kafka and writes to Kafka. It uses only map-side operators: the parse, `posexplode` and `to_json`. It has one stage, so the number of tasks equals the number of input topic partitions. There is no shuffle, so `tasks_required = partitions`.

Slots equal `num_workers * worker_vcpus`. The driver runs no tasks. The tuned profile (contract section 8) requires `tasks_required <= 0.75 * slots`. The `databricks_rtm_job` module checks this in a precondition, together with `slots >= tasks_required`.

| Partitions | Minimum slots at 75 percent | Example cluster (m5.xlarge, 4 vCPU) |
|---|---|---|
| 8 | 11 | 3 workers (12 slots) |
| 16 | 22 | 6 workers (24 slots) |

The output rate is about three times the input rate, because each order has 1 to 5 items with a mean of 3. The sink therefore writes more records per task than the source reads. This does not change the task count.

## Local tests

The tests need Java 17 and PySpark 4.1, the Spark version of DBR 18. Install `patterns/requirements-test.txt` into a Python environment once. Set `RTMPACK_TEST_PYTHON` to that environment's interpreter. The script uses it for the Spark driver and the workers, so their versions match. Run these commands from the repository root:

```sh
python3 -m venv /tmp/rtm-venv && /tmp/rtm-venv/bin/pip install -r patterns/requirements-test.txt   # once
RTMPACK_TEST_PYTHON=/tmp/rtm-venv/bin/python infra/scripts/test-pattern.sh 02-unnest
```

This runs the RTM tests, the Flink build and tests, and the harness tests. To run only the RTM tests, add `--skip-flink --skip-harness`.

The real-time trigger, Kafka, and secret handling run only on Databricks. Local tests do not cover them.
