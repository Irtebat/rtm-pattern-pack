# 03 UDF: Spark Real-Time Mode

This directory holds the Spark Real-Time Mode (RTM) job for the 03-udf pattern. It reads events from `rtmpack.03-udf.input`, scores each one with an Arrow-optimized Python scalar UDF, and writes the result to `rtmpack.03-udf.out.rtm`. The pattern overview is in [../README.md](../README.md). The scoring rule and the formats are in the Benchmark setup section of [../README.md](../README.md).

| Path | Purpose |
|---|---|
| PySpark job | The job. The transformation `score_events` and the scoring function `score_decision` are pure and testable. |
| `tests/` | Local pytest suite. Runs the transformation in batch mode. |
| [../score_vectors.csv](../score_vectors.csv) | Shared test vectors: `event_id` and `key` pairs with the expected `score` and `decision`. The Flink tests read the same file. |

## Scoring rule

```
h        = SHA-256(UTF-8(event_id + "|" + key))
score    = (first 4 bytes of h as an unsigned big-endian integer) mod 1000
decision = "flag" if score >= 950, "review" if score >= 800, else "approve"
```

`score_decision(event_id, key)` returns `(score, decision)`. It returns `None` if either argument is null.

## The UDF API

The UDF is declared with the Arrow-optimized Python UDF of PySpark 4.1:

```python
from pyspark.sql.functions import udf
score_udf = udf(score_decision, returnType=<struct<score:int, decision:string>>, useArrow=True)
```

With `useArrow=True`, Spark moves data between the JVM and the Python worker in Arrow format. It calls the Python function once per row. The same behavior can be turned on for a whole session with `spark.sql.execution.pythonUDF.arrow.enabled`. The job uses the explicit parameter, so the choice is visible in the code.

PySpark 4.1 also has `pyspark.sql.functions.arrow_udf`. Its function receives and returns `pyarrow.Array` batches. This job does not use it. The SHA-256 is computed per value, so a batch function would only wrap a loop. The pattern asks for an Arrow-optimized Python scalar UDF, which is the `udf(..., useArrow=True)` form.

The job sets `spark.sql.execution.arrow.maxRecordsPerBatch = 1` before the query starts. The RTM reference recommends this for DBR 18 and below. Each batch then holds one record, so no record waits for an Arrow batch to fill. This is a session SQL setting, so it does not need to go in the cluster configuration. `--arrow-max-records-per-batch 0` keeps the runtime default, which batches by time on DBR 19 and later.

The UDF returns one struct with `score` and `decision`. The function runs once per record, and the query reads both fields from the result.

## Data flow

1. The job reads `rtmpack.03-udf.input` with `startingOffsets=latest`. It uses `SASL_SSL` with `SCRAM-SHA-512`. The credentials and the bootstrap address come from the secret scope `rtmpack` (keys `kafka-bootstrap`, `kafka-username`, `kafka-password`).
2. The Kafka value is parsed with `from_json` into `event_id`, `key`, `seq`, `event_ts`, `is_dup` and `payload`. Rows with a null `event_id` or `key` are dropped, including malformed JSON.
3. `in_ts` is the Kafka record timestamp converted to epoch milliseconds. Latency is measured against it.
4. The UDF is applied to `event_id` and `key`.
5. The output record has the Kafka key `event_id` and a JSON value with `event_id`, `key`, `seq`, `event_ts`, `score`, `decision`, `in_ts` and `engine="rtm"`. It is written in update output mode with `trigger(realTime=...)`.

The `payload` field is read but not copied to the output.

## Job parameters

| Argument | Default | Meaning |
|---|---|---|
| `--secret-scope` | `rtmpack` | Secret scope that holds the MSK credentials. |
| `--input-topic` | `rtmpack.03-udf.input` | Input topic. |
| `--output-topic` | `rtmpack.03-udf.out.rtm` | Output topic. |
| `--partitions` | unset | Accepted for the common job interface. Not used, because the query has no shuffle. |
| `--checkpoint-interval` | `5 minutes` | Real-time trigger interval, which is also the checkpoint interval. A bare number is read as seconds. |
| `--checkpoint-location` | required | Checkpoint path on a Unity Catalog volume, such as `/Volumes/<catalog>/<schema>/<volume>/checkpoints/03-udf-rtm`. |
| `--kafka-linger-ms` | unset | When set, adds the sink option `kafka.linger.ms`. The benchmark profile uses `0`. |
| `--scoring` | `arrow-udf` | `arrow-udf` follows the pattern definition. `builtin` computes the same score with built-in SQL functions (`sha2`, `substring`, `conv`, `pmod`). It uses no Python worker. |
| `--arrow-max-records-per-batch` | `1` | Value for `spark.sql.execution.arrow.maxRecordsPerBatch`. `0` keeps the runtime default. |

The Terraform module sets the cluster configuration: `spark.databricks.streaming.realTimeMode.enabled=true`, Photon off, dedicated access mode, on-demand nodes and a fixed worker count.

## Flink equivalent

Flink uses a Java `ScalarFunction`. It is registered with `createTemporarySystemFunction` and called from SQL, and it runs inside the Flink task thread. The RTM UDF runs in a separate Python worker process for each task, with Arrow serialization between the two. See [../flink/README.md](../flink/README.md).

## Slot sizing

RTM schedules all stages at once, so every task needs a slot. The query reads from Kafka and writes to Kafka. It uses only map-side operators: `from_json`, the UDF and `to_json`. It has one stage, so the number of tasks equals the number of input topic partitions. There is no shuffle, so `tasks_required = partitions`.

Slots equal `num_workers * worker_vcpus`. The driver runs no tasks. The benchmark profile requires `tasks_required <= 0.75 * slots`.

| Partitions | Minimum slots at 75 percent | Example cluster (m5.xlarge, 4 vCPU) |
|---|---|---|
| 8 | 11 | 3 workers (12 slots) |
| 16 | 22 | 6 workers (24 slots) |

Python worker processes run on the same worker as the JVM, so worker memory must cover both.

## Local tests

The tests need Java 17 and PySpark 4.1, the Spark version of DBR 18. Install `patterns/requirements-test.txt` into a Python environment once. Set `RTMPACK_TEST_PYTHON` to that environment's interpreter. The script uses it for the Spark driver and the workers, so their versions match. Run these commands from the repository root:

```sh
python3 -m venv /tmp/rtm-venv && /tmp/rtm-venv/bin/pip install -r patterns/requirements-test.txt   # once
RTMPACK_TEST_PYTHON=/tmp/rtm-venv/bin/python infra/scripts/test-pattern.sh 03-udf
```

This runs the RTM tests, the Flink build and tests, and the harness tests. To run only the RTM tests, add `--skip-flink --skip-harness`.

The tests check the shared vector against the pure function and through the query. They also cover the decision thresholds, the output fields, `in_ts`, null and malformed input, and argument parsing. The query tests always run with the regular Python UDF. They run with the Arrow-optimized UDF when `pyarrow` and `pandas` are installed, and they are skipped otherwise.
