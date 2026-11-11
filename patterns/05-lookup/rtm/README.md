# 05 Lookup: Spark Real-Time Mode

This directory holds the Spark Real-Time Mode (RTM) job for the lookup pattern. The job reads events from `rtmpack.05-lookup.input`. It joins each event with a 10,000-row merchant reference table and writes the enriched record to `rtmpack.05-lookup.out.rtm`. The input, the reference table rule and the output format are in the Benchmark setup section of [../README.md](../README.md). The pattern overview is in [../README.md](../README.md). The Flink side is in [../flink/README.md](../flink/README.md).

| Role | Purpose |
|---|---|
| PySpark job (`*_rtm.py`) | The job. The transformation `enrich_events` is a pure function. |
| `tests/` | Local pytest suite. Runs the transformation in batch mode and checks the plan. |

Terraform deploys the job as the Databricks job `rtmpack-05-lookup-rtm`. Two modules do this work. `../../../infra/terraform/modules/databricks_rtm_job/` uploads the workspace file and creates the job with the RTM cluster. `../../../infra/terraform/modules/databricks_shared/` creates the secret scope and the Kafka secrets.

## Data flow

1. The job reads `merchants.csv` from a Unity Catalog volume with `spark.read`. It uses an explicit schema (`merchant_id`, `risk_tier`, `category`, `country`, all strings) and `header=true`. There is no schema inference.
2. The job reads `rtmpack.05-lookup.input` with `startingOffsets=latest`. It uses `SASL_SSL` with `SCRAM-SHA-512`. Credentials and the bootstrap address come from the secret scope `rtmpack` (keys `kafka-bootstrap`, `kafka-username`, `kafka-password`).
3. The Kafka value is parsed with `from_json` into `event_id`, `key`, `seq`, `event_ts`, `is_dup`, `merchant_id` and `payload`. Rows with a null `event_id` are dropped.
4. `in_ts` is the Kafka record `timestamp` converted to epoch milliseconds.
5. The job joins the events with `F.broadcast(merchants)` on `merchant_id`. This is an inner stream-static join. An event whose `merchant_id` is null or not in the table produces no output.
6. The output record has Kafka key `event_id` and a JSON value with `event_id`, `key`, `event_ts`, `merchant_id`, `risk_tier`, `category`, `country`, `in_ts` and `engine="rtm"`. It is written in update output mode with `trigger(realTime=...)`.

The job reads `seq`, `is_dup` and `payload` but does not copy them to the output, because the contract output has no such fields.

## Job arguments

| Argument | Default | Meaning |
|---|---|---|
| `--secret-scope` | `rtmpack` | Secret scope that holds the MSK credentials. |
| `--input-topic` | `rtmpack.05-lookup.input` | Input topic. |
| `--output-topic` | `rtmpack.05-lookup.out.rtm` | Output topic. |
| `--checkpoint-interval` | `5 minutes` | Real-time trigger interval, which is also the checkpoint interval. The tuned profile sets this (section 8 of the contract). A bare number is read as seconds. |
| `--checkpoint-location` | required | Checkpoint path on a Unity Catalog volume, such as `/Volumes/<catalog>/<schema>/<volume>/checkpoints/05-lookup-rtm`. |
| `--kafka-linger-ms` | unset | When set, adds the sink option `kafka.linger.ms`. The tuned profile uses `0`. |
| `--reference-path` | required | Volume path of the reference file, such as `/Volumes/<catalog>/<schema>/<volume>/refdata/merchants.csv`. |
| `--reference-mode` | `read` | How the static side is held. `read` keeps a file scan in the plan. `cached` caches the data in memory before the query starts. `local` collects the rows to the driver once and embeds them in the plan as a local relation. All three keep the broadcast join. |

The reference file must be uploaded to the volume before the job starts. The job fails at start if the path does not exist.

The Terraform module sets the cluster configuration: `spark.databricks.streaming.realTimeMode.enabled=true`, DBR 18 LTS (`18.x-scala2.13`), Photon off, dedicated access mode, on-demand nodes and a fixed worker count. The job does not set `spark.sql.shuffle.partitions`, because the query has no shuffle stage.

## Broadcast limit

The RTM documentation supports stream-static joins (inner, outer and broadcast) for static tables up to 10 MB. This job uses the broadcast form. The lookup table is about 240 KB, well below that limit. Reference data above 10 MB needs a different design. For example, a scalar UDF with a local cache that calls an external store.

The static side is a file read in the query plan. In `read` mode, the job does not call `cache()` or `persist()` on it. To pick up a change to the table, update the file and restart the query.

## Slot sizing

RTM schedules all stages at once, so every task needs a slot. The streaming query reads from Kafka, joins against the broadcast table using map-side operators only, and writes to Kafka. There is one stage and no shuffle. The number of tasks therefore equals the number of input topic partitions: `tasks_required = partitions`.

Slots equal `num_workers * worker_vcpus`. The driver runs no tasks. The tuned profile requires `tasks_required <= 0.75 * slots`. The `databricks_rtm_job` module checks this in a precondition, together with `slots >= tasks_required`.

| Partitions | Minimum slots at 75 percent | Example cluster (m5.xlarge, 4 vCPU) |
|---|---|---|
| 8 | 11 | 3 workers (12 slots) |
| 16 | 22 | 6 workers (24 slots) |

The output rate is at most the input rate, because events with an unknown merchant are dropped. With the generated data, every `merchant_id` is in the table, so output equals input.

## Run the tests

The tests need Java 17 and PySpark 4.1, the Spark version of DBR 18. Install `patterns/requirements-test.txt` into a Python environment once. Set `RTMPACK_TEST_PYTHON` to that environment's interpreter. The script uses it for the Spark driver and the workers, so their versions match. Run these commands from the repository root:

```sh
python3 -m venv /tmp/rtm-venv && /tmp/rtm-venv/bin/pip install -r patterns/requirements-test.txt   # once
RTMPACK_TEST_PYTHON=/tmp/rtm-venv/bin/python infra/scripts/test-pattern.sh 05-lookup
```

This runs the RTM tests, the Flink build and tests, and the harness tests. To run only the RTM tests, add `--skip-flink --skip-harness`.

The tests generate a 50-row reference file from the contract rule, so they do not need `data/merchants.csv`. They check the output fields and values, and that unknown, null and malformed rows are dropped. They also check `in_ts`. The tests check that the executed plan of the join contains `BroadcastHashJoin` and no `SortMergeJoin`. The plan check runs with `spark.sql.autoBroadcastJoinThreshold=-1`, so only the explicit hint can cause the broadcast.
