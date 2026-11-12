# How a Run Works

This page follows one pattern run from start to finish and names the file and function behind each step. It then explains how each reported number is computed. It ends with a reading order for the code and a checklist for each run.

Pattern ids are the folder names under `patterns/` (for example `01-dedup`). `<id>` stands for one of them.

## 1. The moving parts

| Part | Where it runs | What it does | Defined in |
|---|---|---|---|
| Orchestrator | Your workstation | Drives every step through Terraform, the Databricks CLI and AWS Systems Manager (SSM). Holds the credentials that start and stop the engines | `infra/scripts/run-session.sh`, `infra/scripts/run-benchmark.sh` |
| Amazon MSK | AWS, workspace VPC | Input and output topics. Its broker clock timestamps every record (LogAppendTime), which is what latency is measured with | `infra/terraform/modules/msk` |
| Load generator host | EC2, workspace VPC | Runs the harness: creates topics, writes the input stream, reads the output topics and computes results. Reached only through SSM | `infra/terraform/modules/loadgen_host`, `harness/` |
| RTM job | Databricks classic job cluster | A continuous job that runs the pattern's PySpark script with a real-time trigger | `infra/terraform/modules/databricks_rtm_job`, `patterns/<id>/rtm/` |
| Flink application | Amazon Managed Service for Apache Flink | Runs the pattern's Flink SQL from a jar | `infra/terraform/modules/flink_app`, `patterns/<id>/flink/` |

All per-pattern settings (worker count, task count, job parameters, jar, topics) live in one place: `local.patterns` in `infra/terraform/envs/sandbox-usw2/patterns.tf`. The map keys are the pattern ids.

## 2. One run, step by step

`run-session.sh --patterns 01-dedup,02-unnest` wraps steps 1 and 12 around one `run-benchmark.sh` call per pattern (steps 0 and 2 to 11).

| Step | What happens | Code | What can go wrong, and how it shows |
|---|---|---|---|
| 0. Preflight | AWS identity and Databricks login are checked | `run-benchmark.sh`, the `aws sts get-caller-identity` and `databricks current-user me` lines | Expired login: the script exits before touching anything |
| 1. Host up | Terraform sets `loadgen_running=true`, which starts the EC2 instance. The script waits until SSM reports the host Online. It then reinstalls the harness bundle from S3 | `run-session.sh` `host_state`; `modules/loadgen_host`; `update-harness.sh` | Host never Online: the wait loop gives up after 10 minutes |
| 2. Topics | `rtmpack topics create` creates `rtmpack.<id>.input`, `.out.rtm` and `.out.flink` with LogAppendTime, 12 h retention, replication 3 and 8 partitions. Existing topics are left as they are | `harness/rtmpack_harness/topics.py` `create_topics`; constants in `contract.py` | None in practice; the command is idempotent |
| 3. Engines start | Terraform plans with `running_patterns=["<id>"]`. In `patterns.tf`, `contains(var.running_patterns, key)` sets the RTM job's continuous `pause_status` to UNPAUSED and the Flink application's `start_application` to true. The plan is saved and checked for deletes or replacements. Only then is it applied | `run-benchmark.sh` `apply_state`; `patterns.tf`; `modules/databricks_rtm_job/main.tf`; `modules/flink_app/main.tf` | A plan with deletes stops the run and prints the resource list. RTM slot checks (`precondition` blocks) fail the plan if tasks exceed slots or 75 percent of slots |
| 4. Wait for RUNNING | Polls the RTM job's active run and the Flink application status every 20 s, up to 30 minutes | `run-benchmark.sh`, the polling loop | Step 5 confirms that both engines produce output before the load starts |
| 5. Probes | Sends 20-second bursts at 200 events per second, each with its own seed. It checks that both output topics received probe records, for up to 30 attempts. Then it waits 90 s to settle | `run-benchmark.sh` probe loop | No output after 30 attempts fails the run |
| 6. Load | The generator writes the measured run: by default 10,000 events per second for 1,800 s. It writes a manifest with `status: running`, then `status: complete` with `start_ms`, `warmup_cutoff_ms` (start plus 300 s) and `end_ms` | `harness/rtmpack_harness/loadgen.py` `run_loadgen` and `LoadWorker`; event shapes in `events.py` and `patterns/`; resends in `schedule.py` | `max_lag_ms` and `delivery_errors` in the manifest show whether the generator kept its schedule. Lag above a few milliseconds means the host, not the engines, limited the run |
| 7. Engines process | RTM reads with `startingOffsets=latest`, transforms, and writes to Kafka with `trigger(realTime="300 seconds")`. Flink's job reads its SQL file, applies the `SET` statements in code, and runs source, transform and sink as one statement set | RTM: `patterns/<id>/rtm/*.py`, a `run()` function and a pure transform function. Flink: `*Job.java`, `*Sql.java` and `src/main/resources/*.sql` under `patterns/<id>/flink/` | Engine failures show up as zero output in step 8. Check the RTM run page and the Flink CloudWatch log group |
| 8. Collect | For each engine, reads the output topic from the offset at `start_ms` to `end_ms` plus a grace period (maximum duplicate delay plus 60 s). Every record counts in one or both windows, chosen by its input timestamp `in_ts`. Latency uses `warmup_cutoff_ms` to `end_ms`, split into two halves at the midpoint. Correctness uses `start_ms` to `end_ms` plus the maximum duplicate delay | `harness/rtmpack_harness/collect.py` `collect_run`; `stats.py` `OutputAccumulator`, `LatencyHistogram`, `DistinctCounter`, `split_half_stable` | The `diagnostics` line: `negative_latency_records`, `malformed_records` and `non_append_time_records` must all be 0. A warning about unread partitions means the engine stopped producing |
| 9. Results | One JSON line per engine in `results/<run_id>.jsonl`, copied to S3 and to the local `results/` folder | `results.py` `write_result`; `run-benchmark.sh` S3 copy | |
| 10. Validity checks | Fails the run if an engine produced no output. Prints a note if the split-half check failed (repeat the run) or if the engines are within 15 percent on p99 (report as comparable) | `run-benchmark.sh`, final Python block | |
| 11. Engines stop | On exit, including failure: Terraform `running_patterns=[]` (pauses the RTM job, stops Flink), then `databricks jobs cancel-all-runs`, which cancels the active RTM run | `run-benchmark.sh` `cleanup` (EXIT trap) | If the workstation dies here, the engines keep running and billing. Check with the commands in section 5 |
| 12. Host stop | After the last pattern, Terraform sets `loadgen_running=false` | `run-session.sh` `stop_host` (EXIT trap) | |

Run ids have the form `<id>-<profile>-r<rate>-<UTC stamp>`, for example `01-dedup-tuned-r10000-<UTC stamp>`.

## 3. How each number is computed

- **Latency** is the output record's broker timestamp minus `in_ts`. `in_ts` is the input record's broker timestamp that the engine copies into the output value. Both come from the MSK clock. See `stats.py` `latency_ms`.
- **Percentiles** (p50, p95, p99, p99.9, max) are exact at 1 ms resolution. They come from an integer histogram with nearest-rank lookup (`LatencyHistogram`).
- **Distinct keys** are counted exactly from 128-bit hashes of the output Kafka key (`DistinctCounter`).
- **expected_distinct** comes from the manifest, per pattern. For 01-dedup it is the unique events sent. For 02-unnest it is the sum of items over all orders. For 04-routing it is twice the events sent. For 06-tumbling and 07-sliding it is the number of (key, window) pairs in the expected side file. For 08-join it is the number of orders with a payment. See the pattern files in `harness/rtmpack_harness/patterns/`.
- **missing** is `expected_distinct - distinct_out_keys`.
- **sink_duplicates** is `out_records - distinct_out_keys`. For the window patterns it is reported as null, because updates are expected.
- **value_errors** is the number of outputs (or, for window patterns, final (key, window) pairs) that fail the pattern's value check. It is 0 for patterns without a check. The rules are in [CONTRACT.md](CONTRACT.md).
- **stable** is true when p50 and p99 each differ by at most 15 percent between the two halves of the measured window, relative to the larger value (`split_half_stable`). A half with no records makes it false.
- **throughput_eps** is the number of output records in the latency window divided by its length.
- **cost_per_hour_usd** and **vcpu** are computed by Terraform from the price variables (`variables.tf`, and the `patterns` output in `outputs.tf`) and passed to the collector. They are list prices, not billing data.

## 4. Reading order for the code

Each layer depends only on the ones above it.

Layer 1 is the rules.

1. [CONTRACT.md](CONTRACT.md): topics, event schema, latency definition, run parameters, tuned profiles (section 8), per-pattern rules (section 10).
2. [TUNING.md](TUNING.md): what fair means and the per-pattern protocol.

Layer 2 is one pattern end to end. Start with `02-unnest`, which is stateless and shorter.

1. `patterns/02-unnest/rtm/`. The transform function is the whole logic. `run()` reads from Kafka and writes to Kafka.
2. `patterns/02-unnest/flink/src/main/resources/`: the SQL file. Then the Java classes in `com.rtmpack.unnest` for how the SQL is loaded and run.
3. Then `01-dedup` (`patterns/01-dedup/rtm/` and `patterns/01-dedup/flink/`). This is the stateful case, with the watermark and state TTL.

Layer 3 is the harness.

1. `harness/rtmpack_harness/contract.py`: run parameters and their defaults, result field list.
2. `patterns/`: one file per pattern with what differs (input generator, output keys, expected count).
3. `events.py` and `schedule.py`: deterministic events and duplicate resends.
4. `loadgen.py`: `run_loadgen` (manifest, workers, timing) and `LoadWorker` (fixed send grid).
5. `collect.py` `collect_run`, then `stats.py` `OutputAccumulator`: the two windows and the split halves. Every reported number comes from here.
6. `cli.py`: how the `rtmpack` commands map to these functions.
7. Tests: `harness/tests/test_stats.py` and `test_patterns.py` show the expected behavior on small inputs.

Layer 4 is infrastructure.

1. `infra/terraform/envs/sandbox-usw2/patterns.tf`: the pattern list and the start and stop switches.
2. `infra/terraform/modules/databricks_rtm_job/main.tf`: cluster shape, Spark conf, slot checks.
3. `infra/terraform/modules/flink_app/main.tf`: parallelism, checkpointing, runtime properties, jar version pinning.
4. `infra/terraform/modules/loadgen_host/main.tf`: host, harness bundle, start and stop.
5. `infra/terraform/modules/network/main.tf` and `msk/main.tf`: how the workspace VPC is discovered and how MSK is set up.
6. `variables.tf` and `outputs.tf`: prices and the `patterns` output that the scripts read.

Layer 5 is orchestration.

1. `infra/scripts/run-benchmark.sh`: compare it with the table in section 2.
2. `infra/scripts/run-session.sh` and `update-harness.sh`.

## 5. Checklist for every run

Before the run, check the following.

- `aws sts get-caller-identity --profile <your-aws-profile>` and `databricks current-user me --profile <your-databricks-profile>` both succeed.
- `terraform -chdir=infra/terraform/envs/sandbox-usw2 plan` shows no changes, or only expected in-place updates. Never apply a plan that destroys anything you did not intend to remove.
- No engine is running from an earlier run (commands below).

During the run, check the following.

- The session log shows the probe that succeeded, then the load generator starting.
- Optional: open the RTM job run in the workspace and the Flink application in the AWS console. Both should show steady input and output rates.

After the run, check the following.

- `results/<run_id>.jsonl` has one line per engine. `missing` and `sink_duplicates` are 0 for both, and `stable` is true for both.
- In the collector output, `negative_latency_records`, `malformed_records` and `non_append_time_records` are 0.
- In the manifest, `delivery_errors` and `undelivered` are 0, and `max_lag_ms` is small.

Confirm the engines are stopped with these commands:

```bash
export AWS_PROFILE=<your-aws-profile> AWS_REGION=us-west-2
terraform -chdir=infra/terraform/envs/sandbox-usw2 output -json patterns     # job ids and app names
aws kinesisanalyticsv2 describe-application --application-name <flink_app> \
  --query ApplicationDetail.ApplicationStatus --output text                   # READY means stopped
databricks jobs list-runs --job-id <rtm_job_id> --active-only --profile <your-databricks-profile>   # empty means stopped
```

To stop everything except MSK, run:

```bash
terraform -chdir=infra/terraform/envs/sandbox-usw2 plan -out=stop.tfplan -var 'running_patterns=[]' -var loadgen_running=false
# check the plan says 0 to destroy, then:
terraform -chdir=infra/terraform/envs/sandbox-usw2 apply stop.tfplan
databricks jobs cancel-all-runs --job-id <rtm_job_id> --profile <your-databricks-profile>
```
