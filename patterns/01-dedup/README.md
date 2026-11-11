# 01 Deduplication

## About the pattern

The job reads a stream of events from Kafka and keeps only the first record for each `event_id`. Copies of an event that arrive within the state horizon (60 seconds) are dropped.

The input is the contract event (`event_id`, `key`, `seq`, `event_ts`, `is_dup`, `payload`) on `rtmpack.01-dedup.input`. 20 percent of the events are sent again as duplicates. Each duplicate arrives 0 to 30 seconds after the original and has the same `event_id` and `event_ts`.

The output has the same fields, plus `in_ts` (the Kafka timestamp of the input record) and `engine`. It goes to `rtmpack.01-dedup.out.rtm` or `rtmpack.01-dedup.out.flink`, with the Kafka key `event_id`.

The first arrival of an `event_id` is written. Any later arrival of the same `event_id` within the 60-second horizon is not.

For example, three records arrive for `event_id = a1` at 0 s, 4 s and 12 s, and one record arrives for `b2` at 5 s. The output holds the record for `a1` from 0 s and the record for `b2`. The records for `a1` at 4 s and 12 s are dropped.

## Why sub-second processing matters for this pattern

Deduplication sits in front of services that must act exactly once per event while a request is still open:

- Payment authorization and billing. A duplicate is a double charge.
- Customer notifications. A duplicate is a second message.
- Fraud scoring. A duplicate skews velocity counts.

These services subscribe to the deduplicated topic and act on each record. They do not query a table, so the output goes to Kafka. The deadline is the time the customer or the calling system waits for the answer. A standard stream can land the same topic in Delta for audit and analytics. If the deduplicated data is only needed for reporting, near-real-time streaming into Delta is the better fit.

## How each engine implements it

| Engine | Key construct | Details |
|---|---|---|
| Spark Real-Time Mode (RTM) | `withWatermark("event_time", "60 seconds").dropDuplicates(["event_id", "event_time"])` | [rtm/README.md](rtm/README.md) |
| Amazon Managed Service for Apache Flink | `ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY proc_time) = 1`, state TTL 60 s | [flink/README.md](flink/README.md) |

In RTM, the job parses the Kafka value with `from_json`. It turns `event_ts` into a timestamp column `event_time`, sets a 60-second watermark on it and calls `dropDuplicates` on `event_id` and `event_time`. A duplicate carries the same `event_ts` as its original, so the pair identifies the event. The watermark lets Spark evict old state. This form runs on every DBR version used here. The variant `--dedup-operator within-watermark` uses `dropDuplicatesWithinWatermark` on `event_id`, which removes duplicates by id alone. Not every DBR version supports it, so the standard run does not use it. The query has two stages (the Kafka source, then the shuffle for the dedup state). RTM needs one slot per task across all stages.

In Flink, the job is a Table API application that runs one Flink SQL `INSERT`. `ROW_NUMBER()` ordered by processing time, ascending, keeps the first row per `event_id`, so the output is append-only. State expires with `table.exec.state.ttl = 60 s`. Because the query is keyed, Flink hash-partitions records by `event_id`. Records that cross that exchange wait in a network buffer. Managed Flink fixes the buffer timeout at 100 ms. That property is not on the [list of settings customers can modify](https://docs.aws.amazon.com/managed-flink/latest/java/reference-modifiable-settings.html), and changing it needs an AWS support case. The pack runs Managed Flink as delivered.

Both sinks are at-least-once. The RTM Kafka sink cannot be exactly-once, so Flink uses `at-least-once` to keep the comparison equal. The two engines evict state differently. The Flink TTL depends on access and processing time. The RTM watermark follows event time, so an idle or skewed partition can hold the watermark back and keep state longer.

## Benchmark setup

Shared rules (topics, event schema, latency, run parameters, tuned profiles) are in sections 1 to 8 of [docs/CONTRACT.md](../../docs/CONTRACT.md). The rules below apply to this pattern.

| Parameter | Value |
|---|---|
| Input topic | `rtmpack.01-dedup.input`, section 2 event, Kafka key `key`, 8 partitions, `LogAppendTime` |
| Rate | 10,000 unique events per second |
| Duplicates | 20 percent of events are sent again once, after a delay uniform between 0 and 30 s. A resend has the same `event_id`, `key`, `seq` and `event_ts`, and `is_dup = true` |
| Record size | 1,024 bytes (JSON) |
| Key cardinality | 100,000 |
| Run length | 1,800 s, of which the first 300 s are warm-up and excluded from latency statistics |
| Seed | 42 (probe bursts use seeds 9001 and up) |
| Output topic | `rtmpack.01-dedup.out.rtm` or `rtmpack.01-dedup.out.flink`, Kafka key `event_id` |
| Output | The first record seen for each `event_id`, with the section 3 fields |
| State horizon | 60 s, longer than the maximum duplicate delay. The standard run does not use `dropDuplicatesWithinWatermark`. See RTM variants |
| Delivery | At-least-once on both sinks |

Latency follows section 4 of the contract.

For correctness, `expected_distinct` is the number of unique `event_id`s the generator sent during the run. The collector compares output keys with it over the whole run, from run start to run end plus the maximum duplicate delay, as in section 6. `missing` is the expected distinct count minus the distinct output keys. `sink_duplicates` is the number of output records minus the distinct output keys. Both must be 0.

The harness sends 20-second probe bursts at 200 events per second until both output topics contain records. It then waits 90 s before the measured load. After collection, both engines are stopped through Terraform, and active RTM job runs are cancelled.

Spark RTM is sized as follows.

| Setting | Value |
|---|---|
| Runtime | DBR 18 LTS (`18.x-scala2.13`), Photon off |
| Compute | Classic jobs compute, dedicated access mode, autoscaling off, on-demand |
| Nodes | 1 driver and 4 workers, `m5.xlarge` (16 task slots) |
| Tasks | 8 source tasks plus 4 shuffle tasks (`--partitions 4`), 12 of 16 slots (75 percent) |
| Spark conf | `spark.databricks.streaming.realTimeMode.enabled=true`, `spark.sql.shuffle.partitions=4` |
| Trigger | `realTime="300 seconds"` (documented default) |
| Kafka | SASL_SSL, SCRAM-SHA-512, `startingOffsets=latest`, sink `kafka.linger.ms=0` |
| Checkpoint | A Unity Catalog Volume path. The shuffle partition count is stored in the checkpoint of a stateful query, so a new count needs a new path |

Managed Flink is sized as follows.

| Setting | Value |
|---|---|
| Runtime | `FLINK-2_3`, Java 17 |
| Compute | Parallelism 8, ParallelismPerKPU 1, autoscaling off: 8 KPU plus 1 orchestration KPU, 9 KPU |
| Table options | `table.exec.state.ttl = 60000 ms`, `table.local-time-zone = UTC` |
| Kafka connector | `flink-connector-kafka` 4.0.1-2.0, SASL_SSL, SCRAM-SHA-512, `scan.startup.mode = latest-offset` |
| Sink | `sink.delivery-guarantee = at-least-once`, `linger.ms = 0`, key `event_id` |
| Checkpoints | Every 60 s |
| Network buffer timeout | 100 ms (Managed Flink default, not changed) |

The shared infrastructure is Amazon MSK 3.9.x (4 brokers, `kafka.m7g.large`, two AZs) and one `c6i.2xlarge` host for the load generator and collector, all in the same VPC.

## How to run

Local tests cover the RTM job, the Flink build and tests, and the harness. Nothing in the cloud is touched. Set `RTMPACK_TEST_PYTHON` to an interpreter that has `patterns/requirements-test.txt` installed, then run:

```bash
infra/scripts/test-pattern.sh 01-dedup
```

The full benchmark run starts both engines through Terraform, sends probes, runs the load, collects results and stops both engines:

```bash
infra/scripts/run-benchmark.sh --pattern 01-dedup
```

### RTM variants

A variant changes one RTM job argument for one run. Pass the Terraform variable `rtm_extra_params` with `--tf-var`. It maps the pattern id to extra job arguments, which are appended after the pattern's own arguments. A repeated argument takes the last value. Always add a `--checkpoint-location` of its own, so the variant does not reuse the state of the standard run. The next apply without the variable reverts the change.

The operator variant uses `dropDuplicatesWithinWatermark(["event_id"])` instead of `dropDuplicates`:

```bash
infra/scripts/run-benchmark.sh --pattern 01-dedup \
  --tf-var 'rtm_extra_params={"01-dedup"=["--dedup-operator","within-watermark","--checkpoint-location","/Volumes/<catalog>/<schema>/<volume>/checkpoints/01-dedup-rtm-within-watermark-run1"]}'
```

| Flag | Default | What it changes |
|---|---|---|
| `--dedup-operator` | `drop-duplicates` | `within-watermark` swaps in `dropDuplicatesWithinWatermark`, the operator that the RTM reference page lists. Check that the DBR version supports it |
| `--partitions` | `8` (the Terraform setup passes `4`) | Shuffle partitions of the dedup stage, which sets the task count and so the slots needed. The Terraform cluster size must still cover source tasks plus this value |
| `--state-horizon` | `60 seconds` | Watermark delay, which is how long dedup state is kept. Keep it above the maximum duplicate delay |
| `--checkpoint-interval` | `60` seconds in the script, `300 seconds` from Terraform | Real-time trigger interval, which is also the checkpoint interval. Set it with the Terraform variable `rtm_batch_interval_seconds` |
| `--kafka-linger-ms` | unset (Terraform passes `0`) | Producer `linger.ms` of the Kafka sink |

Each of these can be passed the same way as `--dedup-operator` above. The other job arguments (`--secret-scope`, `--input-topic`, `--output-topic`, `--checkpoint-location`) are set by Terraform. See [rtm/README.md](rtm/README.md).

To build the Flink jar on its own, run `infra/scripts/build-flink.sh 01-dedup`. See [flink/README.md](flink/README.md).

