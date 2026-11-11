# 02 Array unnest

## About the pattern

Each input record is an order that carries an `items` array of 1 to 5 elements. The job emits one output record for every element of that array.

The input is orders on `rtmpack.02-unnest.input`. Each order has `order_id`, `key`, `event_ts`, `items` (an array of `{sku, qty, price}`) and `payload`. There are no duplicates.

The output is one record per array element, on `rtmpack.02-unnest.out.rtm` or `rtmpack.02-unnest.out.flink`. The Kafka key is `<order_id>:<item_idx>`, where `item_idx` is the 0-based position in the array. The value carries `order_id`, `key`, `event_ts`, `item_idx`, `sku`, `qty`, `price`, `in_ts` and `engine`. `in_ts` is the Kafka timestamp of the order, so every record from one order has the same `in_ts`.

The transformation is stateless and handles each record on its own. An order with an empty or null `items` array produces no output.

For example, the order `o-17` with items `[{sku: "A", qty: 1, price: 5.0}, {sku: "B", qty: 2, price: 3.5}]` produces two records. One has key `o-17:0` and `sku = "A"`. The other has key `o-17:1` and `sku = "B"`.

## Why sub-second processing matters for this pattern

Item-level fan-out sits in front of services that act on each item while the customer flow is still open:

- Inventory reservation. Each SKU must be reserved before checkout continues.
- Per-item fraud or risk checks. Each item is scored inside the authorization window.
- IoT per-reading handlers. Each reading in a batch message is handled as it arrives.

These services subscribe to the unnested topic and act on every record. The deadline is set by the process waiting on the item. They do not query a table, so the output goes to Kafka. A Delta copy for analytics can be written separately with a standard stream. If no system acts on each item within a short deadline, near-real-time Structured Streaming into Delta is the better fit.

## How each engine implements it

| Engine | Key construct | Details |
|---|---|---|
| Spark Real-Time Mode (RTM) | `from_json`, then `posexplode(items)`, then `to_json` | [rtm/README.md](rtm/README.md) |
| Amazon Managed Service for Apache Flink | `CROSS JOIN UNNEST(o.items) WITH ORDINALITY`, `item_idx = ord - 1` | [flink/README.md](flink/README.md) |

In RTM, the job parses the Kafka value with `from_json`. It calls `posexplode(items)` to get one row per element, with its 0-based position. It then writes the rows to Kafka with `to_json`, in update output mode. The query is a single stage with no shuffle. The number of tasks therefore equals the number of input partitions, and RTM needs one slot per task. The `--parse variant` argument changes only how the value is parsed. It uses `try_parse_json` into a VARIANT and reads each field with `try_variant_get`. The `posexplode` step is the same, and so is the output.

In Flink, the job runs one Flink SQL `INSERT` with `CROSS JOIN UNNEST(o.items) WITH ORDINALITY AS t(sku, qty, price, ord)`. `WITH ORDINALITY` numbers elements from 1, so the query uses `ord - 1` for `item_idx` and for the key suffix. The plan is Kafka source, correlate (the UNNEST), calc and Kafka sink. There is no `GROUP BY`, keyed join, window or deduplication. The plan therefore has no shuffle (no `Exchange` node), and Flink chains the operators into one task per parallel instance. The unit test asserts that the plan has no `Exchange`. Managed Flink fixes the network buffer timeout at 100 ms and does not allow application code to change it. With no exchange, the timeout has no effect on this plan. See [flink/README.md](flink/README.md).

Both sinks are at-least-once and use `linger.ms = 0`.

## Benchmark setup

Shared rules (topics, event schema, latency, run parameters, tuned profiles) are in sections 1 to 8 of [docs/CONTRACT.md](../../docs/CONTRACT.md). The rules below apply to this pattern.

| Parameter | Value |
|---|---|
| Input topic | `rtmpack.02-unnest.input`, Kafka key `key`, 8 partitions, `LogAppendTime` |
| Input fields | `order_id` (UUID, unique per order), `key` (customer key), `event_ts` (epoch ms), `items` (array of `{sku: string, qty: int, price: double}`), `payload` (padding to the record size) |
| Rate | 10,000 orders per second, which gives about 30,000 output records per second |
| Items per order | 1 to 5, uniform |
| Record size | 1,024 bytes per input record (JSON) |
| Duplicates | None (`dup_ratio = 0`) |
| Key cardinality | 100,000 |
| Run length | 1,800 s, of which the first 300 s are warm-up and excluded from latency statistics |
| Seed | 42 (probe bursts use seeds 9001 and up) |
| Output topics | `rtmpack.02-unnest.out.rtm` or `rtmpack.02-unnest.out.flink`, one record per array element, Kafka key `<order_id>:<item_idx>` |
| Output fields | `order_id`, `key`, `event_ts`, `item_idx` (0-based position), `sku`, `qty`, `price`, `in_ts`, `engine` |
| State | None (stateless pattern) |

Latency is the Kafka append time of each output record minus the `in_ts` of its order. Both timestamps come from the MSK broker clock.

For correctness, `expected_distinct` is the sum of `len(items)` over every order sent. `missing` and `sink_duplicates` are computed on the output keys `<order_id>:<item_idx>`, as in section 6 of the contract. Both must be 0.

The harness sends 20-second probe bursts at 200 events per second until both output topics contain records. It then waits 90 s before the measured load. After collection, both engines are stopped through Terraform, and active RTM job runs are cancelled.

Spark RTM is sized as follows.

| Setting | Value |
|---|---|
| Runtime | DBR 18 LTS (`18.x-scala2.13`), Photon off |
| Compute | Classic jobs compute, dedicated access mode, autoscaling off, on-demand |
| Nodes | 1 driver and 3 workers, `m5.xlarge` (12 task slots) |
| Tasks | 8 source tasks and no shuffle stage, 8 of 12 slots (67 percent), within the 75 percent limit of contract section 8 |
| Spark conf | `spark.databricks.streaming.realTimeMode.enabled=true`, `spark.sql.shuffle.partitions=8`. The shuffle setting does not affect this query, which has no shuffle |
| Trigger | `realTime="300 seconds"` (documented default) |
| Kafka | SASL_SSL, SCRAM-SHA-512, `startingOffsets=latest`, sink `kafka.linger.ms=0` |
| Checkpoint | A Unity Catalog Volume path of its own for each run configuration |

Managed Flink is sized as follows.

| Setting | Value |
|---|---|
| Runtime | `FLINK-2_3`, Java 17 |
| Compute | Parallelism 8, ParallelismPerKPU 1, autoscaling off: 8 KPU plus 1 orchestration KPU, 9 KPU |
| Table options | `table.local-time-zone = UTC` |
| Kafka connector | `flink-connector-kafka` 4.0.1-2.0, SASL_SSL, SCRAM-SHA-512, `scan.startup.mode = latest-offset` |
| Sink | `sink.delivery-guarantee = at-least-once`, `linger.ms = 0`, key `kafka_key` (`<order_id>:<item_idx>`), `value.fields-include = EXCEPT_KEY` |
| Checkpoints | Every 60 s. Only source offsets are checkpointed, as the job has no keyed state |
| Network buffer timeout | 100 ms (Managed Flink default, not changed) |

The shared infrastructure is Amazon MSK 3.9.x (4 brokers, `kafka.m7g.large`, two AZs) and one `c6i.2xlarge` host for the load generator and collector, all in the same VPC.

Engines start by adding the pattern id to `running_patterns` in `infra/terraform/envs/sandbox-usw2` and applying, and stop by removing it. `run-benchmark.sh` does this as part of a run.

## How to run

Local tests cover the RTM job, the Flink build and tests, and the harness. Nothing in the cloud is touched. Set `RTMPACK_TEST_PYTHON` to an interpreter that has `patterns/requirements-test.txt` installed, then run:

```bash
infra/scripts/test-pattern.sh 02-unnest
```

The full benchmark run starts both engines through Terraform, sends probes, runs the load, collects results and stops both engines:

```bash
infra/scripts/run-benchmark.sh --pattern 02-unnest
```

### RTM variants

A variant changes one RTM job argument for one run. Pass the Terraform variable `rtm_extra_params` with `--tf-var`. It maps the pattern id to extra job arguments, which are appended after the pattern's own arguments. A repeated argument takes the last value. Always add a `--checkpoint-location` of its own, so the variant does not reuse the checkpoint of the standard run. The next apply without the variable reverts the change.

The parse variant reads the Kafka value as a VARIANT (`try_parse_json`, `try_variant_get`) instead of `from_json`:

```bash
infra/scripts/run-benchmark.sh --pattern 02-unnest \
  --tf-var 'rtm_extra_params={"02-unnest"=["--parse","variant","--checkpoint-location","/Volumes/<catalog>/<schema>/<volume>/checkpoints/02-unnest-rtm-variant-run1"]}'
```

| Flag | Default | What it changes |
|---|---|---|
| `--parse` | `from-json` | `variant` parses the value with `try_parse_json` into a VARIANT and reads the fields with `try_variant_get`. The transformation after parsing and the output are the same |
| `--checkpoint-interval` | `5 minutes` | Real-time trigger interval, also the checkpoint interval. Set it with the Terraform variable `rtm_batch_interval_seconds` |
| `--kafka-linger-ms` | unset (Terraform passes `0`) | Producer `linger.ms` of the Kafka sink |

The other job arguments (`--secret-scope`, `--input-topic`, `--output-topic`, `--checkpoint-location`) are set by Terraform. See [rtm/README.md](rtm/README.md).

To build the Flink jar on its own, run `infra/scripts/build-flink.sh 02-unnest`. See [flink/README.md](flink/README.md).

