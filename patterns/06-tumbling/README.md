# 06 Tumbling

## About the pattern

The pattern counts events per key in fixed 60-second windows. Each input event has a `key` (100,000 different keys) and an `amount` in cents. For each key and 60-second window, aligned to the epoch and using the event time `event_ts`, the job keeps a running `count`, a running `sum_amount` and the largest `in_ts` seen so far.

The job writes an update record as soon as an input changes the aggregate. It does not wait for the window to close, so the consumer always sees the count so far.

For example, three events for `user-00042` with `event_ts` inside the window that starts at 1762600020000 and amounts of 500, 1200 and 300 cents produce three updates for the same window:

```json
{"key": "user-00042", "window_start_ms": 1762600020000, "window_end_ms": 1762600080000, "count": 1, "sum_amount": 500,  "in_ts": 1762600031004, "engine": "rtm"}
{"key": "user-00042", "window_start_ms": 1762600020000, "window_end_ms": 1762600080000, "count": 2, "sum_amount": 1700, "in_ts": 1762600044120, "engine": "rtm"}
{"key": "user-00042", "window_start_ms": 1762600020000, "window_end_ms": 1762600080000, "count": 3, "sum_amount": 2000, "in_ts": 1762600052877, "engine": "rtm"}
```

Each update has the Kafka key `<key>:<window_start_ms>`. The last record with that Kafka key is the current value of the window. The exact input, rule and output format are in [Benchmark setup](#benchmark-setup).

## Why sub-second processing matters for this pattern

The consumers of this topic are rate limiters, quota enforcement and threshold alerts. They act within the current window. When the count for a key reaches the limit, the next request from that key must be blocked. The deadline is the time until the next request arrives, so a count that arrives seconds late lets through requests that should have been blocked.

When the counts are only shown on a dashboard or used for reports, near-real-time streaming into Delta is the better fit, because no one acts on an individual update.

## How each engine implements it

| Engine | Implementation | Details |
|---|---|---|
| Spark Real-Time Mode (RTM) | `withWatermark("event_time", "10 seconds")`, then `groupBy(window(event_time, "60 seconds"), key)` in update output mode | [rtm/README.md](rtm/README.md) |
| Managed Service for Apache Flink | A regular `GROUP BY` on a key built from the event key and the window start, state TTL 120 s, written to an `upsert-kafka` sink | [flink/README.md](flink/README.md) |

- On RTM, the query has two stages: the Kafka source and the shuffle for the aggregation. RTM schedules all stages at once, so each task needs a slot: 8 source tasks plus 4 shuffle tasks. Output mode must be update. The watermark drops input that is older than the watermark and bounds the window state.
- On Flink, the window table-valued function (`TUMBLE`) emits a result only when the window closes, so it is not used. The window start is computed from `event_ts`, and the aggregate emits an update for every input. Mini-batch stays off. The grouping column is the output key `<key>:<window_start_ms>` (see [Benchmark setup](#benchmark-setup)).
- On Flink, there is no watermark, so state is dropped by the 120 s state TTL after the last update of a group.

## Benchmark setup

Topics are `rtmpack.06-tumbling.input`, `rtmpack.06-tumbling.out.rtm` and `rtmpack.06-tumbling.out.flink`. The shared rules are in sections 2, 5, 8 and 9 of [docs/CONTRACT.md](../../docs/CONTRACT.md).

| Item | Value |
|---|---|
| Input | The shared event (section 2 of the contract) plus `amount` (integer cents, uniform 100 to 50,000). No duplicates (`dup_ratio = 0`) |
| Aggregation | Per `key` and 60-second tumbling window on `event_ts`, aligned to the epoch: `count` and `sum_amount` |
| Output key | `<key>:<window_start_ms>` |
| Output fields | `key`, `window_start_ms`, `window_end_ms`, `count`, `sum_amount`, `in_ts` (the largest `in_ts` in the window so far), `engine` |
| Emission | One update for every input that changes the aggregate, so the running count is available before the window closes |
| Key cardinality | 100,000 |
| Input rate | 10,000 events per second |
| Record size | 1024 bytes |
| Partitions | 8 (input and output topics) |
| Duration | 1800 s |
| Warmup | 300 s, excluded from latency statistics |
| Delivery | at-least-once on both sinks |
| Runtime | RTM on DBR 18 LTS (`18.x-scala2.13`), Photon off. Flink on `FLINK-2_3` |
| Correctness | The load generator writes the expected `count` and `sum_amount` of every (key, window) pair as a side file, `results/<run_id>.expected.*`. The aggregate rules are in section 9 of the contract. The collector counts missing pairs, and counts as `value_errors` the pairs whose final update (the one with the highest `count`) differs from the expected values |

The Flink group is the output key `<key>:<window_start_ms>`, which defines the same groups as (key, window start). This form makes the aggregate's upsert key equal to the sink's primary key. The changelog therefore holds only insert and update-after records, and each input produces exactly one Kafka record.

Each engine is sized as follows.

| Engine | Sizing |
|---|---|
| Spark RTM | 8 source tasks and 4 shuffle partitions give 12 tasks. 1 driver and 4 workers of m5.xlarge (4 slots each) give 16 slots, so slot use is 75 percent. |
| Managed Flink | Parallelism 8, 1 parallelism per KPU: 8 KPUs plus 1 orchestration KPU. Checkpoint every 60 s, state TTL 120 s. Autoscaling off. |

The RTM tuned profile uses `realTime="5 minutes"` and a Kafka producer `linger.ms` of 0. Flink also uses `linger.ms` 0.

## How to run

Local tests do not need a cluster or a broker. They need Java 17, Maven, and a Python with PySpark 4.1 and pytest. The versions are in `patterns/requirements-test.txt`. Set `RTMPACK_TEST_PYTHON` to that interpreter, then run:

```bash
infra/scripts/test-pattern.sh 06-tumbling
```

The full benchmark run:

```bash
infra/scripts/run-benchmark.sh --pattern 06-tumbling
```

The RTM job has one variant flag for the window state, `--state-horizon`, which sets the watermark delay. It changes how late an event can arrive and still be counted, and how long window state is kept. Pass it as an extra job parameter with its own checkpoint:

```bash
infra/scripts/run-benchmark.sh --pattern 06-tumbling \
  --tf-var 'rtm_extra_params={"06-tumbling"=["--state-horizon","30 seconds","--checkpoint-location","/Volumes/<catalog>/<schema>/<volume>/checkpoints/06-tumbling-rtm-horizon30-run1"]}'
```

Other job flags are `--partitions` (shuffle partitions of the aggregation, default 4, which sets the number of aggregation tasks. The cluster size comes from the Terraform entry of the pattern, so raising it also needs more workers), `--checkpoint-interval` (real-time trigger interval) and `--kafka-linger-ms`. The full list is in [rtm/README.md](rtm/README.md).

The three variant variables in `infra/` are `rtm_extra_params` (above), `rtm_spark_version_overrides` (pattern id to Databricks runtime) and `rtm_spark_conf_overrides` (pattern id to extra cluster Spark conf, merged over the pattern's own). The Spark conf override is a map of maps.

The overrides apply to that run only. The next apply without the variables reverts them. See [../../infra/README.md](../../infra/README.md) for all variant variables.

