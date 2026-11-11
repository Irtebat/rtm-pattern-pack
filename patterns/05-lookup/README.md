# 05 Lookup

## About the pattern

Each input event carries a `merchant_id`. The pattern joins the event with a small reference table of 10,000 merchants. It writes the event again with three extra attributes from the table: `risk_tier`, `category` and `country`. An event whose `merchant_id` is not in the table, or is null, produces no output (inner join).

The reference table is fixed for the whole run. At about 240 KB, it is small enough to be held in memory by every task.

For example, this input value is shortened:

```json
{"event_id": "7f3c...", "key": "user-00042", "event_ts": 1762600000123, "merchant_id": "m-00017"}
```

If merchant `m-00017` is in the table as a high-risk fuel merchant in Germany, the output value for the same event is:

```json
{"event_id": "7f3c...", "key": "user-00042", "event_ts": 1762600000123, "merchant_id": "m-00017",
 "risk_tier": "high", "category": "fuel", "country": "DE", "in_ts": 1762600000130, "engine": "rtm"}
```

The Kafka key of the output is `event_id`. The exact input, reference table rule and output format are in [Benchmark setup](#benchmark-setup).

## Why sub-second processing matters for this pattern

The consumer of the enriched topic is a decision service that needs context attached to each event before it acts. Typical examples are a payment decision that looks at the merchant risk tier, or an offer service that needs product attributes. The service reads each enriched record from Kafka and answers a request that is still open. The enrichment therefore has to finish inside that request's budget, which is often well below a second. If the enrichment is late, the service either waits or decides without the context.

When no service acts on each enriched record, and the data is only used for reporting or analytics, near-real-time streaming into Delta is the better fit. A micro-batch stream that joins the events with the reference table and lands them in a Delta table needs no sub-second engine.

## How each engine implements it

Both engines hold the full reference table in memory on every parallel task, and neither needs a shuffle.

| Engine | Implementation | Details |
|---|---|---|
| Spark Real-Time Mode (RTM) | Stream-static inner join with `broadcast()` on the table, read from `merchants.csv` in a Unity Catalog volume | [rtm/README.md](rtm/README.md) |
| Managed Service for Apache Flink | The CSV is packaged in the jar and loaded into a map in the `open()` method of a table function. The SQL calls it per row with `CROSS JOIN LATERAL TABLE(...)` | [flink/README.md](flink/README.md) |

- On RTM, the documentation supports stream-static joins for static tables up to 10 MB. The table here is about 240 KB. Larger reference data does not fit this pattern. RTM schedules all stages at once, so each task needs a slot. This query has one stage and no shuffle, so the number of tasks equals the number of input partitions.
- On RTM, the reference file must be in the volume before the job starts. The job fails at start if the path does not exist. To change the table, update the file and restart the query.
- On Flink, the table is fixed at build time. A change needs a new jar and an application update.
- On Flink, the plan is source, correlate, calc and sink, chained into one task per parallel instance. A table function is used so that an unknown id drops the event without a filter, and so that the lookup is called exactly once per record.

## Benchmark setup

Topics are `rtmpack.05-lookup.input`, `rtmpack.05-lookup.out.rtm` and `rtmpack.05-lookup.out.flink`. The shared rules are in sections 2, 5, 6 and 8 of [docs/CONTRACT.md](../../docs/CONTRACT.md).

| Item | Value |
|---|---|
| Input | The shared event (section 2 of the contract) plus `merchant_id`, drawn uniformly from the 10,000 merchants. No duplicates |
| Output key | `event_id` |
| Output fields | `event_id`, `key`, `event_ts`, `merchant_id`, `risk_tier`, `category`, `country`, `in_ts`, `engine` |
| Input rate | 10,000 events per second |
| Record size | 1024 bytes |
| Partitions | 8 (input and output topics) |
| Duration | 1800 s |
| Warmup | 300 s, excluded from latency statistics |
| Duplicates | none (`dup_ratio = 0`) |
| Reference table | 10,000 merchants in `data/merchants.csv`, about 240 KB, below the 10 MB broadcast limit. Built by the rule below |
| Delivery | at-least-once on both sinks |
| Runtime | RTM on DBR 18 LTS (`18.x-scala2.13`), Photon off. Flink on `FLINK-2_3` |
| Correctness | The number of distinct output `event_id`s must equal the number sent (section 6). No output may carry merchant attributes that differ from the table (`value_errors` = 0) |

The reference file is produced by `rtmpack refdata --pattern 05-lookup`. It writes the header `merchant_id,risk_tier,category,country` and one row for each `i` from 0 to 9999. Each row is built as follows:

- `merchant_id` is `m-` followed by `i` written as five digits.
- `h` is the first 4 bytes of SHA-256 of the UTF-8 string `merchant|` + `i`, read as an unsigned big-endian integer.
- `risk_tier` is `["low", "medium", "high"][h mod 3]`.
- `category` is `["grocery", "fuel", "travel", "electronics", "dining", "apparel", "health", "digital"][(h >> 8) mod 8]`.
- `country` is `["US", "GB", "DE", "FR", "IN", "JP", "BR", "CA", "AU", "SG"][(h >> 16) mod 10]`.

Each engine is sized as follows.

| Engine | Sizing |
|---|---|
| Spark RTM | 8 tasks (one stage, no shuffle). 1 driver and 3 workers of m5.xlarge (4 slots each) give 12 slots, so slot use is 67 percent. |
| Managed Flink | Parallelism 8, 1 parallelism per KPU: 8 KPUs plus 1 orchestration KPU. Checkpoint every 60 s. Autoscaling off. |

The RTM tuned profile uses `realTime="5 minutes"` and a Kafka producer `linger.ms` of 0. Flink also uses `linger.ms` 0.

## How to run

Local tests need no cluster or broker. They need Java 17, Maven, and a Python with PySpark 4.1 and pytest. The versions are in `patterns/requirements-test.txt`. Set `RTMPACK_TEST_PYTHON` to that interpreter, then run:

```bash
infra/scripts/test-pattern.sh 05-lookup
```

The full benchmark run:

```bash
infra/scripts/run-benchmark.sh --pattern 05-lookup
```

The RTM job has one variant flag, `--reference-mode`. It changes how the static side of the join is held. It is passed as an extra job parameter with its own checkpoint, so the variant does not reuse the state of the plain run:

| Value | What it changes |
|---|---|
| `read` (default) | The static side is a file scan in the query plan. |
| `cached` | The same DataFrame is cached in memory and materialized before the query starts. |
| `local` | The rows are collected to the driver once and embedded in the plan as a local relation, so the file is not read again. |

```bash
infra/scripts/run-benchmark.sh --pattern 05-lookup \
  --tf-var 'rtm_extra_params={"05-lookup"=["--reference-mode","cached","--checkpoint-location","/Volumes/<catalog>/<schema>/<volume>/checkpoints/05-lookup-rtm-cached-run1"]}'

infra/scripts/run-benchmark.sh --pattern 05-lookup \
  --tf-var 'rtm_extra_params={"05-lookup"=["--reference-mode","local","--checkpoint-location","/Volumes/<catalog>/<schema>/<volume>/checkpoints/05-lookup-rtm-local-run1"]}'
```

All three modes keep the broadcast join and use the same table. `rtm_extra_params` is appended after the pattern's own parameters, so a repeated `--checkpoint-location` takes the last value. The overrides apply to that run only. The next apply without the variable reverts them. The Spark conf override variable is shown in [../06-tumbling/README.md](../06-tumbling/README.md). All variant variables are described in [../../infra/README.md](../../infra/README.md).

