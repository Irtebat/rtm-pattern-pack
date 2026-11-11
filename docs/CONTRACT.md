# Benchmark Contract

Every pattern, engine and harness component in this pack follows this contract. Change it only on purpose, and update all components when you do.

Rules that only one pattern uses are in that pattern's README, under Benchmark setup. See section 10.

## 1. Kafka (Amazon MSK)

- The cluster is Amazon MSK, provisioned, on Kafka 3.9.x. It has 4 brokers (kafka.m7g.large by default). The brokers are spread across the two private subnets of the workspace VPC in us-west-2.
- MSK, the Flink application and the load generator host all run in the workspace data-plane VPC. Every client reaches the brokers over the private network. The latency path has no peering and no public endpoint.
- Authentication is SASL/SCRAM-SHA-512 over TLS.
  - The private bootstrap is on port 9096.
  - The public bootstrap is on port 9196. It is used only if the network mode requires it.
- Credentials are stored in AWS Secrets Manager under `AmazonMSK_rtmpack`, as JSON `{"username": "...", "password": "..."}`.
  - They are copied into the Databricks secret scope `rtmpack`, under the keys `kafka-bootstrap`, `kafka-username` and `kafka-password`.
  - The Flink application gets them as runtime properties in the property group `kafka`, under the keys `bootstrap.servers`, `username` and `password`.
- Topics are named `rtmpack.<id>.input` and `rtmpack.<id>.out.<engine>`. `<id>` is the pattern id, the folder name under `patterns/` (for example `01-dedup`). `<engine>` is `rtm` or `flink`. Patterns with several topics add a suffix (section 9).
- Every topic uses these settings:
  - `message.timestamp.type=LogAppendTime`
  - `retention.ms=43200000` (12 hours)
  - replication factor 3
  - The partition count is a per-run parameter, 8 by default. Input and output topics use the same count.

## 2. Input event (JSON value, UTF-8)

| Field | Type | Meaning |
|---|---|---|
| `event_id` | string (UUID) | Unique event identity. Duplicates reuse it. |
| `key` | string | Business key (for example `user-00042`), used by keyed patterns. The cardinality is a generator parameter. |
| `seq` | long | Per-key sequence number assigned by the generator. |
| `event_ts` | long | Event time, epoch milliseconds, from the generator clock. |
| `is_dup` | boolean | `true` on a resent duplicate. |
| `payload` | string | Padding that brings the record to the target size (default 1024 bytes). |

The Kafka record key is `key`, UTF-8.

## 3. Output event (JSON value)

Every output contains the input fields the pattern keeps, plus:

| Field | Type | Meaning |
|---|---|---|
| `in_ts` | long | Kafka timestamp of the input record (broker LogAppendTime, epoch ms), read from the source record metadata |
| `engine` | string | `rtm` or `flink` |

The Kafka record key is pattern-specific. Each pattern's Benchmark setup gives its key.

## 4. Latency definition

`latency_ms = output record Kafka timestamp (LogAppendTime) - in_ts`

Both timestamps come from the MSK broker clock, so clock differences between the engine hosts do not affect the result. The measurement runs from input append to output append. It covers engine read, processing, state and sink write.

## 5. Run parameters

| Parameter | Default |
|---|---|
| `rate_eps` | 10000 events per second |
| `record_bytes` | 1024 |
| `partitions` | 8 |
| `key_cardinality` | 100000 |
| `duration_s` | 1800 |
| `warmup_s` | 300 (excluded from statistics) |
| `dup_ratio` | 0.2 (share of events resent as duplicates) |
| `dup_delay_ms` | uniform between 0 and 30000 |
| `seed` | 42 |
| Checkpoint interval | Per engine, from the tuned profile in section 8: RTM `realTime="5 minutes"`, Managed Flink 60 s |

Both sinks use at-least-once delivery. RTM's Kafka sink is at-least-once, so Flink uses `DeliveryGuarantee.AT_LEAST_ONCE` to keep the comparison equal.

## 6. Results record

The collector writes one JSON line per (run, engine) to `results/<run_id>.jsonl`. It can optionally load the lines into a Delta table:

`run_id, pattern, engine, engine_version, started_at, rate_eps, record_bytes, partitions, compute_desc, vcpu, cost_per_hour_usd, out_records, distinct_out_keys, expected_distinct, missing, sink_duplicates, p50_ms, p95_ms, p99_ms, p999_ms, max_ms, throughput_eps, p50_ms_h1, p99_ms_h1, p50_ms_h2, p99_ms_h2, stable, value_errors`

`value_errors` is defined in section 9. It is 0 for patterns without a value check.

The `_h1` and `_h2` fields are the percentiles for the first and second half of the measured window, split by `in_ts`. `stable` is the split-half check described in section 8.

Latency percentiles and throughput use only records whose input arrived after warm-up. Correctness uses the whole run, so warm-up boundary effects cannot show up as missing records.

For patterns with one output record per input, correctness works as follows. 01-dedup is the reference case. See its [Benchmark setup](../patterns/01-dedup/README.md#benchmark-setup).
- `expected_distinct` is the number of distinct output keys the generator's input implies for the run.
- Output records count for correctness when their `in_ts` falls between the run start and the run end plus the maximum duplicate delay.
- `missing = expected_distinct - distinct_out_keys`
- `sink_duplicates = out_records - distinct_out_keys`

## 7. Resource tags

Tag or name every AWS and Databricks resource with `project=rtm-pattern-pack`, `owner=<email>` and `pattern=<id or shared>`.

## 8. Tuned profiles (applied to every published pattern run)

Each published pattern gets one 30-minute run at the section 5 defaults, with each engine on the profile below. Each engine follows its documented best practices, fixed once for all patterns. The checkpoint intervals differ because each is that service's documented default. The levers and their sources are in [TUNING.md](TUNING.md).

| | Spark RTM | Managed Flink |
|---|---|---|
| Runtime | DBR 18 LTS (`18.x-scala2.13`), Photon off | FLINK-2_3 |
| Checkpointing | `realTime="5 minutes"` (documented default) | 60 s (Managed Flink default) |
| Parallelism | Source tasks = topic partitions. Shuffle partitions chosen so total tasks use 50 to 75 percent of slots | Parallelism = partitions, ParallelismPerKPU = 1 |
| Kafka producer | `linger.ms = 0` | `linger.ms = 0` |
| Compute | Smallest cluster whose slot use stays at or below 75 percent | Parallelism / ParallelismPerKPU + 1 orchestration KPU |
| Excluded | Photon (not supported), private settings | Buffer-timeout support case, mini-batch |

The stability check works as follows.
- The collector reports p50 and p99 for the first and second half of the measured window.
- A single run is accepted if the two halves differ by at most 15 percent.
- The run is repeated once if the halves differ by more.
- If the two engines are within 15 percent of each other on p99, the result is reported as comparable at p99, and no faster engine is named. This does not trigger a repeat.

## 9. Conventions shared by the patterns

These rules apply to every pattern. A pattern's Benchmark setup can change them.

- Topic bases. Engines receive `--input-topic` and `--output-topic` (RTM) or `app.input.topic` and `app.output.topic` (Flink), as before. A pattern with several input or output topics adds a suffix to the base, in the form `<base>.<suffix>`. For example, 04-routing writes `rtmpack.04-routing.out.rtm.payment`.
- Input events. Unless a pattern's Benchmark setup says otherwise, the input is the section 2 event with `dup_ratio = 0`, plus the pattern's own fields. Records are padded to 1024 bytes.
- Output values always carry `in_ts` and `engine` (section 3). For an output built from several inputs, `in_ts` is the largest input `in_ts` that contributed to the record. Latency is therefore measured from the newest input the output reflects.
- Value check. Some patterns have a deterministic expected value for each output. The collector recomputes it and counts mismatches as `value_errors`. This is a new results field, and it is 0 for patterns without a check.
- Aggregate patterns (06-tumbling, 07-sliding) produce updates, not one record per input, so their fields mean the following:
  - `out_records` is the number of updates read. `distinct_out_keys` is the number of distinct (key, window) outputs. `expected_distinct` is the number of (key, window) pairs the generator produced. `missing` is their difference.
  - `sink_duplicates` is not meaningful and is reported as null.
  - `value_errors` counts (key, window) pairs whose final update (the one with the highest `count`) does not equal the expected `count` and `sum_amount`.
- Sizing. RTM uses m5.xlarge workers (4 slots each) and follows section 8. Flink uses parallelism 8 and 1 parallelism per KPU, unless the pattern's Benchmark setup says otherwise.

## 10. Per-pattern rules

Each pattern's input, output, keys, correctness rule, expected side files, parameters and state settings are in the Benchmark setup section of its README:

- [01-dedup](../patterns/01-dedup/README.md#benchmark-setup)
- [02-unnest](../patterns/02-unnest/README.md#benchmark-setup)
- [03-udf](../patterns/03-udf/README.md#benchmark-setup)
- [04-routing](../patterns/04-routing/README.md#benchmark-setup)
- [05-lookup](../patterns/05-lookup/README.md#benchmark-setup)
- [06-tumbling](../patterns/06-tumbling/README.md#benchmark-setup)
- [07-sliding](../patterns/07-sliding/README.md#benchmark-setup)
- [08-join](../patterns/08-join/README.md#benchmark-setup)
