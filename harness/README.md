# rtmpack harness

Benchmark harness for the RTM Pattern Pack. It drives the input topic on Amazon MSK at a fixed rate. It collects the output of each engine (Spark Real-Time Mode and Managed Flink), and computes latency and correctness under the rules in [docs/CONTRACT.md](../docs/CONTRACT.md).

The harness does not run the engines. Each engine job reads `rtmpack.<id>.input` and writes `rtmpack.<id>.out.<engine>`, where `<id>` is the pattern id (the folder name under `patterns/`, for example `01-dedup`). The contract defines the topics, event schema, latency definition, run parameters and results record. This package implements them.

## Contents

| Path | Purpose |
|---|---|
| `rtmpack_harness/cli.py` | Command-line entry point (`rtmpack`) |
| `rtmpack_harness/contract.py` | Constants and run-parameter rules from the contract |
| `rtmpack_harness/config.py` | Kafka connection settings (flags, environment, Secrets Manager) |
| `rtmpack_harness/secrets.py` | Reads the SCRAM credentials from AWS Secrets Manager |
| `rtmpack_harness/topics.py` | Creates and deletes the topics for a pattern |
| `rtmpack_harness/patterns/` | One file per pattern (`p01_dedup.py` ... `p08_join.py`): generator, encoder, output keys, expected count, value check. `_template.py` is the starting point for a new one, and `base.py` holds the `Pattern` record |
| `rtmpack_harness/events.py` | The shared input event, the generator base classes and padded JSON encoding |
| `rtmpack_harness/schedule.py` | Duplicate decisions and the resend heap |
| `rtmpack_harness/loadgen.py` | Multi-process, rate-controlled producer and run manifest |
| `rtmpack_harness/collect.py` | Output reader that writes one results line per engine |
| `rtmpack_harness/ingest.py` | Turns output messages into accumulator calls: multi-topic keys, value checks, aggregate updates |
| `rtmpack_harness/stats.py` | Latency histogram, percentiles, distinct counting, correctness, expected aggregates |
| `rtmpack_harness/results.py` | Manifest, results and expected-aggregate side file I/O |
| `rtmpack_harness/report.py` | Markdown table over `results/*.jsonl` |
| `tools/latency_timeline.py` | Prints the seconds with the highest maximum latency for one engine of a finished run |
| `tests/` | Unit tests; none need a broker |

## Patterns supported

The pattern id given to `loadgen` (and stored in the manifest, which `collect` reads) selects the rules in `rtmpack_harness/patterns/`.

| Pattern | Input | Output key | `expected_distinct` comes from |
|---|---|---|---|
| `01-dedup` | Events with `event_id`, duplicates resent (`dup_ratio` default 0.2) | `event_id` | `counts.unique_sent` |
| `02-unnest` | Orders with 1 to 5 items, no duplicates (`dup_ratio` 0) | `<order_id>:<item_idx>` | `counts.expected_outputs` |

For 02-unnest (see its [Benchmark setup](../patterns/02-unnest/README.md#benchmark-setup)), `rate_eps` counts orders. The output rate is therefore about three times higher: 30,000 records per second at 10,000 orders per second. `counts.unique_sent` is the number of orders. `counts.expected_outputs` is the sum of item counts over all orders sent. Latency and throughput are computed per output record, against the `in_ts` of the order. `record_bytes` applies to the whole order JSON and must be at least 400 for 02-unnest. `--dup-ratio` defaults to 0 for 02-unnest, and any other value is rejected. Its keys are `cust-NNNNN`. Its generator is seeded with `rtmpack:<id>:<seed>:<worker_id>`, so the same seed gives the same orders for each worker.

### Patterns 03-udf to 08-join

These follow the shared conventions in contract section 9 and the Benchmark setup of each pattern. None of them has duplicates. `dup_ratio` is 0, and any other value is rejected. All keys are `user-NNNNN`. Every record is padded to `record_bytes`, which has a minimum of 256. Each generator is seeded with `rtmpack:<id>:<seed>:<worker_id>`.

| Pattern | Fields added to the shared input event | Output topics | `expected_distinct` |
|---|---|---|---|
| `03-udf` | none | `rtmpack.03-udf.out.<engine>` | `counts.unique_sent` |
| `04-routing` | `type`: payment 40 percent, order 30, click 20, login 10 | `rtmpack.04-routing.out.<engine>.<type>` for the four types and `.all` | `counts.expected_outputs` (2 per event) |
| `05-lookup` | `merchant_id`, uniform over 10,000 merchants | `rtmpack.05-lookup.out.<engine>` | `counts.unique_sent` |
| `06-tumbling` | `amount`, integer uniform 100 to 50,000 | `rtmpack.06-tumbling.out.<engine>` | pairs in the expected side file |
| `07-sliding` | `amount`, as 06-tumbling | `rtmpack.07-sliding.out.<engine>` | pairs in the expected side file |
| `08-join` | two topics, see below | `rtmpack.08-join.out.<engine>` | `counts.expected_matches` |

`rtmpack topics create` creates every input and output topic of a pattern. That is the three base topics, the two 08-join input topics (`rtmpack.08-join.input.orders`, `rtmpack.08-join.input.payments`) and the five 04-routing topics per engine.

For 08-join, `rate_eps` is the order rate. The pattern defines its own default of 5,000 orders per second, so `rtmpack loadgen --pattern 08-join` needs no `--rate-eps`. An explicit flag overrides the default. The value used is recorded in the manifest as `params.rate_eps`, together with `rate_unit: "orders"`. Payments are extra. 90 percent of orders get one, sent after a delay uniform over 0 to 5,000 ms. That delay comes from the same delayed-resend heap that 01-dedup uses for duplicates. A payment's `event_ts` is its own send time, and its Kafka key is the order's key. The payment id is a UUID derived from the order id (SHA-256), so the collector can check it without any stored mapping. The manifest counts include `orders_sent`, `payments_sent` and `expected_matches` (equal to `payments_sent`).

The collector counts `value_errors` over the same records as `out_records`. The table shows when a record is an error:

| Pattern | A record is an error when |
|---|---|
| `03-udf` | `score` or `decision` differs from the SHA-256 rule in the 03-udf Benchmark setup for its `event_id` and `key` |
| `04-routing` | a record in a type topic has a `type` that differs from the topic suffix. Records in `.all` are not checked |
| `05-lookup` | `risk_tier`, `category` or `country` differs from the merchant table, or the merchant is unknown |
| `08-join` | `payment_id` is not the one derived from the record's `order_id` |
| `06-tumbling`, `07-sliding` | see the aggregate rules below |
| `01-dedup`, `02-unnest` | never; the field is 0 |

For 04-routing, the collector reads every output topic of the engine. Distinct keys are counted on (topic, key), so one event counts twice: once in its type topic and once in `.all`. `sink_duplicates` is `out_records - distinct_out_keys`.

For 05-lookup, `rtmpack refdata --pattern 05-lookup --out patterns/05-lookup/data/merchants.csv` writes the 10,000 row merchant table (header `merchant_id,risk_tier,category,country`) from the rule in the 05-lookup Benchmark setup. The file is deterministic and checked in.

For 06-tumbling and 07-sliding, the load generator adds every event it sends to an expected table of (key, `window_start_ms`) -> (`count`, `sum_amount`). 06-tumbling uses 60 s tumbling windows aligned to the epoch. 07-sliding uses 60 s windows sliding every 20 s, so each event lands in three windows. The table is written when the worker finishes.

Each worker writes one side file, `results/<run_id>.expected.w<NNN>.npz`. It is a compressed numpy archive with three arrays of equal length, sorted by digest:

| Array | Type | Meaning |
|---|---|---|
| `digest` | uint64 | First 8 bytes (little endian) of BLAKE2b of the UTF-8 string `<key>:<window_start_ms>` |
| `count` | int64 | Events in the pair |
| `sum_amount` | int64 | Sum of their amounts |

Workers own disjoint key ranges, so their pairs never overlap. `collect` loads and merges every `*.expected.w*.npz` of the run. Memory is 24 bytes per pair. The manifest records `counts.expected_pairs`.

For these two patterns the collector reads updates. It keeps the final update per pair (the highest `count`, then the highest `sum_amount`) and reports:

| Field | Meaning |
|---|---|
| `out_records` | Updates read in the correctness window |
| `distinct_out_keys` | Distinct (key, window) pairs seen |
| `expected_distinct` | Pairs in the expected side file |
| `missing` | Expected pairs never seen |
| `sink_duplicates` | `null` (not meaningful for updates) |
| `value_errors` | Pairs whose final update differs from the expected `count` or `sum_amount`, plus seen pairs that are not in the expected table at all |

The key and window come from the `key` and `window_start_ms` fields of the value. Latency is measured per update against its `in_ts`, as for every other pattern. Probes must run as separate run ids at least 90 seconds before a measured run, so that their windows do not mix with its pairs.

## Requirements

- Python 3.11 or later
- `confluent-kafka`, `numpy` and `boto3` (installed with the package)
- Network access to the MSK brokers (port 9096 private, 9196 public)
- AWS credentials with `secretsmanager:GetSecretValue` on the MSK secret, if credentials are read from Secrets Manager

## Install

```bash
cd harness
python3.11 -m venv .venv && . .venv/bin/activate
pip install -e ".[test]"
rtmpack --help
```

On the load generator host the package is installed at first boot from the harness bundle. See [the loadgen_host module](../infra/terraform/modules/loadgen_host/README.md).

## Connection settings

Each Kafka option is a flag with an environment fallback. Flags take precedence.

| Setting | Flag | Environment variable |
|---|---|---|
| Bootstrap servers (`host:port,...`) | `--bootstrap` | `RTMPACK_BOOTSTRAP` |
| Security protocol (`SASL_SSL` default) | `--security-protocol` | `RTMPACK_SECURITY_PROTOCOL` |
| SCRAM-SHA-512 username | `--sasl-username` | `RTMPACK_SASL_USERNAME` |
| SCRAM-SHA-512 password | `--sasl-password-file` (path) | `RTMPACK_SASL_PASSWORD` |
| CA bundle for TLS | `--ca-location` | `RTMPACK_CA_LOCATION` |
| Credential source | `--credentials auto\|env\|secrets-manager` | none |
| Secret name or ARN | `--secret-id` | `RTMPACK_SECRET_ID` or `RTMPACK_MSK_SECRET_ARN` |
| AWS region | `--region` | `RTMPACK_AWS_REGION` or `AWS_REGION` |

The password is never accepted as a command-line value.

Credentials resolve as follows. With `--credentials auto` (the default), the harness uses a username and password from flags or the environment, if both are present. Otherwise, it reads the secret `AmazonMSK_rtmpack` (JSON with `username` and `password`) through boto3, using the standard AWS profile and region settings.

```bash
export RTMPACK_BOOTSTRAP="<broker-1>:9096,<broker-2>:9096"
export AWS_PROFILE=<your-aws-profile>
export AWS_REGION=us-west-2
```

## Commands

### topics

Creates or deletes every topic of a pattern. That is the input topic (two for 08-join) and the output topic of each engine (five per engine for 04-routing). Every topic has `message.timestamp.type=LogAppendTime`, `retention.ms=43200000` (12 hours) and replication factor 3. The partition count applies to all of them.

```bash
rtmpack topics create --pattern 01-dedup --partitions 8
rtmpack topics delete --pattern 01-dedup --yes
```

### loadgen

Writes the input topic for one run and writes `results/<run_id>.manifest.json`. Parameters default to the contract values (`rate_eps` 10000, `record_bytes` 1024, `key_cardinality` 100000, `duration_s` 1800, `warmup_s` 300, `dup_ratio` 0.2, delay 0 to 30000 ms, `seed` 42).

```bash
rtmpack loadgen --pattern 01-dedup --rate-eps 100000 --partitions 8 --duration-s 1800 --warmup-s 300 \
    --run-id 01-dedup-100k-example --results-dir results
```

`--rate-eps` defaults to 10000, or to the pattern's own default (08-join: 5000 orders per second). Run it with `--help` for the full list. `--workers` sets the number of producer processes. By default it is `ceil(rate / 25000)`, capped at the CPU count. The loader warns when the worker count exceeds the CPU count.

### refdata

Writes a pattern's reference data file. Only `05-lookup` has one.

```bash
rtmpack refdata --pattern 05-lookup --out patterns/05-lookup/data/merchants.csv
```

### collect

Reads `rtmpack.<id>.out.<engine>` (all its suffixed topics for 04-routing) for the window of the run. It appends one line to `results/<run_id>.jsonl`. Run it once per engine after the loader has finished.

The version, compute and cost values below are placeholders. Record the real values for each run. The benchmark scripts fill them in from Terraform outputs.

```bash
rtmpack collect --run-id 01-dedup-100k-example --engine rtm \
    --engine-version <runtime> --compute-desc "<cluster description>" \
    --vcpu <vcpu> --cost-per-hour-usd <usd>

rtmpack collect --run-id 01-dedup-100k-example --engine flink \
    --engine-version <runtime> --compute-desc "<application description>" \
    --vcpu <vcpu> --cost-per-hour-usd <usd>
```

The command reads from the first record at or after the manifest start time (`offsets_for_times`) to the end time plus a grace period. The grace period defaults to the maximum duplicate delay plus 60 seconds. It can be set with `--grace-s`. Diagnostic counts (records outside the window, malformed records, negative latencies) go to stderr. They are not part of the results record.

### report

```bash
rtmpack report --results-dir results --out results/summary.md
rtmpack report --pattern 01-dedup
```

Produces a markdown table with one row per (run, engine). The columns `p50 h1`, `p99 h1`, `p50 h2`, `p99 h2` and `Stable` show the split-half stability check described under Metrics. The last column is derived. It is the cost per hour divided by the output rate, scaled to one million output records.

## Run procedure

The scripts in `infra/scripts/` do all of this for you (see [infra/README.md](../infra/README.md)). By hand:

1. `rtmpack topics create --pattern <id> --partitions N`, once per pattern and partition count.
2. Start the RTM and Flink jobs. Both must read the input topic and write their output topic.
3. `rtmpack loadgen ...` for the run. Do not start another run on the same input topic until the grace period of the first run has passed. Otherwise its resends fall into the next run's windows.
4. `rtmpack collect ...` once per engine. Each call appends or replaces that engine's line.
5. `rtmpack report`.

## Metrics

Definitions follow contract sections 4 and 6. Each output record counts in one or both windows, chosen by its input timestamp `in_ts`. The latency window (`warmup cutoff` to `run end`) is used for percentiles and throughput. The correctness window (`run start` to `run end` plus the maximum duplicate delay) is used for the record and key counts. Correctness covers the whole run, so the warm-up boundary cannot show up as missing records.

| Field | Definition |
|---|---|
| `out_records` | Output records in the correctness window |
| `distinct_out_keys` | Distinct Kafka record keys among counted records. For 01-dedup the key is `event_id` |
| `expected_distinct` | Output records the whole run should produce, from the manifest counts entry named by the pattern (for 01-dedup, unique `event_id`s sent) |
| `missing` | `expected_distinct - distinct_out_keys` |
| `sink_duplicates` | `out_records - distinct_out_keys`. `null` for 06-tumbling and 07-sliding |
| `value_errors` | Records (or, for 06-tumbling and 07-sliding, pairs) failing the pattern's value check. 0 for 01-dedup and 02-unnest |
| `p50_ms` ... `p999_ms`, `max_ms` | Nearest-rank percentiles of `out_ts - in_ts` in whole milliseconds |
| `throughput_eps` | Output records in the measured window divided by its length (`run end - warm-up cutoff`) |
| `p50_ms_h1`, `p99_ms_h1`, `p50_ms_h2`, `p99_ms_h2` | The same percentiles over the first and second half of the measured window, split by `in_ts` at the midpoint. `null` for an empty half |
| `stable` | `true` if p50 and p99 each differ by at most 15 percent between the halves, relative to the larger value (contract section 8). `false` if either half is empty |

Percentiles come from an integer-millisecond histogram. They are exact at 1 ms resolution and use bounded memory. Distinct keys are counted exactly from 128-bit BLAKE2b digests of the record key. The collector uses about 16 bytes per counted output record. Digests are held in 256 buckets by their first byte, and counted one bucket at a time. Counting therefore needs little memory beyond the stored digests. The consume loop uses `consume(5000)` batches.

## Adding a pattern

Each pattern is one file in `rtmpack_harness/patterns/`. The harness finds it automatically; no other harness file needs editing.

**Input events.** There are two kinds:
- **The shared event plus a few fields.** Most patterns (01 and 03 to 07) use the event every pattern shares (contract section 2: `event_id`, `key`, `seq`, `event_ts`, `is_dup`) and add their own fields, such as `amount`. Start from `_template.py`.
- **Their own event shape.** A pattern whose input is not a single event, such as an order with items (02-unnest) or orders and payments on two topics (08-join), writes its own event class. Use `p02_unnest.py` as the example.

**Steps:**
1. Copy `rtmpack_harness/patterns/_template.py` to `rtmpack_harness/patterns/pNN_<name>.py`, for example `p09_filter_rekey.py`.
2. Fill it in:
   - the generator's extra fields in `_extra()`, drawing random numbers only from `self._rng` and always in the same order
   - the encoder, which writes those fields into the JSON
   - `output_keys`: the Kafka keys of the output records one input must produce
   - `expected_count_field`
   - optionally `check_value`

   The optional `Pattern` fields (duplicates, several topics, windows, reference data) are described in `base.py`, and each existing pattern file shows one of them in use.
3. Add tests in `tests/test_patterns.py`: the same seed gives the same input, records are exactly `record_bytes`, and the expected output count is right. Add the pattern to the table above.
4. Outside the harness, add the pattern's Benchmark setup section to `patterns/<id>/README.md`, link it from the Per-pattern rules in `docs/CONTRACT.md`, and add the Terraform entry (see [infra/README.md](../infra/README.md)).

Run `python -m pytest -q` in `harness/`. The collector handles one output record per input key, aggregates keyed by `key` and `window_start_ms`, and several output topics. A pattern that needs anything else must change `ingest.py`.

## Manifest

`results/<run_id>.manifest.json` is written at start with `status: running`. It is rewritten at the end with `status: complete` and the counts. It includes the pattern, its input topics, the run parameters, start and end times in ISO-8601 and epoch milliseconds, and the warm-up cutoff. It also has the counts over the whole run (`unique_sent`, `expected_outputs`, ...; for 01-dedup `expected_outputs` equals `unique_sent`), the number of workers, per-worker statistics, and the bootstrap address and security protocol. It does not include credentials.

## Event generation

- Each worker has its own seeded generator. Given the same seed, worker id, key range and parameters, the sequence of event ids, keys, sequence numbers and duplicate decisions is identical.
- Workers own disjoint, contiguous key ranges. Keys are `user-NNNNN` with a fixed width.
- `event_ts` is the scheduled send time on the generator grid, not the wall clock at send. The record content therefore does not depend on timing.
- Each first send is resent with probability `dup_ratio`. The resend is delayed by a value drawn uniformly from `[dup_delay_ms_min, dup_delay_ms_max]`. A resend has the same `event_id`, `key`, `seq` and `event_ts`, with `is_dup` set to `true`.
- The value is compact JSON padded in the `payload` field to exactly `record_bytes` bytes, for both first sends and resends.
- `expected_distinct` counts first sends only.
- The maximum delay must be below the 60 s state horizon (see the [01-dedup Benchmark setup](../patterns/01-dedup/README.md#benchmark-setup)). The loader rejects larger values.

## Assumptions

These follow from the contract wording. Each is recorded in the manifest or the results, so that a result can be interpreted without this document.

1. `record_bytes` is the size of the whole serialized value (the JSON document), including the `payload` field.
2. `rate_eps` counts first sends per second across all workers. Duplicates are extra.
3. `dup_ratio` is the probability that a first send is resent once. It is not the share of all records that are duplicates.
4. `warmup_s` is part of `duration_s`. The run lasts `duration_s` seconds in total, and the first `warmup_s` seconds are excluded from statistics.
5. The warm-up applies to latency and throughput only, decided by the broker timestamp of the input (`in_ts`). Correctness compares the whole run's output with everything the generator sent.
6. `distinct_out_keys` counts Kafka record keys. For 01-dedup the output key is `event_id`, so this is the number of distinct `event_id`s in the output.
7. Throughput is measured over the post-warm-up window. It is not the rate of the whole run.
8. Compression, producer batching and `acks=all` are implementation choices of the loader. They are not part of the contract.

## Not implemented

- Loading `results/*.jsonl` into a Delta table. The contract marks this as optional.
- HdrHistogram. The harness uses its own exact integer-millisecond histogram, which needs no extra dependency.
- A bundled Databricks job or Flink application. Those live under `patterns/<id>/`.

## Tests

Unit tests (`pytest`) cover the generators, duplicate scheduling, padding, percentiles, correctness math, the worker's rate and duplicate behavior under a fake clock, and CLI help. They run without a broker.

```bash
cd harness
pytest
```

Use the virtual environment's interpreter if `pytest` resolves to another installation: `python -m pytest -q`. For the first live run against a new cluster, start with a short run (for example `--duration-s 120 --warmup-s 30 --rate-eps 10000`). Check the manifest counts and the collect output before any longer run.
