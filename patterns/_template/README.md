# NN Name

<!-- Template. Copy this folder to patterns/<id>/ and replace every <...>. Delete these comments when done. -->

## About the pattern

<!-- What the job does, in two or three sentences. Then a small worked example: one or two input records and the exact output records they produce. -->

For example, <input record> produces <output records>.

## Why sub-second processing matters for this pattern

<!-- Who acts on each output record, what they do with it, and what their deadline is. Say when near-real-time streaming into Delta is the better fit. -->

## How each engine implements it

| Engine | Key construct | Details |
|---|---|---|
| Spark Real-Time Mode (RTM) | <API or operator> | [rtm/README.md](rtm/README.md) |
| Amazon Managed Service for Apache Flink | <SQL construct or DataStream API> | [flink/README.md](flink/README.md) |

- RTM: <runtime, output mode, how state is bounded>.
- Flink: <how it emits, how state is bounded>.
- Delivery is at-least-once on both sinks.

## Benchmark setup

This pattern follows the shared rules in [docs/CONTRACT.md](../../docs/CONTRACT.md). <List the sections that apply.>

| Item | Value |
|---|---|
| Input topic | `rtmpack.<id>.input`: <the shared event (section 2) plus pattern fields> |
| Output topics | `rtmpack.<id>.out.rtm` and `rtmpack.<id>.out.flink` |
| Output key | <Kafka key of each output record> |
| Output fields | <fields>, `in_ts`, `engine` |
| Emission | <when an output record is written> |
| Rate | 10,000 events per second |
| Record size | 1024 bytes |
| Partitions | 8 on input and output topics |
| Key cardinality | 100,000 |
| Duration and warmup | 1800 s, of which the first 300 s are excluded from statistics |
| Correctness rule | <how the collector decides an output is correct: expected count, value check, missing and duplicate rules> |
| RTM sizing | <workers, slots, tasks, trigger interval> |
| Flink sizing | <parallelism, KPUs, checkpoint interval> |

## How to run

Local tests cover the RTM job, the Flink build and tests, and the harness. Nothing in the cloud is touched.

```sh
infra/scripts/test-pattern.sh <id>
```

The full benchmark run takes 30 minutes at the defaults. Both engines read the same input.

```sh
infra/scripts/run-benchmark.sh --pattern <id>
```

For a short smoke run, add `--rate 2000 --duration 180 --warmup 30`.

### RTM variants

| Argument | Default | What the variant changes |
|---|---|---|
| `--<argument>` | <default> | <effect> |
