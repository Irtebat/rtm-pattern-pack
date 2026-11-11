# 04 Routing

## About the pattern

This pattern covers two routing jobs that run in one query. Demux splits one topic by event type. Fan-out sends every event to a second consumer.

The input is one JSON event per Kafka record on `rtmpack.04-routing.input`. Each event is the common event (`event_id`, `key`, `seq`, `event_ts`, `is_dup`, `payload`) plus a `type` field. The type is one of four values: `payment` (40 percent), `order` (30 percent), `click` (20 percent) or `login` (10 percent). There are no duplicates.

Every event is written twice, with the Kafka key `event_id`. The output base is `rtmpack.04-routing.out.<engine>`, where `<engine>` is `rtm` or `flink`:

| Topic | Content |
|---|---|
| `<base>.payment`, `<base>.order`, `<base>.click`, `<base>.login` | Events of that type (demux) |
| `<base>.all` | Every event (fan-out) |

The value carries the input fields without `payload`, plus `in_ts` and `engine`. Both copies of an event carry the same `in_ts`.

For example, an input event with `type = "payment"` produces one record on `rtmpack.04-routing.out.rtm.payment` and one on `rtmpack.04-routing.out.rtm.all`. The output rate is therefore twice the input rate.

The query is stateless, with no shuffle and no state.

## Why sub-second processing matters for this pattern

Demux feeds the service that owns each event type: payments, orders, clicks and so on, each subscribed to its own topic. Sub-second routing matters when that service responds to a user or a transaction. A payment service that must confirm while the customer waits is one example. The delay added by routing counts against that response deadline.

Fan-out feeds several operational systems at once, such as an application topic and a serving store. Each system can act on the same event without waiting for the others. If the routed events only feed analytics, near-real-time streaming into Delta is the better fit. The Delta copy can be written separately from the same topic.

## How each engine implements it

| Engine | Key construct | Details |
|---|---|---|
| Spark Real-Time Mode (RTM) | One query. `explode(array(type_topic, all_topic))` turns each event into two rows with a `topic` column, and the Kafka sink is started without a `topic` option so it writes each row to the topic in that column | [rtm/README.md](rtm/README.md) |
| Managed Service for Apache Flink | One statement set with five `INSERT` statements (four with `WHERE type = '<type>'`, one without a filter) over a single source table | [flink/README.md](flink/README.md) |

- On RTM, the per-row `topic` column is a feature of the open-source Spark Kafka sink, and the job relies on it in real-time mode. All five output topics must exist before the job starts. An event with a type outside the four known values is written to the `.all` topic only.
- The RTM variant `--mode all-only` writes each event once, to the `.all` topic, through the same `topic` column. It separates the cost of the per-row topic column from the cost of writing to five topics.
- On Flink, the planner reads the source once and feeds five chained calc and sink pipelines. There is no `Exchange`, so there is no network shuffle. Each task holds five Kafka producers. All five output topics must exist before the job starts.

## Benchmark setup

Shared rules (topics, event schema, latency, run parameters, tuned profiles) are in sections 1 to 8 of [docs/CONTRACT.md](../../docs/CONTRACT.md). The rules below apply to this pattern.

| Item | Value |
|---|---|
| Input topic | `rtmpack.04-routing.input`, section 2 event plus `type`, Kafka key `key` |
| Event types | `payment` 40 percent, `order` 30 percent, `click` 20 percent, `login` 10 percent. The seeded generator draws the type |
| Input rate | 10,000 events per second (20,000 output records per second) |
| Record size | 1,024 bytes |
| Partitions | 8 (input and all five output topics) |
| Duplicates | none |
| Output | Kafka key `event_id`. The value has the input fields without `payload`, plus `in_ts` and `engine` (section 3). Both copies of an event carry the same `in_ts` |
| Duration | 1,800 seconds, with the first 300 seconds excluded from latency statistics |
| Correctness rule | `expected_distinct` is 2 x the unique events, counted on (topic, `event_id`), as in section 6. Value errors count records in a type topic whose `type` does not match the topic |
| Latency | output record broker append time minus input record broker append time |
| Delivery | at-least-once on both sinks, producer `linger.ms = 0` |

Each engine is sized as follows.

- RTM runs on DBR 18 LTS (`18.x-scala2.13`), Photon off, on 1 driver and 3 workers of m5.xlarge (12 slots). The query is one stage, so it runs 8 tasks, which is 67 percent of the slots. The trigger is `realTime="5 minutes"`.
- Managed Flink runs `FLINK-2_3` with parallelism 8 and `ParallelismPerKPU = 1`, plus 1 orchestration KPU. It checkpoints every 60 seconds, and autoscaling is off.

## How to run

Local tests cover the RTM job, the Flink build and tests, and the harness. Nothing in the cloud is touched.

```bash
infra/scripts/test-pattern.sh 04-routing
```

The full benchmark run starts both engines on the same input topic:

```bash
infra/scripts/run-benchmark.sh --pattern 04-routing
```

The RTM job has one variant flag, passed with `rtm_extra_params`. The variant gets its own checkpoint location, because a checkpoint cannot be shared between different queries. A repeated argument takes its last value, so the extra `--checkpoint-location` replaces the default one.

| Flag | Values | What it changes |
|---|---|---|
| `--mode` | `demux-fanout` (default), `all-only` | `all-only` writes each event once to the `.all` topic instead of twice, so the output rate equals the input rate. |

```bash
infra/scripts/run-benchmark.sh --pattern 04-routing \
  --tf-var 'rtm_extra_params={"04-routing"=["--mode","all-only","--checkpoint-location","/Volumes/<catalog>/<schema>/<volume>/checkpoints/04-routing-rtm-all-only-run1"]}'
```

The `all-only` variant is a diagnostic for the RTM job. The Flink side always runs the full pattern. The override variables (`rtm_extra_params`, `rtm_spark_version_overrides`, `rtm_spark_conf_overrides`) are described in [../../infra/README.md](../../infra/README.md).

