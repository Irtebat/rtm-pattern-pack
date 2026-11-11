# Tuning and Fair Comparison

This page defines how the two engines are configured and compared. The goal is that neither engine gets an advantage the other was denied. It covers what "fair" means, the tuned profile of each engine, and the protocol for each pattern run. It also covers the tuning levers, with their public sources. For each lever, it says whether it was used. The levers that were rejected are listed too.

The run parameters and the profile table are also in [CONTRACT.md](CONTRACT.md), section 8.

## What "fair" means here

Fair does not mean identical hardware. The two engines use compute differently.
- RTM schedules every stage at once and needs one slot for every task in every stage, plus a driver.
- Flink shares slots across chained operators. Managed Service for Apache Flink also bills one extra orchestration KPU.

Forcing equal vCPU would penalize one engine for its architecture. Fair here means equal opportunity under the same rules.

1. Both engines get the same problem.
   - The same workload, input stream and partition count.
   - The same delivery guarantee (at-least-once sinks).
   - The same latency definition.
   - The same correctness check.

   Both read the same input topic at the same time, with separate consumer groups.
2. Each engine follows its own documented best practice. Each engine runs a tuned profile that is fixed once for all patterns (next section). It is not re-tuned per pattern. A pattern page lists any override it needs, and any variation run on top of the profile.
3. Levers are sorted into classes. A lever counts only if a customer could use it on that service. The classes are these.
   - Class A is documented and self-service. These are settings in the public docs that the customer can set without contacting the vendor.
   - Class B is sizing in the service's own units: node types and counts for RTM, and parallelism and parallelism per KPU for Flink.
   - Class C is vendor-assisted. These are settings that need a support case, or features in private preview. Class C is excluded from the tuned profiles and from any comparison.
4. Tradeoffs are reported with the result. If a lever buys latency with something else, that cost is stated next to the number. For example, a longer RTM batch interval means a longer replay after a failure. A lower Flink buffer timeout means more CPU.
5. Sizing follows the fixed rule in each engine's profile (next section). It is not a search for the smallest shape. vCPU and hourly cost are recorded for both engines, so any cost difference that comes from the architecture stays visible.
6. A result counts only if the output is correct for the pattern, measured over the whole run. Missing records, sink duplicates and value errors are recorded.

## Tuned profiles

| | Spark Real-Time Mode | Managed Service for Apache Flink |
|---|---|---|
| Runtime | Databricks Runtime 18 LTS (`18.x-scala2.13`), Photon off | `FLINK-2_3` |
| Checkpointing | `trigger(realTime="5 minutes")`, the documented default | 60 s, the service default |
| Parallelism | Source tasks equal the topic partitions. Shuffle partitions are chosen so total tasks use 50 to 75 percent of slots | Parallelism equals the topic partitions, 1 parallelism per KPU |
| Kafka producer | `linger.ms = 0` | `linger.ms = 0` |
| Compute | Smallest classic cluster whose slot use stays at or below 75 percent | Parallelism divided by parallelism per KPU, plus 1 orchestration KPU |
| Excluded | Photon (not supported with RTM) | Buffer timeout change (needs a support case), mini-batch |

The checkpoint intervals differ on purpose. Each is the vendor's documented default. Forcing one engine onto the other's value would not follow best practice for either engine.

## Per-pattern protocol

Every pattern gets this:

1. Build both reference implementations and their tests.
2. Run once for 30 minutes at the rate in the contract (10,000 events per second unless the pattern says otherwise), with each engine on its tuned profile. The first 5 minutes are excluded from latency statistics.
3. Check stability. The collector reports p50 and p99 for the first and second half of the measured window. The run is accepted if the two halves differ by at most 15 percent. Repeat the run once if they differ by more.
4. Compare the engines. If they are within 15 percent of each other on p99, report them as comparable and name no faster engine. This does not trigger a repeat.
5. Report vCPU and hourly cost for both engines next to the latency numbers.

A pattern can add variation runs to answer a specific question, such as another runtime or another API for the same logic. Each variation is one more run of both engines on the same input, under the same protocol. Load curves and cost searches across compute shapes were not run.

## Tuning levers

Only levers stated in the public documentation are listed. Each lever has a class and a use.
- The class is A (documented and self-service), B (sizing) or C (vendor-assisted).
- The use is one of four things: in the tuned profile, used in a variation (named on the pattern page), not tested, or rejected.

### Spark Real-Time Mode

| Lever | Setting | What it does, and the tradeoff | Class | Use |
|---|---|---|---|---|
| Batch (checkpoint) interval | `trigger(realTime="N seconds")`; the documented default is 5 minutes | A longer interval means fewer batch boundaries. The tradeoff is a longer replay after a failure and later metrics | A | In the tuned profile (5 minutes); 60 s used in a variation |
| Slot headroom | `spark.sql.shuffle.partitions` lowered, or one more worker | The RTM performance page recommends about 50 percent slot utilization. Fewer shuffle partitions cost nothing. An extra worker adds cost (class B) | A and B | In the tuned profile (50 to 75 percent of slots) |
| Kafka producer linger | `kafka.linger.ms=0` on the sink | Removes the producer batching wait | A | In the tuned profile |
| Async progress tracking and async state checkpointing | `asyncProgressTrackingEnabled`, `spark.databricks.streaming.statefulOperator.asyncCheckpoint.enabled` (on by default for Databricks Runtime 18 LTS and later) | Takes progress and state checkpoint writes off the critical path | A | Not tested (left at the default) |
| Runtime version | A newer Databricks Runtime | Newer runtimes carry RTM fixes. Read the release notes before switching | A | Used in variations (DBR 18.2 and DBR 19) |
| VARIANT parsing | `try_parse_json` and `try_variant_get` instead of `from_json`, then `posexplode` | An alternative parse path for JSON input | A | Used in a variation (02 Array unnest) |

### Managed Flink

| Lever | Setting | What it does, and the tradeoff | Class | Use |
|---|---|---|---|---|
| Producer linger | `'properties.linger.ms' = '0'` on the sink table | Recent Kafka clients default `linger.ms` to 5 (KIP-1030). Setting 0 removes up to 5 ms of batching wait | A | In the tuned profile |
| Checkpoint interval | `CheckpointConfiguration` (CUSTOM) | Sets how often state is checkpointed. It changes replay length and duplicates under at-least-once | A | In the tuned profile (60 s, the service default) |
| Two parallelism per KPU | `ParallelismPerKPU = 2` | Fewer KPUs for the same parallelism, so lower cost. Each task gets half a vCPU | B | Not tested |
| Network buffer timeout | `execution.buffer-timeout.interval` (default 100 ms) | A network buffer flushes when the timer fires or the buffer fills. A shorter timeout cuts the wait in keyed pipelines at some CPU cost. Managed Flink does not let application code change this setting, so it needs a support case | C | Rejected (vendor-assisted) |
| Buffer debloating, unaligned checkpoints, state backend | `taskmanager.network.memory.buffer-debloat.*`, `execution.checkpointing.unaligned`, `state.backend.type` | Settings aimed at backpressure. They need a support case on the service | C | Not tested |

## Rejected levers

| Engine | Lever | Reason |
|---|---|---|
| Spark RTM | Photon | Not supported with RTM. The setup documentation requires Photon to be off |
| Spark RTM | Private or preview-only settings | Not self-service, so not available to every customer (class C) |
| Managed Flink | Mini-batch (`table.exec.mini-batch.*`) | Raises latency by design. Useful only for throughput tests |
| Managed Flink | Network buffer timeout | Needs a support case (class C) |

## Sources

Spark Real-Time Mode and Databricks:
- https://docs.databricks.com/aws/en/structured-streaming/real-time/performance
- https://docs.databricks.com/aws/en/structured-streaming/real-time/setup
- https://docs.databricks.com/aws/en/structured-streaming/real-time/reference
- https://docs.databricks.com/aws/en/release-notes/runtime/

Managed Service for Apache Flink, Apache Flink and Kafka:
- https://nightlies.apache.org/flink/flink-docs-release-2.0/docs/deployment/config/
- https://nightlies.apache.org/flink/flink-docs-release-2.0/docs/dev/table/config/
- https://nightlies.apache.org/flink/flink-docs-release-2.0/docs/connectors/table/kafka/
- https://kafka.apache.org/40/configuration/producer-configs/
- https://docs.aws.amazon.com/managed-flink/latest/java/how-scaling.html
- https://docs.aws.amazon.com/managed-flink/latest/java/reference-modifiable-settings.html
