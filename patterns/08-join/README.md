# 08 Join

## About the pattern

The job joins two streams on a shared id. Orders arrive on one topic and payments on another. For each payment, the job finds the order with the same `order_id`. It writes one output record when the payment happened between the order time and 30 seconds after it.

This is an inner join. An order without a payment, a payment without an order, and a payment outside the 30-second window all produce no output.

For example:

- The orders topic receives order `o1` for key `k`, with `amount` 2500 and `event_ts` 10.000 s.
- The payments topic receives payment `p1` for order `o1`, with method `card` and `event_ts` 12.300 s.
- The output is one record with Kafka key `o1` and the fields `order_id`, `payment_id`, `key`, `amount`, `method`, `order_ts` (10.000 s), `payment_ts` (12.300 s), `in_ts` and `engine`. `in_ts` is the larger of the two input timestamps, so latency is measured from the newer input.
- A payment for `o1` at 41.000 s is more than 30 seconds after the order, so it produces nothing.

In the benchmark, 5,000 orders per second are sent. 90 percent of them get a payment, and each payment follows its order after a delay of 0 to 5,000 ms. The input and output fields are listed in [Benchmark setup](#benchmark-setup).

## Why sub-second processing matters for this pattern

Attribution and fraud services correlate two events, for example an order and its payment, or an ad impression and a click. They then act before the next step of the customer flow. The consumer of each joined record is that service. It decides whether to release the order, flag the payment or credit the click, while the customer is still waiting. Its deadline is the time the customer flow allows before it moves on.

If the joined data is only used for reporting, reconciliation or training sets, near-real-time streaming into Delta is the better fit. The join can run as a stream-stream join into a table that analysts query with a delay of seconds.

## How each engine implements it

| Engine | Key construct | Details |
|---|---|---|
| Spark Real-Time Mode (RTM) | Inner stream-stream join with `withWatermark` of 30 seconds on both inputs and a time-range join condition | [rtm/README.md](rtm/README.md) |
| Amazon Managed Service for Apache Flink | Interval join over rowtime attributes with 5-second watermarks | [flink/README.md](flink/README.md) |

- RTM needs DBR 18 LTS (`18.x-scala2.13`), Photon off, and five Spark settings for stream-stream joins (listed below). The job sets them on the session at start. The Terraform job definition sets the same values on the cluster.
- Flink removes the rows an interval join buffers by watermark, so `table.exec.state.ttl` does not apply to this operator. State size follows the rate, the 30-second window and the watermark delay.
- Delivery is at-least-once on both sinks.

The five RTM stream-stream join settings, from the [Real-Time Mode setup page](https://docs.databricks.com/aws/en/structured-streaming/real-time/):

| Setting | Value |
|---|---|
| `spark.databricks.streaming.realTimeMode.streamStreamJoin.enabled` | `true` |
| `spark.sql.streaming.join.stateFormatVersion` | `4` |
| `spark.sql.streaming.join.stateFormatV4.enabled` | `true` |
| `spark.sql.streaming.stateStore.rocksdb.mergeOperatorVersion` | `2` |
| `spark.sql.streaming.realTimeMode.controlMessage.enabled` | `true` |

## Benchmark setup

This pattern follows the shared rules in [docs/CONTRACT.md](../../docs/CONTRACT.md): run parameters (section 5), tuned profiles (section 8), output event (section 3) and the value check (section 9).

| Item | Value |
|---|---|
| Input topics | `rtmpack.08-join.input.orders` and `rtmpack.08-join.input.payments` |
| Orders input | `order_id` (UUID), `key`, `event_ts`, `amount` (integer cents, uniform 100 to 50,000), `payload`. 5,000 orders per second |
| Payments input | `payment_id` (UUID), `order_id`, `event_ts` (the payment's own send time), `method` (one of `card`, `wallet`, `bank`), `payload`. Sent for 90 percent of orders, each after a delay drawn uniformly from 0 to 5,000 ms |
| Partitions | 8 on every topic. Both input topics use the order's `key` as the Kafka key, so an order and its payment land in the same partition number |
| Join | Inner join on `order_id` where `payment.event_ts` is between `order.event_ts` and `order.event_ts + 30 s` |
| Output topics | `rtmpack.08-join.out.rtm` and `rtmpack.08-join.out.flink` |
| Output key | `order_id` |
| Output fields | `order_id`, `payment_id`, `key`, `amount`, `method`, `order_ts`, `payment_ts`, `in_ts` (the larger of the two input `in_ts` values), `engine` |
| Record size | 1024 bytes |
| Duration and warmup | 1800 s, of which the first 300 s are excluded from statistics |
| Correctness rule | `expected_distinct` is the number of orders with a payment. Distinct outputs are counted on `order_id`. `value_errors` counts outputs whose `payment_id` does not belong to the order |
| RTM sizing | 8 source tasks per input plus 4 shuffle partitions is 20 tasks. 7 workers of m5.xlarge give 28 slots, 71 percent. Trigger `realTime="5 minutes"`, `linger.ms = 0` |
| Flink sizing | Parallelism 8, 1 parallelism per KPU: 8 KPUs plus 1 orchestration KPU. Checkpoint every 60 s, `linger.ms = 0` |

## How to run

Local tests cover the RTM job, the Flink build and tests, and the harness. Nothing in the cloud is touched.

```sh
infra/scripts/test-pattern.sh 08-join
```

The full benchmark run takes 30 minutes at the defaults. Both engines read the same input.

```sh
infra/scripts/run-benchmark.sh --pattern 08-join
```

For a short smoke run, add `--rate 2000 --duration 180 --warmup 30`. The default rate for this pattern is 5,000 orders per second.

### RTM variants

A variant changes one RTM job argument for one run. Pass it with `--tf-var` as `rtm_extra_params`, a map from pattern id to extra arguments. The extra arguments are appended after the pattern's own. A repeated argument takes the last value, so every variant should also set its own `--checkpoint-location`. A checkpoint written with another setting cannot be reused. Use `/Volumes/<catalog>/<schema>/<volume>/checkpoints/` as the path prefix.

| Argument | Default | What the variant changes |
|---|---|---|
| `--output-mode` | `update` | The sink output mode, `update` or `append`. RTM documents update mode only. Structured Streaming also supports inner stream-stream joins in append mode, so `append` is the alternative |
| `--watermark-delay` | `30 seconds` | The watermark delay on both inputs, which sets how long join state is kept |
| `--join-window` | `30 seconds` | How long after the order time a payment can still match |
| `--checkpoint-interval` | set by the Terraform batch interval (300 s in the benchmark) | The real-time trigger interval, which is also the checkpoint interval |
| `--kafka-linger-ms` | `0` in the benchmark | The Kafka producer `linger.ms` of the sink |
| `--partitions` | `4` | The number of shuffle partitions for the join. The cluster size is fixed in Terraform, so keep source tasks plus shuffle partitions within the available slots |

This example uses append output mode:

```sh
infra/scripts/run-benchmark.sh --pattern 08-join \
  --tf-var 'rtm_extra_params={"08-join"=["--output-mode","append","--checkpoint-location","/Volumes/<catalog>/<schema>/<volume>/checkpoints/08-join-rtm-append-run1"]}'
```

This example uses a shorter watermark delay:

```sh
infra/scripts/run-benchmark.sh --pattern 08-join \
  --tf-var 'rtm_extra_params={"08-join"=["--watermark-delay","10 seconds","--checkpoint-location","/Volumes/<catalog>/<schema>/<volume>/checkpoints/08-join-rtm-wm10-run1"]}'
```

