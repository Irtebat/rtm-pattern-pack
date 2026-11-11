# RTM Pattern Pack

This benchmark compares Spark Real-Time Mode (RTM) on Databricks with Amazon Managed Service for Apache Flink. It tests one stream-processing pattern at a time, from Kafka (Amazon MSK) to Kafka.

For each pattern, the pack has a reference implementation for each engine. It also has a contract that defines the input, the output and the latency measurement. The infrastructure runs both engines side by side. A harness generates load and computes latency and correctness. This repository describes how to run and compare the engines. It contains no results.

## The comparison setup

```
   load generator host                 Amazon MSK
   (EC2, runs the harness)    +------------------------------------+
   writes the input stream -> | rtmpack.<id>.input                 |
                              | rtmpack.<id>.out.rtm               |
                              | rtmpack.<id>.out.flink             |
                              +--------+-----------------+---------+
                                       |                 |
                         reads input   |                 |   reads input
                         writes out.rtm|                 |   writes out.flink
                                       v                 v
                     +----------------------+   +-------------------------+
                     | RTM job              |   | Managed Flink           |
                     | classic Databricks   |   | application             |
                     | cluster (PySpark)    |   | (Flink SQL)             |
                     +----------------------+   +-------------------------+

   the collector (on the load generator host) reads both output topics
   and writes results/<run_id>.jsonl
```

Both engines read the same input topic, each in its own consumer group, and write to their own output topic. Latency is the output record's broker timestamp minus the input record's broker timestamp. Host clocks do not enter the measurement. Everything runs in one VPC, on the Databricks workspace's data plane network.

## Patterns

Each folder under `patterns/` holds the RTM job (`rtm/`), the Flink application (`flink/`) and a write-up. The pattern id is the folder name. It is used in topic names, Terraform keys, harness flags and run ids.

| Pattern | What it does |
|---|---|
| [01-dedup](patterns/01-dedup/README.md) | Deduplication: keep the first record per key, drop resends within a 60 second horizon |
| [02-unnest](patterns/02-unnest/README.md) | Array unnest: one output record per element of an array in the input record |
| [03-udf](patterns/03-udf/README.md) | Scalar UDF scoring: a SHA-256 based score and decision computed by a user-defined function |
| [04-routing](patterns/04-routing/README.md) | Demux and fan-out: write each event to a topic per event type and to an all-events topic |
| [05-lookup](patterns/05-lookup/README.md) | Lookup join: enrich each event from a small in-memory reference table |
| [06-tumbling](patterns/06-tumbling/README.md) | Tumbling window aggregation: running count and sum per key over 60 second windows |
| [07-sliding](patterns/07-sliding/README.md) | Sliding window aggregation: the same aggregates over 60 second windows that slide every 20 seconds |
| [08-join](patterns/08-join/README.md) | Inner stream-stream join: match orders with their payments within 30 seconds |

The shared rules are in [docs/CONTRACT.md](docs/CONTRACT.md). The exact input, output and correctness rules for each pattern are in its README, under Benchmark setup.

## Repository layout

```
rtm-pattern-pack/
  docs/
    CONTRACT.md                benchmark contract shared by all components
    HOW_A_RUN_WORKS.md         the moving parts, the run steps, how each number is computed
    TUNING.md                  fair comparison rules, tuned profiles, tuning levers
  harness/                     Python CLI: topic setup, load generator, collector, report
  patterns/
    01-dedup/ ... 08-join/     one folder per pattern:
      rtm/                       PySpark RTM job and tests
      flink/                     Flink SQL application (Maven) and tests
      README.md                  pattern description
  infra/
    terraform/
      modules/                 network, msk, artifacts, flink_app, loadgen_host,
                               databricks_shared, databricks_rtm_job
      envs/sandbox-usw2/       root module; patterns.tf lists the patterns and their sizing
    scripts/                   jar build, benchmark and session runs, harness update, local tests
```

Terraform creates all AWS and Databricks resources. Shell scripts cover only the steps that are not resource management: building the Flink jar, starting a benchmark run, and collecting results. Every resource is tagged `project=rtm-pattern-pack`. The Terraform variable `running_patterns` starts and stops the engines, and `loadgen_running` starts and stops the load generator host. The shared infrastructure stays in place between patterns.

## Quick start

You need Terraform 1.6 or later, AWS CLI version 2 with the Session Manager plugin, the Databricks CLI, JDK 17, Maven and Python 3.11. You also need an AWS login and a Databricks CLI profile for a classic workspace. The workspace's data plane VPC must be in the same AWS account and region as the benchmark resources.

To deploy, copy and fill in the example variables file, build the Flink jar, then plan and apply:

```bash
cd infra/terraform/envs/sandbox-usw2
cp terraform.tfvars.example terraform.tfvars    # fill in the placeholders
cd ../../../..
infra/scripts/build-flink.sh 01-dedup           # builds the Flink jar
cd infra/terraform/envs/sandbox-usw2
terraform init
terraform plan -out=deploy.tfplan
terraform apply deploy.tfplan
```

The engines are created stopped. MSK is billed while it exists.

To run one pattern:

```bash
infra/scripts/run-session.sh --patterns 01-dedup
```

This starts the load generator host and runs a 30-minute measured run for the pattern with both engines. It writes `results/<run_id>.jsonl`, then stops the engines and the host. For a short smoke run, use `infra/scripts/run-benchmark.sh --pattern 01-dedup --rate 2000 --duration 180 --warmup 30`.

To test one pattern locally:

```bash
pip install -r patterns/requirements-test.txt
infra/scripts/test-pattern.sh 01-dedup
```

This runs the RTM job tests, builds the Flink jar with its tests, and runs the harness tests. No cloud resources are touched.

More detail is in [infra/README.md](infra/README.md) and [harness/README.md](harness/README.md).

## Fairness rules

The full protocol is in [docs/TUNING.md](docs/TUNING.md).

1. Both engines get the same input. One seeded, deterministic generator writes one input topic, and both engines read it.
2. Both engines use one clock. Latency uses broker timestamps (LogAppendTime) on both topics.
3. Each engine follows its own documented best practice. Both engines run a tuned profile that is fixed once for all patterns. The profile uses only documented, self-service settings. Settings that need a support case or a private preview are excluded.
4. Both engines run the same run shape. The rate is 10,000 events per second unless a pattern says otherwise. Records are 1 KB, with 8 partitions. Each run lasts 30 minutes, and the first 5 minutes are excluded from latency statistics. Both engines use at-least-once sinks.
5. Each engine runs at the smallest configuration that sustains the rate within its profile. vCPU and hourly list-price cost are recorded for both engines.
6. Each pattern gets one run, with a stability check. The collector reports p50 and p99 for each half of the measured window. The run is repeated once if the halves differ by more than 15 percent. If the engines are within 15 percent of each other on p99, they are reported as comparable.
7. A result counts only if the output is correct for the pattern over the whole run. The collector records missing records, sink duplicates and value errors.

## Documentation

| Document | Contents |
|---|---|
| [docs/CONTRACT.md](docs/CONTRACT.md) | Topics, schemas, latency definition, run parameters, tuned profiles, per-pattern rules |
| [docs/HOW_A_RUN_WORKS.md](docs/HOW_A_RUN_WORKS.md) | The run step by step, how each number is computed, code reading order |
| [docs/TUNING.md](docs/TUNING.md) | What fair means, tuned profiles, per-pattern protocol, tuning levers and sources |
| [infra/README.md](infra/README.md) | Terraform modules, deploy, sessions, variant runs, adding a pattern, teardown |
| [harness/README.md](harness/README.md) | The `rtmpack` CLI, metrics, adding a pattern to the harness |
| [infra/terraform/modules/loadgen_host/README.md](infra/terraform/modules/loadgen_host/README.md) | The load generator host module |
