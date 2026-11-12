# NN Name: Spark Real-Time Mode

<!-- Template. Replace every <...>. -->

This directory holds the Spark Real-Time Mode (RTM) side of the <name> pattern. The job reads the input topic from Amazon MSK, <does what>, and writes to the RTM output topic. The pattern is described in [../README.md](../README.md). The Flink side is in [../flink/README.md](../flink/README.md).

| Path | Purpose |
|---|---|
| `<name>_rtm.py` | PySpark job. Keep the transformation a pure function so it can be tested in batch mode. |
| `tests/test_<name>_rtm.py` | Local pytest suite. Runs the transformation in batch mode and as a finite streaming query. |

## How the job works

1. The job reads `rtmpack.<id>.input` with `startingOffsets=latest`, over `SASL_SSL` with `SCRAM-SHA-512`. Credentials and the bootstrap address come from the secret scope `rtmpack`.
2. <Parse step>
3. <Transformation step, with the key code>
4. <Sink step: output key, value, topic>

## Arguments

| Argument | Default | Meaning |
|---|---|---|
| `--input-topic` | `rtmpack.<id>.input` | Input topic |
| `--output-topic` | `rtmpack.<id>.out.rtm` | Output topic |
| `--checkpoint-location` | <path> | Checkpoint path |
| `--<argument>` | <default> | <meaning> |

## Cluster configuration

<!-- Runtime, worker type and count, slots against tasks, Photon off, any Spark configs the pattern needs. -->

## Mapping to the Flink job

| Concern | Flink | RTM (this job) |
|---|---|---|
| <operator> | <Flink construct> | <RTM construct> |
| State lifetime | <> | <> |
| Delivery | <> | at-least-once |

## Running the local tests

```sh
infra/scripts/test-pattern.sh <id>
```
