# NN Name: Flink side

<!-- Template. Replace every <...>. The Java package is com.rtmpack.<name>, and the pom artifactId is rtmpack-flink-<id>. -->

This directory holds the Apache Flink implementation of the <name> pattern. The job reads `rtmpack.<id>.input`, <does what>, and writes to `rtmpack.<id>.out.flink`. It runs as an Amazon Managed Service for Apache Flink application named `rtmpack-flink-<id>`. The pattern is described in [../README.md](../README.md), and the RTM side is in [../rtm/README.md](../rtm/README.md).

## Layout

| Path | Purpose |
|---|---|
| `pom.xml` | Maven build, artifactId `rtmpack-flink-<id>` |
| `sql/<name>.sql` | The Flink SQL statements |
| `src/main/java/com/rtmpack/<name>/<Name>Job.java` | Entry point |
| `src/main/java/com/rtmpack/<name>/JobConfig.java` | Runtime properties |
| `src/test/java/com/rtmpack/<name>/<Name>SqlTest.java` | Local tests |

## How it works

<!-- The SQL or DataStream logic, step by step. -->

## Plan shape and changelog mode

<!-- The operators in the plan and the changelog mode of the result (insert only, upsert, retract). -->

## Configuration

| Group, key | Property | Override | Required | Default |
|---|---|---|---|---|
| `app`, `input.topic` | `app.input.topic` | `--app.input.topic` | yes | `rtmpack.<id>.input` |
| `app`, `output.topic` | `app.output.topic` | `--app.output.topic` | yes | `rtmpack.<id>.out.flink` |

## Configuration choices

<!-- Why each non-default setting is set, such as state TTL, mini-batch or watermarks. -->

## Parallelism and KPUs

<!-- Parallelism, parallelism per KPU, total KPUs. -->

## Build and test

```sh
infra/scripts/build-flink.sh <id>
```

## Deploy

The `infra/terraform/modules/flink_app` module uploads the jar and creates the application.
