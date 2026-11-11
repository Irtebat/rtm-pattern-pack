# Infrastructure

Terraform manages all AWS and Databricks resources for the pack. Scripts cover only the steps that are not resource management.

## Layout

| Path | Contents |
|---|---|
| `terraform/modules/network` | Discovers the workspace VPC (by tags, default `Resource = <workspace_name>`), its private subnets and the cluster security group, or takes explicit IDs. Creates security groups for MSK, Flink and the load generator. It does not modify any existing resource. |
| `terraform/modules/artifacts` | S3 bucket for the Flink jar, the harness bundle and benchmark results. |
| `terraform/modules/msk` | Amazon MSK (SASL/SCRAM over TLS), the SCRAM secret and its KMS key, broker configuration (LogAppendTime, no automatic topic creation). |
| `terraform/modules/flink_app` | One Managed Flink application per pattern (`rtmpack-flink-<id>`): IAM role, logs, jar upload, runtime properties. |
| `terraform/modules/loadgen_host` | EC2 host that runs the harness. Access goes through SSM Session Manager only. See its [README](terraform/modules/loadgen_host/README.md). |
| `terraform/modules/databricks_shared` | Secret scope `rtmpack` and the three Kafka secrets used by every RTM job. |
| `terraform/modules/databricks_rtm_job` | One continuous RTM job per pattern (`rtmpack-<id>-rtm`): job script upload and an RTM-compliant job cluster. Plan-time checks enforce slots >= tasks and slot use at or below 75 percent. |
| `terraform/envs/sandbox-usw2` | Root module. `patterns.tf` lists every pattern, keyed by pattern id, with its RTM sizing and job parameters. One RTM job and one Flink application are created per entry. |
| `scripts/build-flink.sh` | Builds a pattern's Flink jar. Maven downloads come from Maven Central. To use a mirror, set `RTMPACK_MAVEN_MIRROR`, or use your own Maven settings file. |
| `scripts/run-benchmark.sh` | Runs one pattern benchmark end to end and stops that pattern's engines afterwards. |
| `scripts/run-session.sh` | Runs several patterns in one session. It starts the load generator host once, runs each pattern, and stops the host at the end. |
| `scripts/update-harness.sh` | Reinstalls the current harness bundle on the host over SSM. |
| `scripts/test-pattern.sh` | Runs every local test for one pattern. Nothing in the cloud is touched. |

## Pointing the pack at your workspace

Copy `terraform/envs/sandbox-usw2/terraform.tfvars.example` to `terraform.tfvars` in the same folder, and fill in the placeholders. The key values are `workspace_name`, `databricks_profile`, `aws_profile` and `uc_catalog`.
- The network is discovered from the workspace name.
- MSK places brokers in every private subnet it finds (`msk_brokers_per_az` per zone).
- To use a VPC that is not tagged this way, set `workspace_vpc_id` and `workspace_security_group_id` explicitly.

The workspace must be a classic workspace. Its data plane VPC must be in the same AWS account and region as the benchmark resources.

## Prerequisites

- Terraform 1.6 or later, the AWS CLI version 2 with the Session Manager plugin, the Databricks CLI, JDK 17 and Maven.
- An AWS login for the profile in `terraform.tfvars` (for example `aws sso login --profile <your-aws-profile>`).
- A Databricks CLI login for the workspace profile in `terraform.tfvars`.

## Deploy

```bash
infra/scripts/build-flink.sh 01-dedup
infra/scripts/build-flink.sh 02-unnest
cd infra/terraform/envs/sandbox-usw2
terraform init
terraform plan -out=deploy.tfplan
terraform apply deploy.tfplan
```

Build the jar of every pattern you plan to run. Engines are created stopped. Two variables control them:
- `running_patterns` (default `[]`): patterns whose RTM job is unpaused and whose Flink application is started.
- `loadgen_running` (default `false`): whether the load generator host is running.

With the defaults, no engine compute is billed. MSK is billed while it exists, and it stays up between patterns so that later runs do not wait for a new cluster. A stopped host costs only its EBS volume.

The scripts cancel active RTM runs after stopping a pattern. After a manual stop, cancel them too:

```bash
terraform apply -var 'running_patterns=[]' -var loadgen_running=false
databricks jobs cancel-all-runs --job-id <rtm_job_id> --profile <your-databricks-profile>
```

The job IDs are in `terraform output patterns`.

## Run benchmarks

### Sessions

The usual entry point is a session. It batches patterns, starts the load generator host once, and uses the tuned profiles from contract section 8:

```bash
infra/scripts/run-session.sh --patterns 01-dedup,02-unnest
```

The session takes `--rate`, `--duration` (default 1800), `--warmup` (default 300) and `--continue-on-error`. Run ids are `<id>-tuned-r<rate>-<UTC stamp>`.

### One pattern

```bash
infra/scripts/run-benchmark.sh --pattern 02-unnest --rate 10000 --duration 1800 --warmup 300
```

The options are:

| Option | Meaning |
|---|---|
| `--pattern <id>` | Pattern id (required) |
| `--rate <eps>` | Input rate; the default is the pattern's own default |
| `--duration <s>`, `--warmup <s>` | Run length and the part of it excluded from latency statistics |
| `--partitions <n>` | Partition count for the topics (default 8) |
| `--run-id <id>` | Override the generated run id |
| `--keep-running` | Do not stop the engines afterwards |
| `--tf-var name=value` | Pass a Terraform variable to the plan, repeatable (see variant runs) |

`run-benchmark.sh` does the following:
1. Creates the pattern topics, then starts the pattern's engines through a saved Terraform plan. It refuses to apply if the plan would delete or replace any resource.
2. Waits until both engines report RUNNING. It then sends probe bursts until both output topics receive records, and waits 90 s.
3. Runs the load generator over SSM.
4. Collects latency, correctness and split-half stability for each engine.
5. Copies the results to `results/` locally and to S3. It prints a note if the run must be repeated (contract section 8).
6. Stops the engines and cancels active RTM runs, also on failure. The host is stopped too, unless the run is part of a session.

The step-by-step view with code references is in [docs/HOW_A_RUN_WORKS.md](../docs/HOW_A_RUN_WORKS.md).

A session takes longer than the measured run for each pattern, because it includes start-up, probing and collection. The local machine must stay online for the whole session, because the scripts drive each step over SSM.

For a short smoke run, use `--rate 2000 --duration 180 --warmup 30`.

### Variant runs

A variant changes one RTM setting for one run. It is set through Terraform variables passed with `--tf-var`:
- `rtm_spark_version_overrides`: pattern id to Databricks runtime, for example `{"03-udf"="<runtime-version>"}`.
- `rtm_extra_params`: pattern id to extra job parameters, appended after the pattern's own. A repeated parameter such as `--checkpoint-location` takes the last value, which gives a variant its own checkpoint.
- `rtm_spark_conf_overrides`: pattern id to extra cluster Spark conf.

A variant run looks like this:

```bash
infra/scripts/run-benchmark.sh --pattern 03-udf \
  --tf-var 'rtm_spark_version_overrides={"03-udf"="<runtime-version>"}'
```

The results record shows the overrides in `engine_version` and `compute_desc`. The next apply without the variables reverts them.

## Testing a pattern locally

```bash
pip install -r patterns/requirements-test.txt      # once
infra/scripts/test-pattern.sh 06-tumbling
```

The argument is the pattern id, which is the folder name under `patterns/`. The script runs the RTM job tests (`pytest` in `patterns/<id>/rtm`), builds the Flink jar with its tests, and runs the harness tests. Use `--skip-flink` or `--skip-harness` to leave a part out. Set `RTMPACK_TEST_PYTHON` to an interpreter that has the test requirements installed. The same interpreter is used for the Spark workers.

## Adding a pattern

1. Add `patterns/<nn-name>/rtm` and `patterns/<nn-name>/flink`, following `01-dedup`. The Flink Java package is `com.rtmpack.<name>`, and the pom `artifactId` is `rtmpack-flink-<nn-name>`. Build the jar with `build-flink.sh <nn-name>`.
2. Add an entry to `local.patterns` in `envs/sandbox-usw2/patterns.tf`, keyed by the pattern id: script, worker count, `tasks_required`, checkpoint version and job parameters. Topics, jar path, job and application names are derived from the id. Size it so tasks use 50 to 75 percent of slots. The plan fails otherwise.
3. Register the pattern in `harness/rtmpack_harness/patterns.py` (generator and correctness rule). Add a Benchmark setup section for it in `patterns/<id>/README.md`, linked from the Per-pattern rules in `docs/CONTRACT.md`.
4. Write the pattern `README.md`, including why the use case needs sub-second latency. Add the pattern to the table in the root `README.md`.
5. Run `infra/scripts/test-pattern.sh <nn-name>`.

## Teardown

`terraform destroy` in the environment folder removes every resource the pack created. Existing workspace network resources are only read, so they are not affected. Review the destroy plan before applying it.

## Local state

State is local to the environment folder by default. To share it, copy `backend.tf.example` to `backend.tf` and point it at an S3 bucket.
