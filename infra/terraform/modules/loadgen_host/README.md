# loadgen_host

Terraform module that provisions the EC2 load generator host for the rtmpack harness. The harness writes each pattern's input topic and collects the output of both engines. The instance runs in the same VPC as Amazon MSK. It installs the `rtmpack` Python package at first boot. It is reached only through AWS Systems Manager Session Manager.

## What the module creates

- An S3 object holding a zip of the harness package, uploaded on `terraform apply`. The object key includes the MD5 of the zip, so a change to the harness source uploads a new object.
- A security group in the VPC with egress only and no ingress rules.
- An IAM role, instance profile, and inline policy. The role has the `AmazonSSMManagedInstanceCore` managed policy, read access to the MSK secret, `kms:Decrypt` on the secret's KMS key when one is given, and read access to the harness prefix in the artifact bucket.
- An EC2 instance based on the latest Amazon Linux 2023 x86_64 AMI. IMDSv2 is required. The root volume is encrypted. The instance has no public IP address and no key pair.

The bootstrap script (`user_data.sh.tftpl`) installs Python 3.11, downloads the harness zip into `/opt/rtmpack/harness`, creates a virtual environment at `/opt/rtmpack/venv`, and installs the package into it. Progress is logged to `/var/log/rtmpack-bootstrap.log`. Harness changes are installed on the running host with `infra/scripts/update-harness.sh`. The instance is not replaced.

## Prerequisites

- An existing S3 bucket for the harness artifact. The module does not create it.
- The Secrets Manager secret `AmazonMSK_rtmpack` with the MSK client credentials. Pass its ARN as `msk_secret_arn`.
- If the secret is encrypted with a customer managed KMS key, pass the key ARN as `kms_key_arn`.
- A VPC and subnet with a route to MSK, and a route to S3, Secrets Manager, and SSM. Use a NAT gateway or VPC endpoints for these services. The subnet should be private.

## Usage

```hcl
module "loadgen_host" {
  source = "./modules/loadgen_host"

  vpc_id          = "<vpc-id>"
  subnet_id       = "<private-subnet-id>"
  msk_secret_arn  = "<msk-secret-arn>"
  kms_key_arn     = "<kms-key-arn>"
  artifact_bucket = "<artifact-bucket>"

  tags = {
    project = "rtm-pattern-pack"
    owner   = "<owner-email>"
    pattern = "shared"
  }
}
```

## Inputs

| Name | Type | Default | Required | Description |
|------|------|---------|----------|-------------|
| `vpc_id` | `string` | n/a | yes | VPC that hosts the load generator and MSK. |
| `subnet_id` | `string` | n/a | yes | Subnet for the instance. Use a private subnet. |
| `name_prefix` | `string` | `"rtmpack"` | no | Prefix for resource names. |
| `tags` | `map(string)` | `{}` | no | Tags applied to all taggable resources. |
| `msk_secret_arn` | `string` | n/a | yes | ARN of the Secrets Manager secret `AmazonMSK_rtmpack`. |
| `kms_key_arn` | `string` | `null` | no | KMS key that encrypts the secret. When set, `kms:Decrypt` is granted on it. |
| `artifact_bucket` | `string` | n/a | yes | Name of an existing S3 bucket for the harness zip. |
| `artifact_prefix` | `string` | `"rtm-pattern-pack/loadgen"` | no | S3 key prefix for the harness zip. |
| `instance_type` | `string` | `"c6i.2xlarge"` | no | EC2 instance type. |
| `harness_source_dir` | `string` | `"${path.module}/../../../../harness"` | no | Directory of the rtmpack package to package and upload. |

## Outputs

| Name | Description |
|------|-------------|
| `instance_id` | EC2 instance ID of the load generator. |
| `security_group_id` | ID of the load generator security group. |
| `role_arn` | ARN of the IAM role attached to the instance. |
| `instance_profile_name` | Name of the IAM instance profile. |
| `harness_s3_uri` | S3 URI of the uploaded harness zip. |

## Access

The instance has no inbound ports and no key pair. Open a shell with Session Manager:

```sh
aws ssm start-session --target <instance_id>
```

On the instance, load the environment and run the harness. The benchmark scripts do this for you over SSM. See [infra/README.md](../../../README.md).

```sh
source /opt/rtmpack/rtmpack.env
rtmpack --help
```

## Harness artifact

The harness zip is built from `harness_source_dir` and uploaded on `terraform apply`. The `build/` directory inside the module holds the local zip and is not committed. The `__pycache__`, `*.pyc`, `.pytest_cache`, and `results/` paths are excluded from the zip.
