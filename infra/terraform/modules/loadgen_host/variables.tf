variable "vpc_id" {
  description = "VPC that hosts the load generator and Amazon MSK."
  type        = string
}

variable "subnet_id" {
  description = "Subnet for the load generator instance. Use a private subnet with a NAT gateway or VPC endpoints for S3, Secrets Manager and SSM."
  type        = string
}

variable "name_prefix" {
  description = "Prefix for resource names."
  type        = string
  default     = "rtmpack"
}

variable "tags" {
  description = "Tags applied to all taggable resources. Expected keys: project, owner, pattern."
  type        = map(string)
  default     = {}
}

variable "msk_secret_arn" {
  description = "ARN of the Secrets Manager secret AmazonMSK_rtmpack holding the MSK client credentials."
  type        = string
}

variable "kms_key_arn" {
  description = "ARN of the KMS key that encrypts the MSK secret. When set, kms:Decrypt is granted on this key."
  type        = string
  default     = null
}

variable "artifact_bucket" {
  description = "Name of an existing S3 bucket that receives the harness zip. The bucket is not created by this module."
  type        = string
}

variable "artifact_prefix" {
  description = "S3 key prefix under which the harness zip is stored."
  type        = string
  default     = "rtm-pattern-pack/loadgen"
}

variable "instance_type" {
  description = "EC2 instance type for the load generator."
  type        = string
  default     = "c6i.2xlarge"

  validation {
    condition     = length(var.instance_type) > 0
    error_message = "instance_type must be a non-empty string."
  }
}

variable "harness_source_dir" {
  description = "Directory of the rtmpack Python package to package and upload. Defaults to the repository harness/ directory when null."
  type        = string
  default     = null
}

variable "extra_security_group_ids" {
  type        = list(string)
  description = "Additional security groups for the instance, for example the group MSK accepts clients from."
  default     = []
}

variable "results_prefix" {
  description = "S3 key prefix the host uploads benchmark results to."
  type        = string
  default     = "rtm-pattern-pack/results"
}

variable "instance_running" {
  type        = bool
  description = "Desired instance state. false stops the instance (it is not terminated)."
  default     = true
}
