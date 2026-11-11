variable "name_prefix" {
  description = "Prefix applied to all resource names created by this module."
  type        = string
  default     = "rtmpack"
}

variable "app_name" {
  description = "Application name suffix, normally the pattern id (for example 01-dedup). The Flink application is named \"<name_prefix>-flink-<app_name>\"."
  type        = string
}

variable "owner" {
  description = "Owner tag value applied to all resources created by this module."
  type        = string
  default     = "unset"
}

variable "tags" {
  description = "Additional tags merged into the tags of all resources created by this module."
  type        = map(string)
  default     = {}
}

variable "artifact_bucket" {
  description = "Name of an existing S3 bucket that holds the application jar. The bucket is not created by this module."
  type        = string
}

variable "jar_path" {
  description = "Local filesystem path to the shaded application jar that is uploaded to the artifact bucket."
  type        = string
}

variable "runtime_environment" {
  description = "Managed Service for Apache Flink runtime identifier. FLINK-2_3 runs Flink 2.3 on Java 17."
  type        = string
  default     = "FLINK-2_3"
}

variable "parallelism" {
  description = "Application parallelism. The benchmark contract sets it equal to the Kafka partition count (default 8)."
  type        = number
  default     = 8

  validation {
    condition     = var.parallelism >= 1
    error_message = "parallelism must be at least 1."
  }
}

variable "parallelism_per_kpu" {
  description = "Number of parallel tasks per Kinesis Processing Unit (KPU)."
  type        = number
  default     = 1

  validation {
    condition     = var.parallelism_per_kpu >= 1 && var.parallelism_per_kpu <= 8
    error_message = "parallelism_per_kpu must be between 1 and 8."
  }
}

variable "checkpoint_interval_ms" {
  description = "Interval between Flink checkpoints, in milliseconds."
  type        = number
  default     = 60000

  validation {
    condition     = var.checkpoint_interval_ms >= 1000
    error_message = "checkpoint_interval_ms must be at least 1000."
  }
}

variable "subnet_ids" {
  description = "Subnet IDs for the application VPC configuration. Must be non-empty."
  type        = list(string)

  validation {
    condition     = length(var.subnet_ids) > 0
    error_message = "subnet_ids must contain at least one subnet ID."
  }
}

variable "security_group_ids" {
  description = "Security group IDs for the application VPC configuration. Must be non-empty."
  type        = list(string)

  validation {
    condition     = length(var.security_group_ids) > 0
    error_message = "security_group_ids must contain at least one security group ID."
  }
}

variable "kafka_bootstrap" {
  description = "Kafka SASL/SCRAM bootstrap servers (private endpoint, port 9096), as a comma-separated host:port list."
  type        = string
}

variable "kafka_username" {
  description = "Kafka SASL/SCRAM username used by the application."
  type        = string
}

variable "kafka_password" {
  description = "Kafka SASL/SCRAM password used by the application. Stored in application environment properties."
  type        = string
  sensitive   = true
}

variable "input_topic" {
  description = "Kafka topic the application reads from."
  type        = string
  default     = "rtmpack.01-dedup.input"
}

variable "output_topic" {
  description = "Kafka topic the application writes to."
  type        = string
  default     = "rtmpack.01-dedup.out.flink"
}

variable "state_ttl_ms" {
  description = "Time-to-live for keyed state (table.exec.state.ttl), in milliseconds."
  type        = number
  default     = 60000

  validation {
    condition     = var.state_ttl_ms >= 1000
    error_message = "state_ttl_ms must be at least 1000."
  }
}

variable "start_application" {
  description = "Whether Terraform should start the application after creating or updating it."
  type        = bool
  default     = false
}

variable "log_retention_days" {
  description = "CloudWatch Logs retention for the application log group, in days. 0 means never expire."
  type        = number
  default     = 14

  validation {
    condition = contains(
      [0, 1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, 400, 545, 731, 1096, 1827, 2192, 2557, 2922, 3288, 3653],
      var.log_retention_days
    )
    error_message = "log_retention_days must be one of the retention values accepted by CloudWatch Logs (0, 1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, 400, 545, 731, 1096, 1827, 2192, 2557, 2922, 3288, 3653)."
  }
}


variable "pattern_tag" {
  type        = string
  description = "Value of the pattern tag on the application resources: the pattern id, for example 01-dedup."
}
