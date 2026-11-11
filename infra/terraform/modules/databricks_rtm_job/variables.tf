variable "name_prefix" {
  description = "Prefix for the job name and workspace paths."
  type        = string
  default     = "rtmpack"
}

variable "pattern" {
  description = "Pattern id used in the job name, for example 01-dedup. The job is named <name_prefix>-<pattern>-rtm."
  type        = string
}

variable "pattern_dir" {
  description = "Pattern directory name used in the workspace path, for example 01-dedup."
  type        = string
}

variable "script_path" {
  description = "Local path of the PySpark script to upload."
  type        = string
}

variable "task_key" {
  description = "Task key of the single Python task, for example dedup_rtm."
  type        = string
}

variable "task_parameters" {
  description = "Command-line parameters passed to the Python task."
  type        = list(string)
}

variable "spark_version" {
  description = "Databricks Runtime version string. RTM needs 16.4 LTS or later."
  type        = string
  default     = "18.x-scala2.13"
}

variable "node_type_id" {
  description = "Worker node type (AWS). Must be a classic instance type."
  type        = string
  default     = "m5.xlarge"
}

variable "driver_node_type_id" {
  description = "Driver node type. null means the same as node_type_id."
  type        = string
  default     = null
}

variable "num_workers" {
  description = "Fixed worker count. Autoscaling is off."
  type        = number
  default     = 4

  validation {
    condition     = var.num_workers >= 1
    error_message = "num_workers must be at least 1."
  }
}

variable "worker_vcpus" {
  description = "vCPUs per worker for node_type_id (m5.xlarge = 4). Used for the slot check."
  type        = number
  default     = 4

  validation {
    condition     = var.worker_vcpus >= 1
    error_message = "worker_vcpus must be at least 1."
  }
}

variable "tasks_required" {
  description = "Tasks that must run concurrently across all stages (source partitions plus shuffle partitions for shuffling queries)."
  type        = number

  validation {
    condition     = var.tasks_required >= 1
    error_message = "tasks_required must be at least 1."
  }
}

variable "shuffle_partitions" {
  description = "Value of spark.sql.shuffle.partitions on the job cluster."
  type        = number

  validation {
    condition     = var.shuffle_partitions >= 1
    error_message = "shuffle_partitions must be at least 1."
  }
}

variable "extra_spark_conf" {
  description = "Additional Spark configuration for the job cluster. The RTM and shuffle partition keys set by this module take precedence."
  type        = map(string)
  default     = {}
}

variable "aws_attributes" {
  description = "Optional AWS settings for the job cluster. Availability is always ON_DEMAND (spot off)."
  type = object({
    zone_id              = optional(string, "auto")
    instance_profile_arn = optional(string)
    ebs_volume_count     = optional(number, 1)
    ebs_volume_size_gb   = optional(number, 100)
  })
  default = {}
}

variable "run_continuously" {
  description = "Unpause the continuous job. Keep false outside benchmark runs so no compute is started."
  type        = bool
  default     = false
}

variable "tags" {
  description = "Tags applied to the job cluster as custom_tags. Expected keys: project, owner, pattern."
  type        = map(string)
  default     = {}
}

variable "max_slot_utilization" {
  description = "Maximum share of slots the query may use (contract section 8 tuned profile: 0.75)."
  type        = number
  default     = 0.75

  validation {
    condition     = var.max_slot_utilization > 0 && var.max_slot_utilization <= 1
    error_message = "max_slot_utilization must be in (0, 1]."
  }
}
