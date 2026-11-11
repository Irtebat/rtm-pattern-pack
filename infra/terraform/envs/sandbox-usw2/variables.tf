variable "aws_profile" {
  type = string
}

variable "aws_region" {
  type    = string
  default = "us-west-2"
}

variable "databricks_profile" {
  type = string
}

variable "owner" {
  type        = string
  description = "Owner email, applied as a tag on every resource."
}

variable "name_prefix" {
  type    = string
  default = "rtmpack"
}

# Existing workspace network (read only). By default it is discovered from the
# workspace name; explicit IDs override discovery.
variable "workspace_name" {
  type        = string
  description = "Deployment name of the classic workspace. The data-plane VPC is found by the tag Resource = <name>."
}

variable "workspace_vpc_id" {
  type    = string
  default = null
}

variable "workspace_security_group_id" {
  type    = string
  default = null
}

# Unity Catalog location for checkpoints and results.
variable "uc_catalog" {
  type = string
}

variable "uc_schema" {
  type    = string
  default = "rtmpack"
}

# MSK sizing.
variable "msk_brokers_per_az" {
  type        = number
  description = "Brokers per availability zone. The total must be at least 3 for replication factor 3."
  default     = 2
}

variable "msk_broker_instance_type" {
  type    = string
  default = "kafka.m7g.large"
}

# Benchmark shape shared by both engines.
variable "partitions" {
  type    = number
  default = 8
}

# Which patterns' engines are running. Empty means every RTM job is paused and
# every Flink application is stopped. Benchmark scripts set this per run.
variable "running_patterns" {
  type        = list(string)
  description = "Pattern ids whose RTM job is unpaused and Flink application started, for example [\"01-dedup\"]."
  default     = []
}

# Tuned profile (docs/CONTRACT.md section 8).
variable "rtm_batch_interval_seconds" {
  type        = number
  description = "RTM real-time batch (checkpoint) interval. The documented default is 5 minutes."
  default     = 300
}

variable "flink_checkpoint_interval_seconds" {
  type        = number
  description = "Managed Flink checkpoint interval. The service default is 60 seconds."
  default     = 60
}

variable "kafka_linger_ms" {
  type    = number
  default = 0
}

# List prices used for the cost column of results (us-west-2, on demand).
variable "ec2_usd_per_hour" {
  type    = map(number)
  default = { "m5.large" = 0.096, "m5.xlarge" = 0.192, "m5.2xlarge" = 0.384 }
}

variable "dbu_per_node_hour" {
  type    = map(number)
  default = { "m5.large" = 0.345, "m5.xlarge" = 0.69, "m5.2xlarge" = 1.37 }
}

variable "dbu_usd" {
  type        = number
  description = "Enterprise Jobs Compute list price per DBU (system.billing.list_prices)."
  default     = 0.20
}

variable "kpu_usd_per_hour" {
  type    = number
  default = 0.11
}

# Spark RTM compute.
variable "rtm_spark_version" {
  type    = string
  default = "18.x-scala2.13" # DBR 18 LTS
}

variable "rtm_node_type_id" {
  type    = string
  default = "m5.xlarge"
}

variable "rtm_worker_vcpus" {
  type    = number
  default = 4
}

# Managed Flink compute.
variable "flink_runtime_environment" {
  type = string
}

variable "flink_parallelism" {
  type    = number
  default = 8
}

variable "flink_parallelism_per_kpu" {
  type    = number
  default = 1
}

# Load generator host.
variable "loadgen_running" {
  type        = bool
  description = "Start the load generator host. Benchmark scripts set this to true for a session; it is stopped otherwise."
  default     = false
}

variable "loadgen_instance_type" {
  type    = string
  default = "c6i.2xlarge"
}

# Per-pattern overrides for variant runs (for example a different runtime for one
# pattern). Pass them with run-benchmark.sh --tf-var; a normal apply reverts them.
variable "rtm_spark_version_overrides" {
  description = "Pattern id -> Databricks runtime for that pattern's RTM job, instead of rtm_spark_version. Example: {\"03-udf\" = \"<runtime version string>\"}."
  type        = map(string)
  default     = {}
}

variable "rtm_extra_params" {
  description = "Pattern id -> extra RTM job parameters appended after the pattern's own. Example: {\"05-lookup\" = [\"--reference-mode\", \"cached\", \"--checkpoint-location\", \"/Volumes/<catalog>/<schema>/<volume>/checkpoints/05-lookup-rtm-cached-run1\"]}."
  type        = map(list(string))
  default     = {}
}

variable "rtm_spark_conf_overrides" {
  description = "Pattern id -> extra cluster Spark conf for that pattern's RTM job, merged over the pattern's own. Example: {\"06-tumbling\" = {\"<spark conf key>\" = \"<value>\"}}."
  type        = map(map(string))
  default     = {}
}
