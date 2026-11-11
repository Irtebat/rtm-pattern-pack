variable "name_prefix" {
  type    = string
  default = "rtmpack"
}

variable "tags" {
  type    = map(string)
  default = {}
}

variable "subnet_ids" {
  type        = list(string)
  description = "Client subnets, one per availability zone. Broker count must be a multiple of this list's length."
}

variable "security_group_id" {
  type = string
}

variable "kafka_version" {
  type    = string
  default = "3.9.x"
}

variable "broker_count" {
  type        = number
  default     = 4
  description = "Brokers across all subnets. Replication factor 3 needs at least 3; with two AZs use an even number."
}

variable "broker_instance_type" {
  type    = string
  default = "kafka.m7g.large"
}

variable "broker_volume_gb" {
  type    = number
  default = 200
}

variable "scram_username" {
  type    = string
  default = "rtmpack"
}
