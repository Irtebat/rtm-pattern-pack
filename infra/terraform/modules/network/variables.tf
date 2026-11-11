variable "name_prefix" {
  type    = string
  default = "rtmpack"
}

variable "tags" {
  type    = map(string)
  default = {}
}

variable "vpc_id" {
  type        = string
  description = "Workspace data-plane VPC. Leave null to discover it with vpc_lookup_tags."
  default     = null
}

variable "vpc_lookup_tags" {
  type        = map(string)
  description = "Tags that identify the workspace VPC when vpc_id is null (FEVM tags it with Resource = <workspace name>)."
  default     = {}
}

variable "workspace_security_group_id" {
  type        = string
  description = "Security group on Databricks cluster nodes. Leave null to discover it."
  default     = null
}

variable "workspace_security_group_name_pattern" {
  type        = string
  description = "Group-name filter used to discover the workspace security group. Must match exactly one group in the VPC."
  default     = "terraform-*"
}

variable "private_subnet_name_pattern" {
  type        = string
  description = "Name tag pattern for the workspace private subnets (wildcards allowed)."
  default     = "*private_subnet*"
}
