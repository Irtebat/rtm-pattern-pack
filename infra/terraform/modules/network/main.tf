# Network module.
#
# Reuses the existing Databricks workspace data-plane VPC and its private subnets.
# Nothing existing is modified: the VPC, subnets, route tables and the workspace
# security group are read through data sources only. The module creates new
# security groups for MSK, the Flink application and the load generator host.

terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

# The workspace VPC is either given explicitly or discovered from tags, so the
# same environment works for any classic workspace whose network lives in this
# AWS account.
data "aws_vpc" "workspace" {
  id   = var.vpc_id
  tags = var.vpc_id == null ? var.vpc_lookup_tags : null
}

data "aws_subnets" "private" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.workspace.id]
  }
  filter {
    name   = "tag:Name"
    values = [var.private_subnet_name_pattern]
  }
}

data "aws_subnet" "private" {
  for_each = toset(data.aws_subnets.private.ids)
  id       = each.value
}

# Workspace (cluster node) security group: explicit, or discovered as the
# non-default groups in the VPC that match the lookup filter.
data "aws_security_groups" "workspace_candidates" {
  count = var.workspace_security_group_id == null ? 1 : 0

  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.workspace.id]
  }
  filter {
    name   = "group-name"
    values = [var.workspace_security_group_name_pattern]
  }
}

locals {
  workspace_sg_id = coalesce(
    var.workspace_security_group_id,
    try(one(data.aws_security_groups.workspace_candidates[0].ids), null),
  )
}

data "aws_security_group" "workspace" {
  id = local.workspace_sg_id
}

locals {
  # Sort subnets by availability zone so broker placement is stable across runs.
  private_subnet_ids = [
    for az in sort(distinct([for s in data.aws_subnet.private : s.availability_zone])) :
    one([for s in data.aws_subnet.private : s.id if s.availability_zone == az])
  ]
}

resource "aws_security_group" "flink" {
  name        = "${var.name_prefix}-flink"
  description = "Managed Flink application ENIs"
  vpc_id      = data.aws_vpc.workspace.id
  tags        = merge(var.tags, { Name = "${var.name_prefix}-flink" })
}

resource "aws_vpc_security_group_egress_rule" "flink_all" {
  security_group_id = aws_security_group.flink.id
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"
  description       = "Outbound to MSK and AWS APIs"
}

resource "aws_security_group" "loadgen" {
  name        = "${var.name_prefix}-loadgen"
  description = "Benchmark load generator and collector host"
  vpc_id      = data.aws_vpc.workspace.id
  tags        = merge(var.tags, { Name = "${var.name_prefix}-loadgen" })
}

resource "aws_vpc_security_group_egress_rule" "loadgen_all" {
  security_group_id = aws_security_group.loadgen.id
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"
  description       = "Outbound to MSK, SSM, S3 and package repositories"
}

resource "aws_security_group" "msk" {
  name        = "${var.name_prefix}-msk"
  description = "MSK brokers; SASL/SCRAM over TLS from benchmark clients only"
  vpc_id      = data.aws_vpc.workspace.id
  tags        = merge(var.tags, { Name = "${var.name_prefix}-msk" })
}

locals {
  msk_client_sgs = {
    workspace = data.aws_security_group.workspace.id
    flink     = aws_security_group.flink.id
    loadgen   = aws_security_group.loadgen.id
  }
}

resource "aws_vpc_security_group_ingress_rule" "msk_scram" {
  for_each                     = local.msk_client_sgs
  security_group_id            = aws_security_group.msk.id
  referenced_security_group_id = each.value
  ip_protocol                  = "tcp"
  from_port                    = 9096
  to_port                      = 9096
  description                  = "SASL/SCRAM TLS from ${each.key}"
}

resource "aws_vpc_security_group_egress_rule" "msk_all" {
  security_group_id = aws_security_group.msk.id
  ip_protocol       = "-1"
  cidr_ipv4         = data.aws_vpc.workspace.cidr_block
  description       = "Broker responses within the VPC"
}
