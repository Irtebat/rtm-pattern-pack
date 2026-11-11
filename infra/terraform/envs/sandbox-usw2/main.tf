# Environment: sandbox, us-west-2, one classic Databricks workspace (var.workspace_name).
#
# All benchmark infrastructure runs inside the existing workspace data-plane
# VPC. Existing network resources are only read, never modified.

locals {
  tags = {
    project = "rtm-pattern-pack"
    # Required by the sandbox tagging policy; untagged resources are marked for deletion.
    Owner = var.owner
    env   = "sandbox-usw2"
  }
}

module "network" {
  source = "../../modules/network"

  name_prefix                 = var.name_prefix
  tags                        = local.tags
  vpc_id                      = var.workspace_vpc_id
  vpc_lookup_tags             = { Resource = var.workspace_name }
  workspace_security_group_id = var.workspace_security_group_id
}

module "artifacts" {
  source = "../../modules/artifacts"

  name_prefix = var.name_prefix
  tags        = local.tags
}

module "msk" {
  source = "../../modules/msk"

  name_prefix          = var.name_prefix
  tags                 = local.tags
  subnet_ids           = module.network.private_subnet_ids
  security_group_id    = module.network.msk_security_group_id
  broker_count         = var.msk_brokers_per_az * module.network.availability_zone_count
  broker_instance_type = var.msk_broker_instance_type
}
