module "loadgen" {
  source = "../../modules/loadgen_host"

  name_prefix              = var.name_prefix
  tags                     = local.tags
  vpc_id                   = module.network.vpc_id
  subnet_id                = module.network.private_subnet_ids[0]
  extra_security_group_ids = [module.network.loadgen_security_group_id]
  instance_type            = var.loadgen_instance_type
  instance_running         = var.loadgen_running

  msk_secret_arn  = module.msk.scram_secret_arn
  kms_key_arn     = module.msk.scram_kms_key_arn
  artifact_bucket = module.artifacts.bucket_name
}
