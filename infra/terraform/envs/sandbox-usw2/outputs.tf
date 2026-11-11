output "msk_bootstrap" {
  value = module.msk.bootstrap_brokers_sasl_scram
}

output "msk_scram_secret_arn" {
  value = module.msk.scram_secret_arn
}

output "artifact_bucket" {
  value = module.artifacts.bucket_name
}

output "private_subnet_ids" {
  value = module.network.private_subnet_ids
}

output "checkpoint_volume" {
  value = local.checkpoint_root
}

output "loadgen_instance_id" {
  value = module.loadgen.instance_id
}

# Everything the run scripts need per pattern: engine ids, descriptions and cost.
output "patterns" {
  value = {
    for k, p in local.patterns : k => {
      rtm_job_id          = module.rtm_job[k].job_id
      rtm_job_url         = module.rtm_job[k].job_url
      rtm_version         = "DBR ${lookup(var.rtm_spark_version_overrides, k, var.rtm_spark_version)}"
      rtm_desc            = "1 driver + ${p.rtm_workers} workers ${var.rtm_node_type_id}, classic, dedicated, ${p.rtm_tasks_required} tasks on ${module.rtm_job[k].slots_available} slots, batch interval ${var.rtm_batch_interval_seconds}s, linger ${var.kafka_linger_ms} ms${length(lookup(var.rtm_extra_params, k, [])) > 0 ? ", ${join(" ", lookup(var.rtm_extra_params, k, []))}" : ""}${length(lookup(var.rtm_spark_conf_overrides, k, {})) > 0 ? ", conf ${join(" ", [for ck, cv in lookup(var.rtm_spark_conf_overrides, k, {}) : "${ck}=${cv}"])}" : ""}"
      rtm_vcpu            = (p.rtm_workers + 1) * var.rtm_worker_vcpus
      rtm_cost_per_hour   = format("%.4f", (p.rtm_workers + 1) * local.rtm_node_usd)
      flink_app           = module.flink_app[k].application_name
      flink_version       = var.flink_runtime_environment
      flink_desc          = "${local.flink_kpu} KPU (parallelism ${var.flink_parallelism}, ${var.flink_parallelism_per_kpu} per KPU, plus orchestration), checkpoint ${var.flink_checkpoint_interval_seconds}s, linger ${var.kafka_linger_ms} ms"
      flink_vcpu          = local.flink_kpu
      flink_cost_per_hour = format("%.4f", local.flink_usd_hour)
    }
  }
}
