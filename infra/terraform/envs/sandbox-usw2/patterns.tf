# Pattern registry: one entry per benchmarked pattern, keyed by the pattern id (the
# folder name under patterns/). Adding a pattern means adding an entry here, plus
# its RTM script and Flink project under patterns/<id>/.
#
# Raise rtm_checkpoint_version when a change needs a new RTM checkpoint, for example
# a new shuffle partition count, which is stored in the checkpoint of a stateful query.
# The checkpoint is <checkpoint volume>/<id>-rtm-v<rtm_checkpoint_version>.
#
# Names derived from the id: topics rtmpack.<id>.input and rtmpack.<id>.out.<engine>,
# RTM job rtmpack-<id>-rtm, Flink application rtmpack-flink-<id>, Flink jar
# patterns/<id>/flink/target/rtmpack-flink-<id>-1.0.0.jar, tag pattern = <id>.
#
# RTM sizing follows the tuned profile (docs/CONTRACT.md section 8): source tasks
# equal the topic partitions, shuffle partitions keep total tasks within 50 to 75
# percent of slots, and the cluster is the smallest that satisfies that.

locals {
  patterns_root = "${path.root}/../../../../patterns"

  patterns = {
    "01-dedup" = {
      rtm_script = "dedup_rtm.py"
      rtm_task   = "dedup_rtm"
      # 8 source tasks + 4 shuffle tasks = 12 of 16 slots (75 percent)
      rtm_workers            = 4
      rtm_shuffle            = 4
      rtm_tasks_required     = var.partitions + 4
      rtm_checkpoint_version = 1
      rtm_params             = ["--partitions", "4", "--state-horizon", "60 seconds"]
      flink_ttl_ms           = 60000
      rtm_spark_conf         = {}
    }
    "02-unnest" = {
      rtm_script = "unnest_rtm.py"
      rtm_task   = "unnest_rtm"
      # Stateless, no shuffle: 8 source tasks on 12 slots (67 percent)
      rtm_workers            = 3
      rtm_shuffle            = var.partitions
      rtm_tasks_required     = var.partitions
      rtm_checkpoint_version = 1
      rtm_params             = []
      flink_ttl_ms           = 60000 # unused by the stateless job
      rtm_spark_conf         = {}
    }
    "03-udf" = {
      rtm_script = "udf_rtm.py"
      rtm_task   = "udf_rtm"
      # Stateless, no shuffle: 8 source tasks on 12 slots (67 percent)
      rtm_workers            = 3
      rtm_shuffle            = var.partitions
      rtm_tasks_required     = var.partitions
      rtm_checkpoint_version = 1
      rtm_params             = []
      flink_ttl_ms           = 60000 # unused by the stateless job
      rtm_spark_conf         = {}
    }
    "04-routing" = {
      rtm_script = "routing_rtm.py"
      rtm_task   = "routing_rtm"
      # Stateless, no shuffle: 8 source tasks on 12 slots (67 percent)
      rtm_workers            = 3
      rtm_shuffle            = var.partitions
      rtm_tasks_required     = var.partitions
      rtm_checkpoint_version = 1
      rtm_params             = []
      flink_ttl_ms           = 60000 # unused by the stateless job
      rtm_spark_conf         = {}
    }
    "05-lookup" = {
      rtm_script = "lookup_rtm.py"
      rtm_task   = "lookup_rtm"
      # Broadcast stream-static join, no streaming shuffle: 8 tasks on 12 slots (67 percent)
      rtm_workers            = 3
      rtm_shuffle            = var.partitions
      rtm_tasks_required     = var.partitions
      rtm_checkpoint_version = 1
      rtm_params             = ["--reference-path", databricks_file.merchants.path]
      flink_ttl_ms           = 60000 # unused by the stateless job
      rtm_spark_conf         = {}
    }
    "06-tumbling" = {
      rtm_script = "tumbling_rtm.py"
      rtm_task   = "tumbling_rtm"
      # 8 source tasks + 4 shuffle tasks = 12 of 16 slots (75 percent)
      rtm_workers            = 4
      rtm_shuffle            = 4
      rtm_tasks_required     = var.partitions + 4
      rtm_checkpoint_version = 1
      rtm_params             = ["--partitions", "4", "--state-horizon", "10 seconds"]
      flink_ttl_ms           = 120000
      rtm_spark_conf         = {}
    }
    "07-sliding" = {
      rtm_script = "sliding_rtm.py"
      rtm_task   = "sliding_rtm"
      # 8 source tasks + 4 shuffle tasks = 12 of 16 slots (75 percent)
      rtm_workers            = 4
      rtm_shuffle            = 4
      rtm_tasks_required     = var.partitions + 4
      rtm_checkpoint_version = 1
      rtm_params             = ["--partitions", "4", "--state-horizon", "10 seconds"]
      flink_ttl_ms           = 120000
      rtm_spark_conf         = {}
    }
    "08-join" = {
      rtm_script = "join_rtm.py"
      rtm_task   = "join_rtm"
      # 8 + 8 source tasks + 4 shuffle tasks = 20 of 28 slots (71 percent)
      rtm_workers            = 7
      rtm_shuffle            = 4
      rtm_tasks_required     = 2 * var.partitions + 4
      rtm_checkpoint_version = 1
      rtm_params             = ["--partitions", "4", "--watermark-delay", "30 seconds", "--join-window", "30 seconds"]
      flink_ttl_ms           = 60000 # interval join state is cleaned by watermark
      # Stream-stream join settings from the RTM setup page, set on the cluster as well as in the job.
      rtm_spark_conf = {
        "spark.databricks.streaming.realTimeMode.streamStreamJoin.enabled" = "true"
        "spark.sql.streaming.join.stateFormatVersion"                      = "4"
        "spark.sql.streaming.join.stateFormatV4.enabled"                   = "true"
        "spark.sql.streaming.stateStore.rocksdb.mergeOperatorVersion"      = "2"
        "spark.sql.streaming.realTimeMode.controlMessage.enabled"          = "true"
      }
    }
  }

  checkpoint_root = "/Volumes/${var.uc_catalog}/${databricks_schema.pack.name}/${databricks_volume.checkpoints.name}"

  # Hourly list cost per pattern, for the results record.
  rtm_node_usd   = var.ec2_usd_per_hour[var.rtm_node_type_id] + var.dbu_per_node_hour[var.rtm_node_type_id] * var.dbu_usd
  flink_kpu      = var.flink_parallelism / var.flink_parallelism_per_kpu + 1
  flink_usd_hour = local.flink_kpu * var.kpu_usd_per_hour
}

module "rtm_job" {
  source   = "../../modules/databricks_rtm_job"
  for_each = local.patterns

  name_prefix = var.name_prefix
  pattern     = each.key
  pattern_dir = each.key
  tags        = merge(local.tags, { pattern = each.key })
  script_path = "${local.patterns_root}/${each.key}/rtm/${each.value.rtm_script}"
  task_key    = each.value.rtm_task

  task_parameters = concat([
    "--secret-scope", module.databricks_shared.secret_scope_name,
    "--input-topic", "${var.name_prefix}.${each.key}.input",
    "--output-topic", "${var.name_prefix}.${each.key}.out.rtm",
    "--checkpoint-interval", "${var.rtm_batch_interval_seconds} seconds",
    "--checkpoint-location", "${local.checkpoint_root}/${each.key}-rtm-v${each.value.rtm_checkpoint_version}",
    "--kafka-linger-ms", tostring(var.kafka_linger_ms),
  ], each.value.rtm_params, lookup(var.rtm_extra_params, each.key, []))

  spark_version      = lookup(var.rtm_spark_version_overrides, each.key, var.rtm_spark_version)
  node_type_id       = var.rtm_node_type_id
  worker_vcpus       = var.rtm_worker_vcpus
  num_workers        = each.value.rtm_workers
  shuffle_partitions = each.value.rtm_shuffle
  tasks_required     = each.value.rtm_tasks_required
  extra_spark_conf   = merge(each.value.rtm_spark_conf, lookup(var.rtm_spark_conf_overrides, each.key, {}))
  run_continuously   = contains(var.running_patterns, each.key)
}

module "flink_app" {
  source   = "../../modules/flink_app"
  for_each = local.patterns

  name_prefix = var.name_prefix
  app_name    = each.key
  pattern_tag = each.key
  owner       = var.owner
  tags        = local.tags

  artifact_bucket     = module.artifacts.bucket_name
  jar_path            = "${local.patterns_root}/${each.key}/flink/target/rtmpack-flink-${each.key}-1.0.0.jar"
  runtime_environment = var.flink_runtime_environment

  parallelism            = var.flink_parallelism
  parallelism_per_kpu    = var.flink_parallelism_per_kpu
  checkpoint_interval_ms = var.flink_checkpoint_interval_seconds * 1000
  state_ttl_ms           = each.value.flink_ttl_ms
  input_topic            = "${var.name_prefix}.${each.key}.input"
  output_topic           = "${var.name_prefix}.${each.key}.out.flink"

  subnet_ids         = module.network.private_subnet_ids
  security_group_ids = [module.network.flink_security_group_id]

  kafka_bootstrap = module.msk.bootstrap_brokers_sasl_scram
  kafka_username  = module.msk.scram_username
  kafka_password  = module.msk.scram_password

  start_application = contains(var.running_patterns, each.key)
}
