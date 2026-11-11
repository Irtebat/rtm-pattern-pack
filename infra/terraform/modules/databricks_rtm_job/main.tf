data "databricks_current_user" "me" {}

locals {
  # Real-time mode schedules all stages at once, so every task needs a slot.
  # The driver does not run tasks, so slots = num_workers * worker_vcpus.
  slots_available = var.num_workers * var.worker_vcpus
  job_name        = "${var.name_prefix}-${var.pattern}-rtm"
  workspace_path  = "${data.databricks_current_user.me.home}/${var.name_prefix}/${var.pattern_dir}/rtm/${basename(var.script_path)}"
}

resource "databricks_workspace_file" "script" {
  source = var.script_path
  path   = local.workspace_path
}

resource "databricks_job" "this" {
  name                = local.job_name
  max_concurrent_runs = 1

  # Streaming query: when unpaused, the job keeps the query running and restarts it on failure.
  # Created paused so that no compute starts until a benchmark run unpauses it.
  continuous {
    pause_status = var.run_continuously ? "UNPAUSED" : "PAUSED"
  }

  job_cluster {
    job_cluster_key = "rtm"

    new_cluster {
      spark_version       = var.spark_version
      node_type_id        = var.node_type_id
      driver_node_type_id = var.driver_node_type_id # null: same as worker
      num_workers         = var.num_workers         # fixed size, autoscaling off

      runtime_engine = "STANDARD" # Photon off
      # Dedicated (single-user) access mode, owned by the deploying user.
      data_security_mode = "DATA_SECURITY_MODE_DEDICATED"
      single_user_name   = data.databricks_current_user.me.user_name

      aws_attributes {
        availability         = "ON_DEMAND" # spot off
        zone_id              = var.aws_attributes.zone_id
        instance_profile_arn = var.aws_attributes.instance_profile_arn
        # Node types without local disks (for example m5) need an EBS volume.
        ebs_volume_type  = var.aws_attributes.ebs_volume_count > 0 ? "GENERAL_PURPOSE_SSD" : null
        ebs_volume_count = var.aws_attributes.ebs_volume_count
        ebs_volume_size  = var.aws_attributes.ebs_volume_count > 0 ? var.aws_attributes.ebs_volume_size_gb : null
      }

      spark_conf = merge(
        var.extra_spark_conf,
        {
          "spark.databricks.streaming.realTimeMode.enabled" = "true"
          "spark.sql.shuffle.partitions"                    = tostring(var.shuffle_partitions)
        }
      )

      custom_tags = var.tags
    }
  }

  task {
    task_key        = var.task_key
    job_cluster_key = "rtm"

    spark_python_task {
      python_file = databricks_workspace_file.script.workspace_path
      parameters  = var.task_parameters
    }
  }

  lifecycle {
    precondition {
      condition     = local.slots_available >= var.tasks_required
      error_message = "RTM slot check failed: num_workers * worker_vcpus (${var.num_workers} * ${var.worker_vcpus} = ${local.slots_available}) must be >= tasks_required (${var.tasks_required}). Increase num_workers or reduce partitions."
    }
    precondition {
      condition     = var.tasks_required <= var.max_slot_utilization * local.slots_available
      error_message = "Tuned-profile slot check failed: tasks_required (${var.tasks_required}) must be <= ${var.max_slot_utilization} * slots (${local.slots_available}). Increase num_workers or reduce partitions."
    }
  }
}
