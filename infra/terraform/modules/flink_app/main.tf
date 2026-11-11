data "aws_partition" "current" {}

data "aws_region" "current" {}

data "aws_caller_identity" "current" {}

locals {
  app_name_full = "${var.name_prefix}-flink-${var.app_name}"

  # Jar object key inside the artifact bucket: flink/<name_prefix>-flink-<app_name>.jar.
  jar_key = "flink/${local.app_name_full}.jar"

  partition  = data.aws_partition.current.partition
  region     = data.aws_region.current.region
  account_id = data.aws_caller_identity.current.account_id

  log_group_name  = "/aws/kinesis-analytics/${local.app_name_full}"
  log_stream_name = "kinesis-analytics-log-stream"

  log_group_arn  = "arn:${local.partition}:logs:${local.region}:${local.account_id}:log-group:${local.log_group_name}"
  log_stream_arn = "${local.log_group_arn}:log-stream:${local.log_stream_name}"

  tags = merge(var.tags, {
    project = "rtm-pattern-pack"
    pattern = var.pattern_tag
    Owner   = var.owner
  })
}

# Execution role assumed by Managed Service for Apache Flink.
data "aws_iam_policy_document" "flink_assume_role" {
  statement {
    sid     = "AllowKinesisAnalyticsAssumeRole"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["kinesisanalytics.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "flink_exec" {
  # Read the application jar from the artifact bucket.
  statement {
    sid     = "ReadApplicationJar"
    effect  = "Allow"
    actions = ["s3:GetObject", "s3:GetObjectVersion"]
    resources = [
      "arn:${local.partition}:s3:::${var.artifact_bucket}/${aws_s3_object.jar.key}",
    ]
  }

  # Application log group and log stream access.
  statement {
    sid       = "ListApplicationLogGroups"
    effect    = "Allow"
    actions   = ["logs:DescribeLogGroups"]
    resources = ["${local.log_group_arn}:*"]
  }

  statement {
    sid       = "ListApplicationLogStreams"
    effect    = "Allow"
    actions   = ["logs:DescribeLogStreams"]
    resources = [local.log_stream_arn]
  }

  statement {
    sid       = "WriteApplicationLogs"
    effect    = "Allow"
    actions   = ["logs:PutLogEvents"]
    resources = [local.log_stream_arn]
  }

  # VPC access required for the application to run inside the subnets.
  # These EC2 actions do not support resource-level permissions for the
  # describe calls, so the resource is "*".
  statement {
    sid    = "VpcDescribe"
    effect = "Allow"
    actions = [
      "ec2:DescribeVpcs",
      "ec2:DescribeSubnets",
      "ec2:DescribeSecurityGroups",
      "ec2:DescribeDhcpOptions",
    ]
    resources = ["*"]
  }

  statement {
    sid    = "VpcNetworkInterfaces"
    effect = "Allow"
    actions = [
      "ec2:CreateNetworkInterface",
      "ec2:CreateNetworkInterfacePermission",
      "ec2:DescribeNetworkInterfaces",
      "ec2:DeleteNetworkInterface",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role" "flink_exec" {
  name               = "${var.name_prefix}-flink-${var.app_name}-exec"
  assume_role_policy = data.aws_iam_policy_document.flink_assume_role.json
  tags               = local.tags
}

resource "aws_iam_role_policy" "flink_exec" {
  name   = "${var.name_prefix}-flink-${var.app_name}-exec"
  role   = aws_iam_role.flink_exec.id
  policy = data.aws_iam_policy_document.flink_exec.json
}

resource "aws_cloudwatch_log_group" "flink" {
  name              = local.log_group_name
  retention_in_days = var.log_retention_days
  tags              = local.tags
}

# Log streams do not support tags.
resource "aws_cloudwatch_log_stream" "flink" {
  name           = local.log_stream_name
  log_group_name = aws_cloudwatch_log_group.flink.name
}

# Application jar, uploaded to the existing artifact bucket.
resource "aws_s3_object" "jar" {
  bucket      = var.artifact_bucket
  key         = local.jar_key
  source      = var.jar_path
  source_hash = filemd5(var.jar_path)
  tags        = local.tags
}

resource "aws_kinesisanalyticsv2_application" "flink" {
  name                   = local.app_name_full
  runtime_environment    = var.runtime_environment
  service_execution_role = aws_iam_role.flink_exec.arn
  application_mode       = "STREAMING"
  start_application      = var.start_application

  cloudwatch_logging_options {
    log_stream_arn = aws_cloudwatch_log_stream.flink.arn
  }

  application_configuration {
    application_code_configuration {
      code_content_type = "ZIPFILE"

      code_content {
        s3_content_location {
          bucket_arn = "arn:${local.partition}:s3:::${var.artifact_bucket}"
          file_key   = aws_s3_object.jar.key
          # Pinning the object version makes a new jar upload update the application.
          object_version = aws_s3_object.jar.version_id
        }
      }
    }

    application_snapshot_configuration {
      snapshots_enabled = true
    }

    environment_properties {
      property_group {
        property_group_id = "kafka"
        property_map = {
          "bootstrap.servers" = var.kafka_bootstrap
          "username"          = var.kafka_username
          "password"          = var.kafka_password
        }
      }

      property_group {
        property_group_id = "app"
        property_map = {
          "state.ttl.ms" = tostring(var.state_ttl_ms)
          "input.topic"  = var.input_topic
          "output.topic" = var.output_topic
        }
      }
    }

    flink_application_configuration {
      checkpoint_configuration {
        configuration_type    = "CUSTOM"
        checkpointing_enabled = true
        checkpoint_interval   = var.checkpoint_interval_ms
      }

      monitoring_configuration {
        configuration_type = "CUSTOM"
        log_level          = "INFO"
        metrics_level      = "APPLICATION"
      }

      parallelism_configuration {
        configuration_type   = "CUSTOM"
        auto_scaling_enabled = false
        parallelism          = var.parallelism
        parallelism_per_kpu  = var.parallelism_per_kpu
      }
    }

    vpc_configuration {
      subnet_ids         = var.subnet_ids
      security_group_ids = var.security_group_ids
    }
  }

  # The execution role policy must exist before the application is created or started.
  depends_on = [aws_iam_role_policy.flink_exec]

  tags = local.tags
}
