# Amazon MSK module.
#
# Provisioned MSK cluster with SASL/SCRAM-SHA-512 over TLS. The SCRAM secret is
# stored in Secrets Manager under the AmazonMSK_ prefix and encrypted with a
# customer managed KMS key, as MSK requires.

terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

resource "aws_kms_key" "msk_scram" {
  description             = "${var.name_prefix} MSK SCRAM secret key"
  deletion_window_in_days = 7
  enable_key_rotation     = true
  tags                    = var.tags
}

resource "aws_kms_alias" "msk_scram" {
  name          = "alias/${var.name_prefix}-msk-scram"
  target_key_id = aws_kms_key.msk_scram.key_id
}

resource "random_password" "scram" {
  length  = 32
  special = false
}

resource "aws_secretsmanager_secret" "scram" {
  name                    = "AmazonMSK_${var.name_prefix}"
  kms_key_id              = aws_kms_key.msk_scram.arn
  recovery_window_in_days = 7
  tags                    = var.tags
}

resource "aws_secretsmanager_secret_version" "scram" {
  secret_id = aws_secretsmanager_secret.scram.id
  secret_string = jsonencode({
    username = var.scram_username
    password = random_password.scram.result
  })
}

resource "aws_msk_configuration" "this" {
  name           = "${var.name_prefix}-config"
  kafka_versions = [var.kafka_version]
  description    = "Benchmark defaults: broker clock timestamps, no auto topic creation"

  server_properties = <<-PROPS
    auto.create.topics.enable=false
    default.replication.factor=3
    min.insync.replicas=2
    num.partitions=8
    log.message.timestamp.type=LogAppendTime
    log.retention.hours=24
    allow.everyone.if.no.acl.found=true
    unclean.leader.election.enable=false
  PROPS
}

resource "aws_cloudwatch_log_group" "broker" {
  name              = "/aws/msk/${var.name_prefix}"
  retention_in_days = 7
  tags              = var.tags
}

resource "aws_msk_cluster" "this" {
  cluster_name           = var.name_prefix
  kafka_version          = var.kafka_version
  number_of_broker_nodes = var.broker_count

  broker_node_group_info {
    instance_type   = var.broker_instance_type
    client_subnets  = var.subnet_ids
    security_groups = [var.security_group_id]

    storage_info {
      ebs_storage_info {
        volume_size = var.broker_volume_gb
      }
    }
  }

  client_authentication {
    unauthenticated = false
    sasl {
      scram = true
      iam   = false
    }
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
  }

  configuration_info {
    arn      = aws_msk_configuration.this.arn
    revision = aws_msk_configuration.this.latest_revision
  }

  enhanced_monitoring = "PER_TOPIC_PER_BROKER"

  logging_info {
    broker_logs {
      cloudwatch_logs {
        enabled   = true
        log_group = aws_cloudwatch_log_group.broker.name
      }
    }
  }

  tags = var.tags
}

resource "aws_msk_single_scram_secret_association" "this" {
  cluster_arn = aws_msk_cluster.this.arn
  secret_arn  = aws_secretsmanager_secret.scram.arn
  depends_on  = [aws_secretsmanager_secret_version.scram]
}

# Build a stable, sorted list from all nodes so the value does not change between plans.
data "aws_msk_broker_nodes" "this" {
  cluster_arn = aws_msk_cluster.this.arn
}

locals {
  bootstrap_sasl_scram = join(",", sort([
    for n in data.aws_msk_broker_nodes.this.node_info_list : "${tolist(n.endpoints)[0]}:9096"
  ]))
}
