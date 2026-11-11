data "aws_region" "current" {}

data "aws_ami" "al2023" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-2023*-x86_64"]
  }

  filter {
    name   = "architecture"
    values = ["x86_64"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

locals {
  name               = "${var.name_prefix}-loadgen"
  harness_source_dir = coalesce(var.harness_source_dir, "${path.module}/../../../../harness")

  # Fixed key: harness updates overwrite the object (the bucket is versioned) and are
  # installed on the running host with infra/scripts/update-harness.sh.
  harness_key = "${var.artifact_prefix}/rtmpack-harness.zip"
  harness_uri = "s3://${var.artifact_bucket}/${local.harness_key}"
}

data "archive_file" "harness" {
  type        = "zip"
  source_dir  = local.harness_source_dir
  output_path = "${path.module}/build/rtmpack-harness.zip"

  excludes = [
    "**/__pycache__",
    "**/__pycache__/**",
    "**/*.pyc",
    "**/.pytest_cache",
    "**/.pytest_cache/**",
    "**/results",
    "**/results/**",
  ]
}

# Earlier versions stored the zip under a content-hash key. Stop tracking that
# object without deleting it.
removed {
  from = aws_s3_object.harness

  lifecycle {
    destroy = false
  }
}

resource "aws_s3_object" "harness_bundle" {
  bucket       = var.artifact_bucket
  key          = local.harness_key
  source       = data.archive_file.harness.output_path
  etag         = data.archive_file.harness.output_md5
  content_type = "application/zip"
  tags         = var.tags
}

resource "aws_security_group" "loadgen" {
  name        = "${local.name}-host"
  description = "rtmpack load generator. Egress only, no ingress."
  vpc_id      = var.vpc_id

  egress {
    description      = "All outbound traffic (MSK, S3, Secrets Manager, SSM)"
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }

  tags = merge(var.tags, { Name = local.name })
}

data "aws_iam_policy_document" "assume_ec2" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "loadgen" {
  name               = local.name
  assume_role_policy = data.aws_iam_policy_document.assume_ec2.json
  tags               = var.tags
}

resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.loadgen.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

data "aws_iam_policy_document" "loadgen" {
  statement {
    sid       = "ReadMskSecret"
    actions   = ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"]
    resources = [var.msk_secret_arn]
  }

  dynamic "statement" {
    for_each = var.kms_key_arn == null ? [] : [var.kms_key_arn]

    content {
      sid       = "DecryptMskSecret"
      actions   = ["kms:Decrypt"]
      resources = [statement.value]
    }
  }

  statement {
    sid       = "ReadHarnessArtifact"
    actions   = ["s3:GetObject"]
    resources = ["arn:aws:s3:::${var.artifact_bucket}/${var.artifact_prefix}/*"]
  }

  statement {
    sid       = "WriteResults"
    actions   = ["s3:PutObject"]
    resources = ["arn:aws:s3:::${var.artifact_bucket}/${var.results_prefix}/*"]
  }

  statement {
    sid       = "ListHarnessPrefix"
    actions   = ["s3:ListBucket"]
    resources = ["arn:aws:s3:::${var.artifact_bucket}"]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = [var.artifact_prefix, "${var.artifact_prefix}/*"]
    }
  }
}

resource "aws_iam_role_policy" "loadgen" {
  name   = "${local.name}-access"
  role   = aws_iam_role.loadgen.id
  policy = data.aws_iam_policy_document.loadgen.json
}

resource "aws_iam_instance_profile" "loadgen" {
  name = local.name
  role = aws_iam_role.loadgen.name
  tags = var.tags
}

resource "aws_instance" "loadgen" {
  ami                         = data.aws_ami.al2023.id
  instance_type               = var.instance_type
  subnet_id                   = var.subnet_id
  vpc_security_group_ids      = concat([aws_security_group.loadgen.id], var.extra_security_group_ids)
  iam_instance_profile        = aws_iam_instance_profile.loadgen.name
  associate_public_ip_address = false

  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 1
  }

  root_block_device {
    volume_type = "gp3"
    volume_size = 30
    encrypted   = true
  }

  user_data = templatefile("${path.module}/user_data.sh.tftpl", {
    region         = data.aws_region.current.region
    harness_s3_uri = local.harness_uri
    msk_secret_arn = var.msk_secret_arn
  })
  # The host is long-lived; harness updates are installed in place, not by replacement.
  user_data_replace_on_change = false

  tags = merge(var.tags, { Name = local.name })

  lifecycle {
    ignore_changes = [user_data, ami]
  }

  depends_on = [
    aws_iam_role_policy.loadgen,
    aws_iam_role_policy_attachment.ssm,
    aws_s3_object.harness_bundle,
  ]
}

# Running or stopped (never terminated). Benchmark scripts start the host for a
# session and stop it afterwards, so it costs nothing between sessions.
resource "aws_ec2_instance_state" "loadgen" {
  instance_id = aws_instance.loadgen.id
  state       = var.instance_running ? "running" : "stopped"
}
