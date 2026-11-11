output "instance_id" {
  description = "EC2 instance ID of the load generator. Use it with aws ssm start-session."
  value       = aws_instance.loadgen.id
}

output "security_group_id" {
  description = "ID of the load generator security group (egress only)."
  value       = aws_security_group.loadgen.id
}

output "role_arn" {
  description = "ARN of the IAM role attached to the load generator."
  value       = aws_iam_role.loadgen.arn
}

output "instance_profile_name" {
  description = "Name of the IAM instance profile attached to the load generator."
  value       = aws_iam_instance_profile.loadgen.name
}

output "harness_s3_uri" {
  description = "S3 URI of the uploaded rtmpack harness zip."
  value       = local.harness_uri
}
