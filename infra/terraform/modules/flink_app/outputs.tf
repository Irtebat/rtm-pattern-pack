output "application_name" {
  description = "Name of the Managed Service for Apache Flink application."
  value       = aws_kinesisanalyticsv2_application.flink.name
}

output "application_arn" {
  description = "ARN of the Managed Service for Apache Flink application."
  value       = aws_kinesisanalyticsv2_application.flink.arn
}

output "role_arn" {
  description = "ARN of the service execution role used by the application."
  value       = aws_iam_role.flink_exec.arn
}

output "log_group_name" {
  description = "Name of the CloudWatch Logs log group that receives application logs."
  value       = aws_cloudwatch_log_group.flink.name
}
