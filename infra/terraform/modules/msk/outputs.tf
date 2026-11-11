output "cluster_arn" {
  value = aws_msk_cluster.this.arn
}

output "bootstrap_brokers_sasl_scram" {
  description = "Private SASL/SCRAM TLS bootstrap string (port 9096)."
  value       = local.bootstrap_sasl_scram
}

output "scram_secret_arn" {
  value = aws_secretsmanager_secret.scram.arn
}

output "scram_kms_key_arn" {
  value = aws_kms_key.msk_scram.arn
}

output "scram_username" {
  value = var.scram_username
}

output "scram_password" {
  value     = random_password.scram.result
  sensitive = true
}
