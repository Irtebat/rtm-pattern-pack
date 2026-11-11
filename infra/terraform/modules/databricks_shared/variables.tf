variable "secret_scope_name" {
  description = "Databricks secret scope that holds the MSK connection settings (contract section 1)."
  type        = string
  default     = "rtmpack"
}

variable "kafka_bootstrap" {
  description = "MSK SASL/SCRAM bootstrap servers, private port 9096 (contract section 1). Stored as secret kafka-bootstrap."
  type        = string
}

variable "kafka_username" {
  description = "MSK SCRAM username (from AWS Secrets Manager AmazonMSK_rtmpack). Stored as secret kafka-username."
  type        = string
}

variable "kafka_password" {
  description = "MSK SCRAM password (from AWS Secrets Manager AmazonMSK_rtmpack). Stored as secret kafka-password."
  type        = string
  sensitive   = true
}
