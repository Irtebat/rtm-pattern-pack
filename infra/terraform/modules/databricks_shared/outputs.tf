output "secret_scope_name" {
  description = "Name of the secret scope holding kafka-bootstrap, kafka-username and kafka-password."
  value       = databricks_secret_scope.rtmpack.name
}
