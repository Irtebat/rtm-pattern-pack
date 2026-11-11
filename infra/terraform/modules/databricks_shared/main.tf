resource "databricks_secret_scope" "rtmpack" {
  name = var.secret_scope_name
}

resource "databricks_secret" "kafka_bootstrap" {
  scope        = databricks_secret_scope.rtmpack.name
  key          = "kafka-bootstrap"
  string_value = var.kafka_bootstrap
}

resource "databricks_secret" "kafka_username" {
  scope        = databricks_secret_scope.rtmpack.name
  key          = "kafka-username"
  string_value = var.kafka_username
}

resource "databricks_secret" "kafka_password" {
  scope        = databricks_secret_scope.rtmpack.name
  key          = "kafka-password"
  string_value = var.kafka_password
}
