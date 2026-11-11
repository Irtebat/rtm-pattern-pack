terraform {
  required_version = ">= 1.5.0"

  required_providers {
    databricks = {
      source = "databricks/databricks"
      # Pinned to the latest published release (1.141.0, checked 2026-11-08).
      version = "1.141.0"
    }
  }
}
