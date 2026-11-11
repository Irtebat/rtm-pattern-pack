output "job_id" {
  description = "ID of the RTM Databricks job."
  value       = databricks_job.this.id
}

output "job_url" {
  description = "URL of the RTM Databricks job in the workspace."
  value       = databricks_job.this.url
}

output "workspace_file_path" {
  description = "Workspace path of the uploaded script."
  value       = databricks_workspace_file.script.workspace_path
}

output "slots_required" {
  description = "Tasks that must run concurrently (tasks_required)."
  value       = var.tasks_required
}

output "slots_available" {
  description = "Task slots on the job cluster (num_workers * worker_vcpus)."
  value       = local.slots_available
}
