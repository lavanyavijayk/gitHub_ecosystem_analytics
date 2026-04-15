# ================================================================
# output.tf  —  GCP Migration
# ================================================================

output "raw_bucket" {
  description = "GCS raw bucket name — upload GH Archive files here."
  value       = google_storage_bucket.raw.name
}

output "silver_bucket" {
  description = "GCS silver bucket — Delta tables written here by silver_layer job."
  value       = google_storage_bucket.silver.name
}

output "gold_bucket" {
  description = "GCS gold bucket — Delta tables written here by gold_layer job."
  value       = google_storage_bucket.gold.name
}

output "scripts_bucket" {
  description = "GCS scripts bucket — Dataproc PySpark scripts stored here."
  value       = google_storage_bucket.scripts.name
}

output "dataproc_sa_email" {
  description = "Dataproc service account email."
  value       = google_service_account.dataproc_sa.email
}

output "workflows_sa_email" {
  description = "Cloud Workflows service account email."
  value       = google_service_account.workflows_sa.email
}

output "bigquery_dataset" {
  description = "BigQuery dataset for gold tables — connect Power BI here."
  value       = "${var.project_id}.${google_bigquery_dataset.gold.dataset_id}"
}

output "batch_workflow" {
  description = "Cloud Workflow that runs the daily batch pipeline (silver/api/gold/BQ/ml_score)."
  value       = google_workflows_workflow.batch.name
}

output "monthly_workflow" {
  description = "Cloud Workflow that runs monthly ML training + scoring."
  value       = google_workflows_workflow.monthly.name
}

output "pubsub_topic" {
  description = "Pub/Sub topic that receives GCS raw upload notifications."
  value       = google_pubsub_topic.raw_uploads.name
}

output "pubsub_subscription" {
  description = "Pub/Sub subscription the batch workflow pulls from."
  value       = google_pubsub_subscription.raw_uploads_sub.name
}

output "upload_command" {
  description = "Command to upload data files to GCS raw bucket."
  value       = "python scripts/upload_to_gcs.py --bucket ${google_storage_bucket.raw.name}"
}
