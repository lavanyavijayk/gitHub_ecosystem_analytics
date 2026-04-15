# ================================================================
# workflows.tf
#
# Provisions:
#   - Cloud Workflow: batch pipeline  (every 30 min via Scheduler)
#   - Cloud Workflow: monthly pipeline (1st of month, full refresh)
#   - Cloud Scheduler: 30-min trigger → batch workflow
#   - Cloud Scheduler: monthly trigger → monthly workflow
#
# File uploads go to Pub/Sub (via pubsub.tf GCS notification).
# The batch workflow pulls from the queue; if empty it exits
# immediately without creating a Dataproc cluster.
# ================================================================

# ── Batch Pipeline Workflow (daily) ───────────────────────────
resource "google_workflows_workflow" "batch" {
  name            = "${var.prefix}-batch-pipeline"
  region          = var.region
  service_account = google_service_account.workflows_sa.email
  description     = "Daily pipeline: pulls queued GCS uploads from Pub/Sub, runs silver/api/gold + BQ write + ml_score. Skips if queue is empty."

  source_contents = templatefile(
    "${path.module}/../workflows/batch_pipeline.yaml",
    {
      project_id          = var.project_id
      region              = var.region
      prefix              = var.prefix
      scripts_bucket      = "${var.prefix}-scripts"
      dataproc_sa         = google_service_account.dataproc_sa.email
      bq_dataset          = var.bq_dataset
      pubsub_subscription = "projects/${var.project_id}/subscriptions/${google_pubsub_subscription.raw_uploads_sub.name}"
    }
  )

  depends_on = [
    google_project_service.apis,
    google_service_account.workflows_sa,
    google_storage_bucket_object.dataproc_scripts,
    google_pubsub_subscription.raw_uploads_sub,
  ]
}

# ── Monthly Pipeline Workflow (ML training only) ─────────────
resource "google_workflows_workflow" "monthly" {
  name            = "${var.prefix}-monthly-pipeline"
  region          = var.region
  service_account = google_service_account.workflows_sa.email
  description     = "Monthly ML training: ml_train + ml_trajectory + ml_language_trends (parallel) then ml_score with fresh model."

  source_contents = templatefile(
    "${path.module}/../workflows/monthly_pipeline.yaml",
    {
      project_id     = var.project_id
      region         = var.region
      prefix         = var.prefix
      scripts_bucket = "${var.prefix}-scripts"
      dataproc_sa    = google_service_account.dataproc_sa.email
      bq_dataset     = var.bq_dataset
    }
  )

  depends_on = [
    google_project_service.apis,
    google_service_account.workflows_sa,
    google_storage_bucket_object.dataproc_scripts,
  ]
}

# ── Cloud Scheduler: daily → batch workflow ──────────────────
resource "google_cloud_scheduler_job" "batch" {
  name        = "${var.prefix}-batch-processor"
  region      = var.region
  description = "Daily at 03:00 UTC: drains Pub/Sub queue and runs full pipeline with BQ write."
  schedule    = "0 3 * * *"
  time_zone   = "UTC"

  http_target {
    uri         = "https://workflowexecutions.googleapis.com/v1/${google_workflows_workflow.batch.id}/executions"
    http_method = "POST"

    body = base64encode(jsonencode({
      argument = jsonencode({ source = "scheduler" })
    }))

    oauth_token {
      service_account_email = google_service_account.workflows_sa.email
    }
  }

  depends_on = [google_workflows_workflow.batch]
}

# ── Cloud Scheduler: monthly ML training ─────────────────────
resource "google_cloud_scheduler_job" "monthly" {
  name        = "${var.prefix}-monthly-full-refresh"
  region      = var.region
  description = "Triggers monthly ML training on the 1st at 02:00 UTC."
  schedule    = "0 2 1 * *"
  time_zone   = "UTC"

  http_target {
    uri         = "https://workflowexecutions.googleapis.com/v1/${google_workflows_workflow.monthly.id}/executions"
    http_method = "POST"

    body = base64encode(jsonencode({
      argument = jsonencode({
        run_mode = "full_refresh"
        source   = "scheduler"
      })
    }))

    oauth_token {
      service_account_email = google_service_account.workflows_sa.email
    }
  }

  depends_on = [google_workflows_workflow.monthly]
}
