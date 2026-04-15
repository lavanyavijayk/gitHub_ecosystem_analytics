# ================================================================
# iam.tf  —  Service Accounts and IAM bindings
#
# Provisions two service accounts:
#   dataproc_sa  — used by Dataproc cluster nodes
#   workflows_sa — used by Cloud Workflows to call Dataproc,
#                  Pub/Sub, and Cloud Scheduler
# ================================================================

# ── Dataproc cluster service account ─────────────────────────
resource "google_service_account" "dataproc_sa" {
  account_id   = "${var.prefix}-dataproc"
  display_name = "Dataproc cluster service account"
}

# Read/write GCS buckets (raw, silver, gold, scripts)
resource "google_project_iam_member" "dataproc_storage" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.dataproc_sa.email}"
}

# Access Secret Manager secrets (github-pat)
resource "google_project_iam_member" "dataproc_secret" {
  project = var.project_id
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${google_service_account.dataproc_sa.email}"
}

# Write to BigQuery
resource "google_project_iam_member" "dataproc_bq" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.dataproc_sa.email}"
}

resource "google_project_iam_member" "dataproc_bq_job" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.dataproc_sa.email}"
}

# Dataproc worker role (required to submit jobs internally)
resource "google_project_iam_member" "dataproc_worker" {
  project = var.project_id
  role    = "roles/dataproc.worker"
  member  = "serviceAccount:${google_service_account.dataproc_sa.email}"
}

# ── Cloud Workflows service account ──────────────────────────
resource "google_service_account" "workflows_sa" {
  account_id   = "${var.prefix}-workflows"
  display_name = "Cloud Workflows service account"
}

# Create and manage Dataproc clusters + submit jobs
resource "google_project_iam_member" "workflows_dataproc" {
  project = var.project_id
  role    = "roles/dataproc.editor"
  member  = "serviceAccount:${google_service_account.workflows_sa.email}"
}

# Invoke workflows (needed for Cloud Scheduler → Workflows trigger)
resource "google_project_iam_member" "workflows_invoker" {
  project = var.project_id
  role    = "roles/workflows.invoker"
  member  = "serviceAccount:${google_service_account.workflows_sa.email}"
}

# Pull messages from and acknowledge Pub/Sub subscription
resource "google_project_iam_member" "workflows_pubsub_subscriber" {
  project = var.project_id
  role    = "roles/pubsub.subscriber"
  member  = "serviceAccount:${google_service_account.workflows_sa.email}"
}

# Act as the Dataproc SA when creating clusters
resource "google_service_account_iam_member" "workflows_act_as_dataproc" {
  service_account_id = google_service_account.dataproc_sa.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.workflows_sa.email}"
}

# GCS service account — used in pubsub.tf to grant publish rights to the topic
data "google_storage_project_service_account" "gcs_sa" {}
