# ================================================================
# secrets.tf  —  Secret Manager
#
# Stores the GitHub PAT so Dataproc scripts can retrieve it
# at runtime via the Secret Manager API (no credentials in code).
# ================================================================

resource "google_secret_manager_secret" "github_pat" {
  secret_id = "github-pat"

  replication {
    auto {}
  }

  labels     = var.labels
  depends_on = [google_project_service.apis]
}

resource "google_secret_manager_secret_version" "github_pat" {
  secret      = google_secret_manager_secret.github_pat.id
  secret_data = var.github_pat
}

# Grant the Dataproc SA access to read this secret
resource "google_secret_manager_secret_iam_member" "dataproc_github_pat" {
  secret_id = google_secret_manager_secret.github_pat.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.dataproc_sa.email}"
}
