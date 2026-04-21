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

# ── Second GitHub PAT (round-robin for faster API ingestion) ──
resource "google_secret_manager_secret" "github_pat_2" {
  secret_id = "github-pat-2"

  replication {
    auto {}
  }

  labels     = var.labels
  depends_on = [google_project_service.apis]
}

resource "google_secret_manager_secret_version" "github_pat_2" {
  secret      = google_secret_manager_secret.github_pat_2.id
  secret_data = var.github_pat_2
}

resource "google_secret_manager_secret_iam_member" "dataproc_github_pat_2" {
  secret_id = google_secret_manager_secret.github_pat_2.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.dataproc_sa.email}"
}

# ── Third GitHub PAT ─────────────────────────────────────────
resource "google_secret_manager_secret" "github_pat_3" {
  secret_id = "github-pat-3"

  replication {
    auto {}
  }

  labels     = var.labels
  depends_on = [google_project_service.apis]
}

resource "google_secret_manager_secret_version" "github_pat_3" {
  secret      = google_secret_manager_secret.github_pat_3.id
  secret_data = var.github_pat_3
}

resource "google_secret_manager_secret_iam_member" "dataproc_github_pat_3" {
  secret_id = google_secret_manager_secret.github_pat_3.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.dataproc_sa.email}"
}

# ── Fourth GitHub PAT ────────────────────────────────────────
resource "google_secret_manager_secret" "github_pat_4" {
  secret_id = "github-pat-4"

  replication {
    auto {}
  }

  labels     = var.labels
  depends_on = [google_project_service.apis]
}

resource "google_secret_manager_secret_version" "github_pat_4" {
  secret      = google_secret_manager_secret.github_pat_4.id
  secret_data = var.github_pat_4
}

resource "google_secret_manager_secret_iam_member" "dataproc_github_pat_4" {
  secret_id = google_secret_manager_secret.github_pat_4.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.dataproc_sa.email}"
}
