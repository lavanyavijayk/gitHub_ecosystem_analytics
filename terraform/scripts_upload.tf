# ================================================================
# scripts_upload.tf  (repurposed from databricks.tf)
#
# Uploads all Dataproc PySpark scripts and the cluster init script
# to the scripts GCS bucket so Dataproc jobs can reference them
# via gs://{prefix}-scripts/{script}.py
# ================================================================

locals {
  dataproc_scripts = [
    "silver_layer",
    "gold_layer",
    "github_api_ingestion",
    "ml_features",
    "ml_train",
    "ml_score",
    "ml_trajectory",
    "ml_language_trends",
  ]
}

resource "google_storage_bucket_object" "dataproc_scripts" {
  for_each = toset(local.dataproc_scripts)

  name   = "${each.value}.py"
  bucket = google_storage_bucket.scripts.name
  source = "${path.module}/../dataproc/${each.value}.py"

  depends_on = [google_storage_bucket.scripts]
}

resource "google_storage_bucket_object" "init_script" {
  name   = "init_cluster.sh"
  bucket = google_storage_bucket.scripts.name
  source = "${path.module}/../dataproc/init_cluster.sh"

  depends_on = [google_storage_bucket.scripts]
}
