# ================================================================
# bigquery.tf  (repurposed from synapse.tf)
#
# Provisions the BigQuery dataset for gold layer tables.
# Tables are written directly by the gold_layer.py Dataproc job
# using the Spark-BigQuery connector (pre-installed on Dataproc 2.1).
#
# Power BI connects to BigQuery via the native BigQuery connector:
#   Data Source: Google BigQuery
#   Project:     var.project_id
#   Dataset:     var.bq_dataset
# ================================================================

resource "google_bigquery_dataset" "gold" {
  dataset_id                  = var.bq_dataset
  friendly_name               = "GitHub OSS Gold Layer"
  description                 = "Star schema gold tables produced by the Dataproc gold_layer job."
  location                    = "US"
  delete_contents_on_destroy  = true

  labels = var.labels

  depends_on = [google_project_service.apis]
}

# Grant the Dataproc SA permission to write tables
resource "google_bigquery_dataset_iam_member" "dataproc_bq_editor" {
  dataset_id = google_bigquery_dataset.gold.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.dataproc_sa.email}"
}
