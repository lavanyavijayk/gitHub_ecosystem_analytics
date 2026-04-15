# ================================================================
# pubsub.tf  —  Pub/Sub topic + subscription + GCS notification
#
# GCS raw bucket publishes an event to the topic on every
# gharchive/ file upload.  The batch_pipeline workflow pulls
# from the subscription every 30 minutes; if the queue is empty
# it returns immediately without creating a Dataproc cluster.
# ================================================================

# ── Topic: receives GCS upload events ────────────────────────
resource "google_pubsub_topic" "raw_uploads" {
  name   = "${var.prefix}-raw-uploads"
  labels = var.labels

  depends_on = [google_project_service.apis]
}

# ── Dead Letter Topic: receives permanently failed messages ──────
resource "google_pubsub_topic" "raw_uploads_dlq" {
  name   = "${var.prefix}-raw-uploads-dlq"
  labels = var.labels

  depends_on = [google_project_service.apis]
}

resource "google_pubsub_subscription" "raw_uploads_dlq_sub" {
  name  = "${var.prefix}-raw-uploads-dlq-sub"
  topic = google_pubsub_topic.raw_uploads_dlq.id

  # Retain DLQ messages for 14 days for investigation.
  message_retention_duration = "1209600s"

  expiration_policy {
    ttl = ""
  }

  labels = var.labels
}

# ── Subscription: batch_pipeline workflow pulls from here ────
resource "google_pubsub_subscription" "raw_uploads_sub" {
  name  = "${var.prefix}-raw-uploads-sub"
  topic = google_pubsub_topic.raw_uploads.id

  # 10-minute ack deadline — messages are acked before cluster
  # creation so this is only relevant if the ack call itself fails.
  ack_deadline_seconds = 600

  # Retain undelivered messages for 7 days so missed windows
  # are caught on the next scheduler run.
  message_retention_duration = "604800s"

  # Never expire the subscription itself.
  expiration_policy {
    ttl = ""
  }

  # Route permanently failed messages to DLQ after 5 delivery attempts.
  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.raw_uploads_dlq.id
    max_delivery_attempts = 5
  }

  labels = var.labels
}

# ── Allow GCS service account to publish to the topic ────────
resource "google_pubsub_topic_iam_member" "gcs_publisher" {
  topic  = google_pubsub_topic.raw_uploads.id
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:${data.google_storage_project_service_account.gcs_sa.email_address}"
}

# ── GCS storage notification: gharchive/ uploads → Pub/Sub ──
resource "google_storage_notification" "raw_upload" {
  bucket             = google_storage_bucket.raw.name
  payload_format     = "JSON_API_V1"
  topic              = google_pubsub_topic.raw_uploads.id
  event_types        = ["OBJECT_FINALIZE"]
  object_name_prefix = "gharchive/"

  depends_on = [
    google_pubsub_topic_iam_member.gcs_publisher,
    google_storage_bucket.raw,
  ]
}
