# ================================================================
# monitoring.tf  —  Cloud Monitoring alerts for pipeline failures
#
# Sends email alerts when Cloud Workflows executions fail.
# Requires var.alert_email to be set in terraform.tfvars.
# ================================================================

# ── Enable Monitoring API ────────────────────────────────────────
resource "google_project_service" "monitoring" {
  service            = "monitoring.googleapis.com"
  disable_on_destroy = false
}

# ── Email notification channel ───────────────────────────────────
resource "google_monitoring_notification_channel" "email" {
  count        = var.alert_email != "" ? 1 : 0
  display_name = "${var.prefix} Pipeline Alerts"
  type         = "email"

  labels = {
    email_address = var.alert_email
  }

  depends_on = [google_project_service.monitoring]
}

# ── Alert: Cloud Workflow execution failures ─────────────────────
resource "google_monitoring_alert_policy" "workflow_failure" {
  count        = var.alert_email != "" ? 1 : 0
  display_name = "${var.prefix} Workflow Execution Failure"
  combiner     = "OR"

  conditions {
    display_name = "Workflow execution failed"
    condition_threshold {
      filter          = <<-EOT
        resource.type = "workflows.googleapis.com/Workflow"
        AND metric.type = "workflows.googleapis.com/finished_execution_count"
        AND metric.labels.status = "FAILED"
      EOT
      duration        = "0s"
      comparison      = "COMPARISON_GT"
      threshold_value = 0

      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_COUNT"
      }

      trigger {
        count = 1
      }
    }
  }

  notification_channels = [google_monitoring_notification_channel.email[0].id]

  alert_strategy {
    auto_close = "1800s"
  }

  depends_on = [google_project_service.monitoring]
}

# ── Alert: Dataproc job failures ─────────────────────────────────
resource "google_monitoring_alert_policy" "dataproc_job_failure" {
  count        = var.alert_email != "" ? 1 : 0
  display_name = "${var.prefix} Dataproc Job Failure"
  combiner     = "OR"

  conditions {
    display_name = "Dataproc job failed"
    condition_threshold {
      filter          = <<-EOT
        resource.type = "cloud_dataproc_cluster"
        AND metric.type = "dataproc.googleapis.com/cluster/job/completion_count"
        AND metric.labels.state = "ERROR"
      EOT
      duration        = "0s"
      comparison      = "COMPARISON_GT"
      threshold_value = 0

      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_COUNT"
      }

      trigger {
        count = 1
      }
    }
  }

  notification_channels = [google_monitoring_notification_channel.email[0].id]

  alert_strategy {
    auto_close = "1800s"
  }

  depends_on = [google_project_service.monitoring]
}
