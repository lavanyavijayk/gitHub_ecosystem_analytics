# ================================================================
# main.tf  —  GCP Core Infrastructure
#
# Provisions: GCP APIs, GCS buckets (raw/silver/gold/scripts)
# ================================================================

terraform {
  required_version = ">= 1.6.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ── Enable required GCP APIs ─────────────────────────────────
resource "google_project_service" "apis" {
  for_each = toset([
    "dataproc.googleapis.com",
    "storage.googleapis.com",
    "bigquery.googleapis.com",
    "secretmanager.googleapis.com",
    "workflows.googleapis.com",
    "cloudscheduler.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "iam.googleapis.com",
    "pubsub.googleapis.com",
  ])
  service            = each.value
  disable_on_destroy = false
}

# ── GCS Data Lake Buckets ─────────────────────────────────────
resource "google_storage_bucket" "raw" {
  name                        = "${var.prefix}-raw"
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  force_destroy               = true

  # Transition to cheaper storage after 90 days, delete after 365 days.
  lifecycle_rule {
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
    condition {
      age = 90
    }
  }
  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 365
    }
  }

  labels     = var.labels
  depends_on = [google_project_service.apis]
}

resource "google_storage_bucket" "silver" {
  name                        = "${var.prefix}-silver"
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  force_destroy               = true
  labels                      = var.labels
  depends_on                  = [google_project_service.apis]
}

resource "google_storage_bucket" "gold" {
  name                        = "${var.prefix}-gold"
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  force_destroy               = true
  labels                      = var.labels
  depends_on                  = [google_project_service.apis]
}

# Scripts bucket — Dataproc PySpark scripts + init script
resource "google_storage_bucket" "scripts" {
  name                        = "${var.prefix}-scripts"
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  force_destroy               = true
  labels                      = var.labels
  depends_on                  = [google_project_service.apis]
}
