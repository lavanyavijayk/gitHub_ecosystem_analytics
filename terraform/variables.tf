# ================================================================
# variables.tf  —  GCP Migration
# ================================================================

variable "project_id" {
  description = "GCP project ID."
  type        = string
}

variable "region" {
  description = "GCP region. us-central1 is cheapest for student credits."
  type        = string
  default     = "us-central1"
}

variable "prefix" {
  description = "Short lowercase prefix for all resource names. Must be globally unique for GCS buckets."
  type        = string
  default     = "ghoss"
  validation {
    condition     = length(var.prefix) <= 8 && can(regex("^[a-z0-9]+$", var.prefix))
    error_message = "Prefix must be lowercase alphanumeric, max 8 chars."
  }
}

variable "github_pat" {
  description = "GitHub Personal Access Token for API enrichment. Stored in Secret Manager."
  type        = string
  sensitive   = true
}

variable "bq_dataset" {
  description = "BigQuery dataset name for gold layer tables."
  type        = string
  default     = "github_oss_gold"
}

variable "alert_email" {
  description = "Email address for pipeline failure alerts."
  type        = string
  default     = ""
}

variable "labels" {
  description = "Labels applied to all resources."
  type        = map(string)
  default = {
    project     = "github-oss-analytics"
    course      = "damg7370"
    environment = "dev"
    managed_by  = "terraform"
  }
}
