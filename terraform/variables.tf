variable "gcp_project_id" {
  description = "GCP project ID"
  type        = string
}

variable "gcp_region" {
  description = "GCP compute region"
  type        = string
  default     = "europe-west2"
}

variable "bq_location" {
  description = "BigQuery dataset multi-region location"
  type        = string
  default     = "EU"
}

variable "environment" {
  description = "Deployment environment: dev | staging | prod"
  type        = string
  default     = "dev"
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be dev, staging, or prod."
  }
}

variable "admin_sa_email" {
  description = "Admin service account email (OWNER on all datasets)"
  type        = string
}

variable "looker_sa_email" {
  description = "Looker Studio service account email"
  type        = string
}

variable "composer_sa_email" {
  description = "Cloud Composer service account email"
  type        = string
}

variable "dbt_sa_email" {
  description = "dbt service account email"
  type        = string
}

variable "notification_channel_ids" {
  description = "Cloud Monitoring notification channel IDs for alerts"
  type        = list(string)
  default     = []
}
