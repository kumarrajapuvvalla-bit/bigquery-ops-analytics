# =============================================================================
# terraform/main.tf
# GCP infrastructure for bigquery-ops-analytics
# Provisions: BigQuery datasets, IAM, scheduled queries, Cloud Monitoring alerts
# =============================================================================

terraform {
  required_version = ">= 1.7"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
  backend "gcs" {
    bucket = "ops-analytics-tfstate"
    prefix = "bigquery-ops-analytics"
  }
}

provider "google" {
  project = var.gcp_project_id
  region  = var.gcp_region
}

# ── BigQuery Datasets ─────────────────────────────────────────────────────────

locals {
  datasets = {
    ops_raw     = { description = "Raw ingestion layer — append-only partitioned event tables" }
    ops_staging = { description = "dbt staging views — cleansed and cast raw data" }
    ops_marts   = { description = "Aggregated mart tables for dashboards and alerting" }
    ops_metrics = { description = "dbt semantic layer metric views" }
    ops_ml      = { description = "BigQuery ML models (ARIMA_PLUS, kmeans anomaly)" }
    ops_ci      = { description = "Ephemeral CI/CD datasets — auto-expires in 24h" }
  }
}

resource "google_bigquery_dataset" "ops" {
  for_each = local.datasets

  dataset_id    = each.key
  friendly_name = each.key
  description   = each.value.description
  location      = var.bq_location

  # CI ephemeral datasets expire in 24 hours
  default_table_expiration_ms = each.key == "ops_ci" ? 86400000 : null

  labels = {
    environment = var.environment
    managed_by  = "terraform"
    team        = "platform-data"
  }

  access {
    role          = "OWNER"
    user_by_email = var.admin_sa_email
  }
}

# ── IAM: Looker Studio SA — ops_marts viewer ──────────────────────────────────

resource "google_bigquery_dataset_iam_member" "looker_viewer" {
  dataset_id = google_bigquery_dataset.ops["ops_marts"].dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${var.looker_sa_email}"
}

resource "google_bigquery_dataset_iam_member" "looker_metrics_viewer" {
  dataset_id = google_bigquery_dataset.ops["ops_metrics"].dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${var.looker_sa_email}"
}

# ── IAM: Cloud Composer SA — editor on raw + staging + marts + ml ────────────

resource "google_bigquery_dataset_iam_member" "composer_editor" {
  for_each   = toset(["ops_raw", "ops_staging", "ops_marts", "ops_ml"])
  dataset_id = google_bigquery_dataset.ops[each.key].dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${var.composer_sa_email}"
}

# ── IAM: dbt SA — BigQuery Job User (project-level) ──────────────────────────

resource "google_project_iam_member" "dbt_job_user" {
  project = var.gcp_project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${var.dbt_sa_email}"
}

resource "google_bigquery_dataset_iam_member" "dbt_editor" {
  for_each   = toset(["ops_staging", "ops_marts", "ops_ml", "ops_ci"])
  dataset_id = google_bigquery_dataset.ops[each.key].dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${var.dbt_sa_email}"
}

resource "google_bigquery_dataset_iam_member" "dbt_raw_reader" {
  dataset_id = google_bigquery_dataset.ops["ops_raw"].dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${var.dbt_sa_email}"
}

# ── BigQuery Scheduled Query: anomaly detection (every 4h) ───────────────────

resource "google_bigquery_data_transfer_config" "fleet_anomaly_detection" {
  display_name           = "Fleet Anomaly Detection — Z-score (4h)"
  data_source_id         = "scheduled_query"
  schedule               = "every 4 hours"
  destination_dataset_id = google_bigquery_dataset.ops["ops_marts"].dataset_id
  location               = var.bq_location

  params = {
    query                  = file("${path.module}/../sql/queries/anomaly_summary.sql")
    destination_table_name = "fleet_anomaly_detections"
    write_disposition      = "WRITE_APPEND"
    create_disposition     = "CREATE_IF_NEEDED"
    partitioning_field     = ""
  }

  service_account_name = var.composer_sa_email
  depends_on           = [google_bigquery_dataset.ops]
}

# ── BigQuery Scheduled Query: data freshness check (every 15min) ─────────────

resource "google_bigquery_data_transfer_config" "freshness_check" {
  display_name           = "Data Freshness Audit (15min)"
  data_source_id         = "scheduled_query"
  schedule               = "every 15 minutes"
  destination_dataset_id = google_bigquery_dataset.ops["ops_marts"].dataset_id
  location               = var.bq_location

  params = {
    query                  = file("${path.module}/../sql/queries/freshness_check.sql")
    destination_table_name = "data_freshness_audit"
    write_disposition      = "WRITE_APPEND"
    create_disposition     = "CREATE_IF_NEEDED"
    partitioning_field     = ""
  }

  service_account_name = var.composer_sa_email
  depends_on           = [google_bigquery_dataset.ops]
}

# ── Cloud Monitoring alert policy: stale data ────────────────────────────────

resource "google_monitoring_alert_policy" "stale_data_alert" {
  display_name = "BigQuery Stale Data Alert"
  combiner     = "OR"

  conditions {
    display_name = "ops_raw.fleet_events stale > 2h"
    condition_threshold {
      filter          = "resource.type="bigquery_dataset" AND metric.type="bigquery.googleapis.com/storage/row_count""
      duration        = "7200s"
      comparison      = "COMPARISON_LT"
      threshold_value = 1
      aggregations {
        alignment_period   = "3600s"
        per_series_aligner = "ALIGN_MEAN"
      }
    }
  }

  notification_channels = var.notification_channel_ids
  severity              = "WARNING"
}
