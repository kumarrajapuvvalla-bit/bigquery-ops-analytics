output "dataset_ids" {
  description = "Map of dataset name to BigQuery dataset ID"
  value       = { for k, v in google_bigquery_dataset.ops : k => v.dataset_id }
}

output "anomaly_detection_transfer_id" {
  description = "BigQuery transfer config ID for anomaly detection scheduled query"
  value       = google_bigquery_data_transfer_config.fleet_anomaly_detection.id
}

output "freshness_check_transfer_id" {
  description = "BigQuery transfer config ID for freshness check scheduled query"
  value       = google_bigquery_data_transfer_config.freshness_check.id
}
