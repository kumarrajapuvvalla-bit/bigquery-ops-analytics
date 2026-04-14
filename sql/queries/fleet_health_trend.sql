-- fleet_health_trend.sql
-- Ad-hoc query: 30-day fleet health score trend with 7-day rolling average.
-- Usage: Run in BigQuery console or via bq query --use_legacy_sql=false

SELECT
    event_date,
    cluster_name,
    environment,
    avg_fleet_health_score,
    rolling_7d_avg_score,
    slo_compliance_rate,
    anomaly_count,
    critical_anomaly_count
FROM `${GCP_PROJECT}.ops_analytics.fleet_readiness_daily`
WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  AND environment = 'prod'
ORDER BY event_date DESC, cluster_name
;
