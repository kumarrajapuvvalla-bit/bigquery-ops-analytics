-- Custom dbt test: no CRITICAL anomalies older than 4h remain unresolved
SELECT
    service_name,
    event_type,
    z_score,
    detected_at
FROM {{ ref('fleet_anomaly_detections') }}
WHERE anomaly_severity = 'CRITICAL'
  AND detected_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 4 HOUR)
  AND resolved_at IS NULL
