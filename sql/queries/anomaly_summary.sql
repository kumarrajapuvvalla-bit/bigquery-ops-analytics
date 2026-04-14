-- anomaly_summary.sql
-- Daily anomaly event counts by severity, for the last 7 days.

SELECT
    event_date,
    anomaly_severity,
    COUNT(*) AS anomaly_count,
    ARRAY_AGG(DISTINCT cluster_name ORDER BY cluster_name LIMIT 5) AS affected_clusters
FROM `${GCP_PROJECT}.ops_analytics.anomaly_log`
WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY 1, 2
ORDER BY event_date DESC, anomaly_severity
;
