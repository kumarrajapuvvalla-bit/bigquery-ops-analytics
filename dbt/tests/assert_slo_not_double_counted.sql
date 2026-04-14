-- Ensures each (service, slo_name, snapshot_timestamp) appears exactly once
SELECT
    service_name,
    slo_name,
    snapshot_timestamp,
    COUNT(*) AS cnt
FROM {{ ref('stg_slo_metrics') }}
GROUP BY 1, 2, 3
HAVING cnt > 1
