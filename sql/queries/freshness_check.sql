-- sql/queries/freshness_check.sql
-- =============================================================================
-- Data freshness audit — runs every 15 minutes via BigQuery scheduled query
-- Flags tables where the latest partition is stale vs. their SLA thresholds
-- Results feed Cloud Monitoring custom metrics → PagerDuty / Slack alerts
-- =============================================================================

WITH sla_config AS (
  SELECT * FROM UNNEST([
    STRUCT('ops_raw'     AS dataset_id, 'fleet_events'        AS table_name, 2  AS sla_hours),
    STRUCT('ops_raw'     AS dataset_id, 'slo_metrics'         AS table_name, 2  AS sla_hours),
    STRUCT('ops_raw'     AS dataset_id, 'cost_billing_export' AS table_name, 25 AS sla_hours),
    STRUCT('ops_staging' AS dataset_id, 'stg_fleet_events'    AS table_name, 3  AS sla_hours),
    STRUCT('ops_staging' AS dataset_id, 'stg_slo_metrics'     AS table_name, 3  AS sla_hours),
    STRUCT('ops_marts'   AS dataset_id, 'fleet_health_daily'  AS table_name, 26 AS sla_hours),
    STRUCT('ops_marts'   AS dataset_id, 'slo_compliance'      AS table_name, 26 AS sla_hours),
    STRUCT('ops_marts'   AS dataset_id, 'cost_attribution'    AS table_name, 49 AS sla_hours)
  ])
),

partitions AS (
  SELECT
    table_schema AS dataset_id,
    table_name,
    MAX(last_modified_time)                     AS latest_modified,
    MAX(SAFE.PARSE_DATE('%Y%m%d', partition_id)) AS latest_partition_date,
    SUM(CAST(row_count AS INT64))               AS total_rows
  FROM `region-eu`.INFORMATION_SCHEMA.PARTITIONS
  WHERE table_schema IN ('ops_raw', 'ops_staging', 'ops_marts')
    AND partition_id NOT IN ('__NULL__', '__UNPARTITIONED__')
  GROUP BY 1, 2
)

SELECT
  p.dataset_id,
  p.table_name,
  p.latest_partition_date,
  p.latest_modified,
  p.total_rows,
  s.sla_hours,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), p.latest_modified, HOUR) AS hours_since_update,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), p.latest_modified, HOUR) > s.sla_hours AS is_stale,
  CASE
    WHEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), p.latest_modified, HOUR) > s.sla_hours * 3 THEN 'CRITICAL'
    WHEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), p.latest_modified, HOUR) > s.sla_hours     THEN 'WARNING'
    ELSE 'OK'
  END AS freshness_status,
  CURRENT_TIMESTAMP() AS checked_at
FROM partitions p
JOIN sla_config s USING (dataset_id, table_name)
ORDER BY is_stale DESC, hours_since_update DESC
