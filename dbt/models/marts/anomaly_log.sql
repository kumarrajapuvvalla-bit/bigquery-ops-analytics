{{/*
  anomaly_log
  ───────────
  Persists all detected anomalies for audit, alerting and trend analysis.
  Grain: cluster_name × environment × event_hour (anomalies only)

  Used by:
    - PagerDuty alert DAG
    - Looker "Anomaly Events" tile
    - ops_kpi_dashboard rollup
*/}}
{{ config(
    materialized = 'table',
    partition_by = {"field": "event_date", "data_type": "date"},
    cluster_by   = ["cluster_name", "anomaly_severity"]
) }}

select
    cluster_name,
    environment,
    event_hour,
    event_date,
    fleet_health_score,
    rolling_mean,
    rolling_stddev,
    z_score,
    iqr_lower_bound,
    iqr_upper_bound,
    is_zscore_anomaly,
    is_iqr_anomaly,
    anomaly_severity,
    _dbt_loaded_at
from {{ ref('int_anomaly_candidates') }}
