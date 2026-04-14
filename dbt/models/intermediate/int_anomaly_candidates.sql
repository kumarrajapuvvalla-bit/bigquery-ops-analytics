{{/*
  int_anomaly_candidates
  ──────────────────────
  Flags hourly fleet health score observations as anomalies using
  a rolling Z-score approach in BigQuery SQL.

  Anomaly criteria (OR):
    - Z-score |z| >= {{ var('anomaly_zscore_threshold') }} (default 3.0)
    - Value < Q1 - 1.5×IQR  or  > Q3 + 1.5×IQR

  Output grain: cluster_name × environment × event_hour
*/}}
{{ config(
    materialized = 'table',
    partition_by = {"field": "event_date", "data_type": "date"},
    cluster_by   = ["cluster_name", "environment"]
) }}

with

scores as (
    select * from {{ ref('int_fleet_health_scores') }}
),

-- Compute rolling statistics over the past {{ var('lookback_days') }} days
rolling_stats as (
    select
        *,
        avg(fleet_health_score) over (
            partition by cluster_name, environment
            order by event_hour
            rows between {{ var('lookback_days') * 24 }} preceding and 1 preceding
        )                                                   as rolling_mean,

        stddev_pop(fleet_health_score) over (
            partition by cluster_name, environment
            order by event_hour
            rows between {{ var('lookback_days') * 24 }} preceding and 1 preceding
        )                                                   as rolling_stddev,

        percentile_cont(fleet_health_score, 0.25) over (
            partition by cluster_name, environment
        )                                                   as q1,

        percentile_cont(fleet_health_score, 0.75) over (
            partition by cluster_name, environment
        )                                                   as q3
    from scores
),

with_zscore as (
    select
        *,
        safe_divide(
            fleet_health_score - rolling_mean,
            nullif(rolling_stddev, 0)
        )                                                   as z_score,

        (q3 - q1) * {{ var('anomaly_iqr_multiplier') }}     as iqr_range,
        q1 - (q3 - q1) * {{ var('anomaly_iqr_multiplier') }} as iqr_lower_bound,
        q3 + (q3 - q1) * {{ var('anomaly_iqr_multiplier') }} as iqr_upper_bound
    from rolling_stats
),

flagged as (
    select
        *,
        abs(z_score) >= {{ var('anomaly_zscore_threshold') }}   as is_zscore_anomaly,
        fleet_health_score < iqr_lower_bound
        or fleet_health_score > iqr_upper_bound                 as is_iqr_anomaly,

        abs(z_score) >= {{ var('anomaly_zscore_threshold') }}
        or fleet_health_score < iqr_lower_bound
        or fleet_health_score > iqr_upper_bound                 as is_anomaly,

        case
            when abs(z_score) >= 5 then 'critical'
            when abs(z_score) >= 4 then 'high'
            when abs(z_score) >= {{ var('anomaly_zscore_threshold') }} then 'medium'
            else 'low'
        end                                                     as anomaly_severity,

        current_timestamp()                                     as _dbt_loaded_at
    from with_zscore
)

select * from flagged
where is_anomaly = true
