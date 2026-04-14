{{/*
  fleet_readiness_daily
  ─────────────────────
  Daily fleet readiness mart — powers the Looker Fleet Health dashboard.
  Grain: cluster_name × environment × event_date

  Key metrics:
    - avg / min / max fleet health score
    - SLO compliance rate
    - Anomaly count
    - 7-day rolling average (trend)
*/}}
{{ config(
    materialized = 'table',
    partition_by = {"field": "event_date", "data_type": "date"},
    cluster_by   = ["cluster_name", "environment"]
) }}

with

hourly as (
    select * from {{ ref('int_fleet_health_scores') }}
),

anomalies as (
    select
        cluster_name,
        environment,
        event_date,
        countif(is_anomaly)              as anomaly_count,
        countif(anomaly_severity = 'critical') as critical_anomaly_count
    from {{ ref('int_anomaly_candidates') }}
    group by 1, 2, 3
),

daily as (
    select
        h.cluster_name,
        h.environment,
        h.event_date,

        round(avg(h.fleet_health_score), 2)   as avg_fleet_health_score,
        round(min(h.fleet_health_score), 2)   as min_fleet_health_score,
        round(max(h.fleet_health_score), 2)   as max_fleet_health_score,
        round(stddev(h.fleet_health_score), 2) as stddev_fleet_health_score,

        countif(h.is_slo_compliant)           as slo_compliant_hours,
        count(*)                              as total_hours,
        safe_divide(
            countif(h.is_slo_compliant),
            count(*)
        )                                     as slo_compliance_rate,

        round(avg(h.avg_latency_p99_ms), 2)   as avg_latency_p99_ms,
        round(avg(h.avg_error_rate), 4)        as avg_error_rate,
        sum(h.event_count)                    as total_events
    from hourly h
    group by 1, 2, 3
),

with_trend as (
    select
        d.*,
        round(
            avg(d.avg_fleet_health_score) over (
                partition by d.cluster_name, d.environment
                order by d.event_date
                rows between 6 preceding and current row
            ), 2
        )                                         as rolling_7d_avg_score,

        round(
            avg(d.slo_compliance_rate) over (
                partition by d.cluster_name, d.environment
                order by d.event_date
                rows between 29 preceding and current row
            ), 4
        )                                         as rolling_30d_slo_rate,

        coalesce(a.anomaly_count, 0)              as anomaly_count,
        coalesce(a.critical_anomaly_count, 0)     as critical_anomaly_count,

        current_timestamp()                       as _dbt_loaded_at
    from daily d
    left join anomalies a
        on  d.cluster_name = a.cluster_name
        and d.environment  = a.environment
        and d.event_date   = a.event_date
)

select * from with_trend
