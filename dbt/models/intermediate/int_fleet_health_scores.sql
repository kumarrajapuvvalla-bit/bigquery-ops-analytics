{{/*
  int_fleet_health_scores
  ───────────────────────
  Computes a weighted Fleet Readiness Score per cluster per hour.
  Mirrors the HealthCalculator logic from ops-platform in SQL.

  Weights:
    availability  50% — are nodes/tasks up?
    latency       30% — is p99 latency within SLO?
    error_rate    20% — are errors below threshold?

  Output grain: cluster_name × environment × event_hour
*/}}
{{ config(
    materialized = 'table',
    partition_by = {"field": "event_date", "data_type": "date"},
    cluster_by   = ["cluster_name", "environment"]
) }}

with

fleet as (
    select * from {{ ref('stg_fleet_events') }}
),

metrics as (
    select * from {{ ref('stg_infra_metrics') }}
),

hourly_fleet as (
    select
        cluster_name,
        environment,
        event_hour,
        event_date,

        -- Availability score (0–100)
        avg(node_health_ratio  * 100)                       as avg_node_health,
        avg(task_running_ratio * 100)                       as avg_task_health,
        avg((node_health_ratio + task_running_ratio) / 2 * 100) as availability_score,

        avg(readiness_score)                                as avg_readiness_score,
        count(*)                                            as event_count
    from fleet
    group by 1, 2, 3, 4
),

hourly_metrics as (
    select
        cluster_name,
        environment,
        metric_hour,
        date(metric_hour)                                   as metric_date,

        avg(latency_p99_ms)                                 as avg_latency_p99_ms,
        avg(error_rate)                                     as avg_error_rate,

        -- Latency score: 100 at SLO (500ms), degrades linearly to 0 at 2× SLO
        greatest(0,
            100.0 - greatest(0, avg(latency_p99_ms) - 500) / 500 * 100
        )                                                   as latency_score,

        -- Error rate score: 100 at 0%, degrades linearly above 5%
        greatest(0,
            100.0 - greatest(0, avg(error_rate) - 0.05) / 0.95 * 100
        )                                                   as error_rate_score
    from metrics
    group by 1, 2, 3, 4
),

joined as (
    select
        f.cluster_name,
        f.environment,
        f.event_hour,
        f.event_date,
        f.availability_score,
        f.avg_readiness_score,
        f.event_count,
        coalesce(m.latency_score,    100)  as latency_score,
        coalesce(m.error_rate_score, 100)  as error_rate_score,
        coalesce(m.avg_latency_p99_ms,  0) as avg_latency_p99_ms,
        coalesce(m.avg_error_rate,      0) as avg_error_rate
    from hourly_fleet f
    left join hourly_metrics m
        on  f.cluster_name = m.cluster_name
        and f.environment  = m.environment
        and f.event_hour   = m.metric_hour
),

scored as (
    select
        *,
        -- Weighted composite score
        round(
            0.50 * availability_score
          + 0.30 * latency_score
          + 0.20 * error_rate_score,
            2
        )                                           as fleet_health_score,

        -- SLO compliance flag
        (0.50 * availability_score
       + 0.30 * latency_score
       + 0.20 * error_rate_score) >= {{ var('slo_readiness_threshold') }}
                                                    as is_slo_compliant,

        current_timestamp()                         as _dbt_loaded_at
    from joined
)

select * from scored
