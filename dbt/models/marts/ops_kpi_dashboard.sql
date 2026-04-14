{{/*
  ops_kpi_dashboard
  ─────────────────
  Single denormalised KPI table for the Executive Ops Dashboard.
  Grain: event_date (one row per day, aggregated across all clusters/routes).

  Powers the top-level Looker Studio dashboard with key health, ops,
  cost and anomaly signals.
*/}}
{{ config(
    materialized = 'table',
    partition_by = {"field": "event_date", "data_type": "date"}
) }}

with

fleet as (
    select
        event_date,
        round(avg(avg_fleet_health_score), 2)  as fleet_health_score,
        round(avg(slo_compliance_rate), 4)     as fleet_slo_rate,
        sum(anomaly_count)                     as fleet_anomaly_count,
        sum(critical_anomaly_count)            as critical_anomaly_count
    from {{ ref('fleet_readiness_daily') }}
    group by 1
),

flight as (
    select
        event_date,
        round(avg(otp_rate), 4)                as avg_otp_rate,
        round(avg(avg_delay_minutes), 2)       as avg_delay_minutes,
        round(avg(cancellation_rate), 4)       as avg_cancellation_rate,
        sum(total_flights)                     as total_flights
    from {{ ref('flight_ops_summary') }}
    group by 1
),

cost as (
    select
        cost_date                              as event_date,
        round(sum(total_cost_usd), 2)          as total_cost_usd,
        countif(is_cost_spike)                 as cost_spike_count
    from {{ ref('infra_cost_by_team') }}
    group by 1
)

select
    coalesce(f.event_date, fl.event_date, c.event_date) as event_date,

    coalesce(f.fleet_health_score, 0)    as fleet_health_score,
    coalesce(f.fleet_slo_rate, 0)        as fleet_slo_rate,
    coalesce(f.fleet_anomaly_count, 0)   as fleet_anomaly_count,
    coalesce(f.critical_anomaly_count, 0) as critical_anomaly_count,

    coalesce(fl.avg_otp_rate, 0)          as avg_otp_rate,
    coalesce(fl.avg_delay_minutes, 0)     as avg_delay_minutes,
    coalesce(fl.avg_cancellation_rate, 0) as avg_cancellation_rate,
    coalesce(fl.total_flights, 0)         as total_flights,

    coalesce(c.total_cost_usd, 0)         as total_cost_usd,
    coalesce(c.cost_spike_count, 0)       as cost_spike_count,

    current_timestamp()                   as _dbt_loaded_at
from fleet     f
full outer join flight fl on f.event_date = fl.event_date
full outer join cost   c  on coalesce(f.event_date, fl.event_date) = c.event_date
order by event_date desc
