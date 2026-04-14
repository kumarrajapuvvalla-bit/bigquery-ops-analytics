{{/*
  flight_ops_summary
  ──────────────────
  Daily flight operations summary mart.
  Grain: route_code × airline × event_date

  Powers the Flight Ops Looker dashboard.
*/}}
{{ config(
    materialized = 'table',
    partition_by = {"field": "event_date", "data_type": "date"},
    cluster_by   = ["route_code", "airline"]
) }}

with

route_perf as (
    select * from {{ ref('int_route_performance') }}
),

with_trend as (
    select
        *,
        round(
            avg(otp_rate) over (
                partition by route_code, airline
                order by event_date
                rows between 6 preceding and current row
            ), 4
        )                                     as rolling_7d_otp_rate,

        round(
            avg(avg_delay_minutes) over (
                partition by route_code, airline
                order by event_date
                rows between 6 preceding and current row
            ), 2
        )                                     as rolling_7d_avg_delay,

        -- Week-over-week OTP change
        otp_rate - lag(otp_rate, 7) over (
            partition by route_code, airline
            order by event_date
        )                                     as wow_otp_delta,

        current_timestamp()                   as _dbt_loaded_at
    from route_perf
)

select * from with_trend
