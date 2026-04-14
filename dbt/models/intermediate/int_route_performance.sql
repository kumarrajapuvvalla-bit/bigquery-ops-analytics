{{/*
  int_route_performance
  ─────────────────────
  Aggregates flight route performance metrics per route × day.
  Covers OTP (on-time performance), average delay, cancellation rate.

  Output grain: route_code × airline × event_date
*/}}
{{ config(
    materialized = 'table',
    partition_by = {"field": "event_date", "data_type": "date"},
    cluster_by   = ["route_code", "airline"]
) }}

with

routes as (
    select * from {{ ref('stg_flight_routes') }}
),

aggregated as (
    select
        route_code,
        origin_iata,
        dest_iata,
        airline,
        aircraft_type,
        event_date,

        count(*)                                                        as total_flights,
        countif(is_on_time)                                             as on_time_flights,
        countif(is_cancelled)                                           as cancelled_flights,
        countif(is_diverted)                                            as diverted_flights,

        safe_divide(countif(is_on_time), count(*))                      as otp_rate,
        safe_divide(countif(is_cancelled), count(*))                    as cancellation_rate,
        safe_divide(countif(is_diverted), count(*))                     as diversion_rate,

        avg(case when not is_cancelled then departure_delay_minutes end) as avg_delay_minutes,
        max(case when not is_cancelled then departure_delay_minutes end) as max_delay_minutes,
        percentile_cont(
            case when not is_cancelled then departure_delay_minutes end,
            0.9
        ) over (partition by route_code, airline, event_date)           as p90_delay_minutes,

        countif(priority = 'HIGH' or priority = 'CRITICAL')             as high_priority_count
    from routes
    group by 1, 2, 3, 4, 5, 6
),

final as (
    select
        *,
        -- SLO: OTP >= 85%
        otp_rate >= 0.85                              as meets_otp_slo,
        -- Classify route health
        case
            when otp_rate >= 0.90 then 'healthy'
            when otp_rate >= 0.80 then 'degraded'
            else 'critical'
        end                                           as route_health_status,
        current_timestamp()                           as _dbt_loaded_at
    from aggregated
)

select * from final
