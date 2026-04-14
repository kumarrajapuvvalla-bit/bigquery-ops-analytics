{{/*
  stg_flight_routes
  ─────────────────
  Normalises raw flight route records.
  One row = one flight leg (departure event).

  Source: ops_raw.flight_routes
  Grain: flight_id + event_date
*/}}
{{ config(materialized='view') }}

with

source as (
    select * from {{ source('ops_raw', 'flight_routes') }}
),

renamed as (
    select
        cast(flight_id     as string)    as flight_id,
        cast(route_code    as string)    as route_code,
        upper(cast(origin  as string))   as origin_iata,
        upper(cast(dest    as string))   as dest_iata,
        cast(airline       as string)    as airline,
        cast(aircraft_type as string)    as aircraft_type,
        cast(cabin_class   as string)    as cabin_class,

        cast(event_type as string)       as event_type,
        cast(priority   as string)       as priority,

        -- Schedule vs actuals
        cast(scheduled_departure_ts as timestamp)   as scheduled_departure_ts,
        cast(actual_departure_ts    as timestamp)   as actual_departure_ts,
        cast(actual_arrival_ts      as timestamp)   as actual_arrival_ts,

        -- Derived delay metrics
        timestamp_diff(
            cast(actual_departure_ts as timestamp),
            cast(scheduled_departure_ts as timestamp),
            minute
        )                                            as departure_delay_minutes,

        case
            when timestamp_diff(
                cast(actual_departure_ts as timestamp),
                cast(scheduled_departure_ts as timestamp),
                minute
            ) <= 15 then true
            else false
        end                                          as is_on_time,

        cast(is_cancelled  as bool)      as is_cancelled,
        cast(is_diverted   as bool)      as is_diverted,

        date(cast(scheduled_departure_ts as timestamp)) as event_date,
        current_timestamp()                             as _dbt_loaded_at
    from source
    where flight_id is not null
)

select * from renamed
