{{/*
  stg_fleet_events
  ────────────────
  Cleans and type-casts raw fleet telemetry events from ops_raw.fleet_events.
  One row = one fleet event (node group health tick, ECS task change, ALB health change).

  Source: ops_raw.fleet_events
  Grain: event_id (unique per event)
  Materialization: view (re-computed on each query)
*/}}
{{ config(materialized='view') }}

with

source as (
    select * from {{ source('ops_raw', 'fleet_events') }}
),

renamed as (
    select
        -- Keys
        cast(event_id         as string)    as event_id,
        cast(cluster_name     as string)    as cluster_name,
        cast(node_group       as string)    as node_group,
        cast(environment      as string)    as environment,

        -- Dimensions
        cast(event_type       as string)    as event_type,
        cast(service_name     as string)    as service_name,
        cast(aws_region       as string)    as aws_region,

        -- Metrics
        safe_cast(readiness_score as float64)   as readiness_score,
        safe_cast(node_count      as int64)     as node_count,
        safe_cast(healthy_nodes   as int64)     as healthy_nodes,
        safe_cast(desired_tasks   as int64)     as desired_tasks,
        safe_cast(running_tasks   as int64)     as running_tasks,

        -- Derived
        safe_divide(
            safe_cast(healthy_nodes as float64),
            nullif(safe_cast(node_count as float64), 0)
        ) as node_health_ratio,

        safe_divide(
            safe_cast(running_tasks as float64),
            nullif(safe_cast(desired_tasks as float64), 0)
        ) as task_running_ratio,

        -- Timestamps
        cast(event_ts as timestamp)                  as event_ts,
        date(cast(event_ts as timestamp))            as event_date,
        timestamp_trunc(
            cast(event_ts as timestamp), hour
        )                                            as event_hour,

        -- Audit
        current_timestamp()                          as _dbt_loaded_at
    from source
    where event_id is not null
      and event_ts  is not null
)

select * from renamed
