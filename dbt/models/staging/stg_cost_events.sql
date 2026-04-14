{{/*
  stg_cost_events
  ───────────────
  Normalises AWS Cost Explorer / GCP Billing export events.
  One row = one billable line item.

  Source: ops_raw.cost_events
  Grain: cost_event_id
*/}}
{{ config(materialized='view') }}

with

source as (
    select * from {{ source('ops_raw', 'cost_events') }}
),

renamed as (
    select
        cast(cost_event_id   as string)    as cost_event_id,
        cast(team            as string)    as team,
        cast(service_name    as string)    as service_name,
        cast(environment     as string)    as environment,
        cast(cost_category   as string)    as cost_category,  -- compute | storage | network | managed
        cast(cloud_provider  as string)    as cloud_provider,
        cast(aws_region      as string)    as aws_region,
        cast(resource_id     as string)    as resource_id,

        safe_cast(cost_usd      as float64)   as cost_usd,
        safe_cast(usage_amount  as float64)   as usage_amount,
        cast(usage_unit     as string)        as usage_unit,

        cast(cost_date as date)               as cost_date,
        current_timestamp()                   as _dbt_loaded_at
    from source
    where cost_event_id is not null
      and cost_date      is not null
      and cost_usd       is not null
)

select * from renamed
