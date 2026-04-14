{{/*
  stg_infra_metrics
  ─────────────────
  Cleans EKS/ECS/ALB/RDS infrastructure utilisation metrics.
  One row = one metric observation (scrape tick from CloudWatch / Prometheus).

  Source: ops_raw.infra_metrics
  Grain: metric_id
*/}}
{{ config(materialized='view') }}

with

source as (
    select * from {{ source('ops_raw', 'infra_metrics') }}
),

renamed as (
    select
        cast(metric_id       as string)    as metric_id,
        cast(service_name    as string)    as service_name,
        cast(cluster_name    as string)    as cluster_name,
        cast(namespace       as string)    as namespace,
        cast(environment     as string)    as environment,
        cast(metric_name     as string)    as metric_name,
        cast(metric_source   as string)    as metric_source,  -- prometheus | cloudwatch

        safe_cast(metric_value     as float64)  as metric_value,
        safe_cast(cpu_utilisation  as float64)  as cpu_utilisation,
        safe_cast(mem_utilisation  as float64)  as mem_utilisation,
        safe_cast(request_count    as int64)    as request_count,
        safe_cast(error_rate       as float64)  as error_rate,
        safe_cast(latency_p99_ms   as float64)  as latency_p99_ms,

        cast(scraped_ts as timestamp)           as scraped_ts,
        date(cast(scraped_ts as timestamp))     as metric_date,
        timestamp_trunc(
            cast(scraped_ts as timestamp), hour
        )                                       as metric_hour,

        current_timestamp()                     as _dbt_loaded_at
    from source
    where metric_id  is not null
      and metric_name is not null
)

select * from renamed
