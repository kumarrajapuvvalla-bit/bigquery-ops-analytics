{{/*
  infra_cost_by_team
  ──────────────────
  Daily infrastructure cost allocation by team and environment.
  Grain: team × environment × cost_category × cost_date

  Supports FinOps chargeback reporting.
*/}}
{{ config(
    materialized = 'table',
    partition_by = {"field": "cost_date", "data_type": "date"},
    cluster_by   = ["team", "environment"]
) }}

with

costs as (
    select * from {{ ref('stg_cost_events') }}
),

daily_costs as (
    select
        team,
        environment,
        cost_category,
        cloud_provider,
        cost_date,

        sum(cost_usd)                                   as total_cost_usd,
        count(*)                                        as line_item_count,
        count(distinct service_name)                    as service_count,
        count(distinct resource_id)                     as resource_count
    from costs
    group by 1, 2, 3, 4, 5
),

with_trend as (
    select
        *,
        sum(total_cost_usd) over (
            partition by team, environment, cost_category
            order by cost_date
            rows between 6 preceding and current row
        )                                               as rolling_7d_cost_usd,

        avg(total_cost_usd) over (
            partition by team, environment, cost_category
            order by cost_date
            rows between 29 preceding and current row
        )                                               as rolling_30d_avg_daily_cost,

        -- Day-over-day cost change
        total_cost_usd - lag(total_cost_usd) over (
            partition by team, environment, cost_category
            order by cost_date
        )                                               as dod_cost_delta_usd,

        -- Flag >20% DoD spike
        safe_divide(
            total_cost_usd - lag(total_cost_usd) over (
                partition by team, environment, cost_category
                order by cost_date
            ),
            nullif(lag(total_cost_usd) over (
                partition by team, environment, cost_category
                order by cost_date
            ), 0)
        ) > 0.20                                        as is_cost_spike,

        current_timestamp()                             as _dbt_loaded_at
    from daily_costs
)

select * from with_trend
