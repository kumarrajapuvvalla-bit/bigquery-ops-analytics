-- models/marts/cost_attribution.sql
-- Mart: Monthly cost attribution by team, project, and service

{{ config(
    materialized='table',
    schema='ops_marts',
    partition_by={'field': 'billing_month', 'data_type': 'date', 'granularity': 'month'},
    cluster_by=['team_label', 'project_id'],
    tags=['mart', 'cost', 'finops']
) }}

WITH billing AS (
    SELECT * FROM {{ ref('stg_cost_billing') }}
),

monthly AS (
    SELECT
        DATE_TRUNC(usage_date, MONTH)           AS billing_month,
        project_id,
        COALESCE(team_label, 'unattributed')    AS team_label,
        COALESCE(env_label, 'unknown')          AS env_label,
        service_description,
        sku_description,
        region,
        SUM(cost_usd)                           AS gross_cost_usd,
        SUM(total_credits_usd)                  AS total_credits_usd,
        SUM(cost_usd + total_credits_usd)       AS net_cost_usd,

        -- MoM cost change %
        SAFE_DIVIDE(
            SUM(cost_usd + total_credits_usd)
            - LAG(SUM(cost_usd + total_credits_usd)) OVER (
                PARTITION BY project_id, team_label, service_description
                ORDER BY DATE_TRUNC(usage_date, MONTH)
            ),
            NULLIF(LAG(SUM(cost_usd + total_credits_usd)) OVER (
                PARTITION BY project_id, team_label, service_description
                ORDER BY DATE_TRUNC(usage_date, MONTH)
            ), 0)
        )                                       AS mom_cost_change_pct,

        COUNT(DISTINCT usage_date)              AS billing_days
    FROM billing
    GROUP BY 1, 2, 3, 4, 5, 6, 7
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['billing_month', 'project_id', 'team_label', 'service_description']) }}
        AS cost_key,
    *,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM monthly
