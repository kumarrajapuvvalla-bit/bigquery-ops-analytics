-- models/marts/slo_compliance.sql
-- Mart: SLO compliance summary per service per calendar week

{{ config(
    materialized='incremental',
    schema='ops_marts',
    unique_key='slo_key',
    partition_by={'field': 'report_week', 'data_type': 'date', 'granularity': 'week'},
    cluster_by=['service_name', 'environment', 'slo_name'],
    tags=['mart', 'slo', 'reliability']
) }}

WITH slo AS (
    SELECT * FROM {{ ref('stg_slo_metrics') }}
    {% if is_incremental() %}
    WHERE snapshot_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
    {% endif %}
),

weekly AS (
    SELECT
        DATE_TRUNC(snapshot_date, WEEK)     AS report_week,
        service_name,
        environment,
        slo_name,
        slo_target,
        slo_window_days,
        AVG(availability)                   AS avg_availability,
        MIN(availability)                   AS min_availability,
        AVG(error_budget_remaining)         AS avg_error_budget_remaining,
        MIN(error_budget_remaining)         AS min_error_budget_remaining,
        AVG(burn_rate_1h)                   AS avg_burn_rate_1h,
        MAX(burn_rate_1h)                   AS peak_burn_rate_1h,
        AVG(burn_rate_6h)                   AS avg_burn_rate_6h,
        COUNTIF(is_breaching)               AS breach_count,
        COUNT(*)                            AS snapshot_count,
        SAFE_DIVIDE(COUNTIF(is_breaching), COUNT(*)) AS breach_pct,
        AVG(availability) >= slo_target     AS is_compliant,
        CASE
            WHEN AVG(burn_rate_1h) >= 14.4  THEN 'P0'
            WHEN AVG(burn_rate_6h) >= 6.0   THEN 'P1'
            WHEN AVG(burn_rate_24h) >= 3.0  THEN 'P2'
            ELSE 'OK'
        END                                 AS alert_tier
    FROM slo
    GROUP BY 1, 2, 3, 4, 5, 6
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['report_week', 'service_name', 'environment', 'slo_name']) }}
        AS slo_key,
    *,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM weekly
