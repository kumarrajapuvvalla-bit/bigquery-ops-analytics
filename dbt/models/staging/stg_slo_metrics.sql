-- models/staging/stg_slo_metrics.sql
-- Staging model: SLO snapshot time-series

{{ config(
    materialized='view',
    schema='ops_staging',
    tags=['staging', 'slo']
) }}

WITH source AS (
    SELECT * FROM {{ source('ops_raw', 'slo_metrics') }}
    WHERE snapshot_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {{ var('lookback_days', 90) }} DAY)
),

enriched AS (
    SELECT
        metric_key,
        LOWER(TRIM(service_name))   AS service_name,
        LOWER(TRIM(environment))    AS environment,
        slo_name,
        slo_target,
        slo_window_days,
        COALESCE(good_events, 0)    AS good_events,
        COALESCE(total_events, 0)   AS total_events,
        COALESCE(error_rate, 0.0)   AS error_rate,

        -- Recalculate availability defensively
        CASE
            WHEN COALESCE(total_events, 0) = 0 THEN NULL
            ELSE SAFE_DIVIDE(COALESCE(good_events, 0), COALESCE(total_events, 0))
        END                         AS availability,

        COALESCE(error_budget_remaining, 1.0)   AS error_budget_remaining,
        COALESCE(burn_rate_1h,  0.0)            AS burn_rate_1h,
        COALESCE(burn_rate_6h,  0.0)            AS burn_rate_6h,
        COALESCE(burn_rate_24h, 0.0)            AS burn_rate_24h,
        COALESCE(burn_rate_72h, 0.0)            AS burn_rate_72h,
        is_breaching,
        alert_tier,
        snapshot_timestamp,
        snapshot_date,
        ingested_at,
        source_system
    FROM source
    WHERE service_name IS NOT NULL
)

SELECT * FROM enriched
