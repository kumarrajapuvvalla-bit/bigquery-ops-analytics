-- models/staging/stg_cost_billing.sql
-- Staging model: GCP billing export

{{ config(
    materialized='view',
    schema='ops_staging',
    tags=['staging', 'cost']
) }}

WITH source AS (
    SELECT * FROM {{ source('ops_raw', 'cost_billing_export') }}
    WHERE usage_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {{ var('lookback_days', 90) }} DAY)
),

flattened AS (
    SELECT
        billing_account_id,
        (service).id             AS service_id,
        (service).description    AS service_description,
        (sku).id                 AS sku_id,
        (sku).description        AS sku_description,
        usage_start_time,
        usage_end_time,
        usage_date,
        (project).id             AS project_id,
        (project).name           AS project_name,

        -- Extract team label safely
        (SELECT value FROM UNNEST(labels) WHERE key = 'team' LIMIT 1)  AS team_label,
        -- Extract env label safely
        (SELECT value FROM UNNEST(labels) WHERE key = 'env' LIMIT 1)   AS env_label,

        (location).region        AS region,
        (location).country       AS country,
        COALESCE(cost, 0)        AS cost_usd,
        currency,
        COALESCE(currency_conversion_rate, 1.0) AS currency_conversion_rate,
        (usage).amount           AS usage_amount,
        (usage).unit             AS usage_unit,

        -- Total credits
        (SELECT COALESCE(SUM(c.amount), 0) FROM UNNEST(credits) AS c) AS total_credits_usd,

        cost_type,
        ingested_at,
        pipeline_run_id
    FROM source
)

SELECT * FROM flattened
