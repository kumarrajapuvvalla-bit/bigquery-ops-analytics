-- models/staging/stg_fleet_events.sql
-- Staging model: cleanse and cast raw fleet events
-- Materialised as VIEW — zero storage cost, re-reads raw on demand

{{ config(
    materialized='view',
    schema='ops_staging',
    tags=['staging', 'fleet']
) }}

WITH source AS (
    SELECT * FROM {{ source('ops_raw', 'fleet_events') }}
    WHERE event_date BETWEEN
        DATE_SUB(CURRENT_DATE(), INTERVAL {{ var('lookback_days', 90) }} DAY)
        AND CURRENT_DATE()
),

cleansed AS (
    SELECT
        event_key,
        LOWER(TRIM(source_system))                      AS source_system,
        LOWER(TRIM(environment))                        AS environment,
        COALESCE(cluster_name, 'unknown')               AS cluster_name,
        COALESCE(region, 'unknown')                     AS region,
        event_id,
        UPPER(TRIM(event_type))                         AS event_type,
        UPPER(TRIM(COALESCE(event_subtype, 'UNKNOWN'))) AS event_subtype,
        LOWER(TRIM(COALESCE(service_name, 'unknown')))  AS service_name,
        event_timestamp,
        event_date,
        COALESCE(duration_ms, 0)                        AS duration_ms,
        UPPER(COALESCE(severity, 'UNKNOWN'))             AS severity,
        UPPER(COALESCE(status, 'UNKNOWN'))               AS status,
        COALESCE(message, '')                            AS message,
        payload_json,
        slo_name,
        COALESCE(error_budget_burn, 0.0)                AS error_budget_burn,
        COALESCE(cost_centre, 'unattributed')           AS cost_centre,
        COALESCE(squad, 'unattributed')                 AS squad,
        ingested_at,
        pipeline_version,
        source_file
    FROM source
    WHERE
        event_id IS NOT NULL
        AND event_timestamp IS NOT NULL
        AND event_type IN ('DEPLOY', 'SCALE', 'INCIDENT', 'ALERT', 'HEAL', 'ROLLBACK')
)

SELECT * FROM cleansed
