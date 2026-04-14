-- models/marts/fleet_health_daily.sql
-- Mart: Daily fleet health aggregated per service + environment
-- Powers the Fleet Health Looker Studio dashboard

{{ config(
    materialized='incremental',
    schema='ops_marts',
    unique_key='fleet_health_key',
    partition_by={
        'field': 'report_date',
        'data_type': 'date',
        'granularity': 'day'
    },
    cluster_by=['environment', 'service_name', 'squad'],
    tags=['mart', 'fleet', 'daily'],
    on_schema_change='append_new_columns'
) }}

WITH events AS (
    SELECT * FROM {{ ref('stg_fleet_events') }}
    {% if is_incremental() %}
    WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
    {% endif %}
),

slo AS (
    SELECT * FROM {{ ref('stg_slo_metrics') }}
    {% if is_incremental() %}
    WHERE snapshot_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
    {% endif %}
),

cost AS (
    SELECT * FROM {{ ref('stg_cost_billing') }}
    {% if is_incremental() %}
    WHERE usage_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
    {% endif %}
),

event_agg AS (
    SELECT
        event_date                                  AS report_date,
        service_name,
        environment,
        cluster_name,
        squad,
        cost_centre,
        COUNT(*)                                    AS total_events,
        COUNTIF(event_type = 'DEPLOY')              AS deploy_count,
        COUNTIF(event_type = 'INCIDENT')            AS incident_count,
        COUNTIF(event_type = 'ALERT')               AS alert_count,
        COUNTIF(event_type = 'HEAL')                AS heal_count,
        COUNTIF(event_type = 'SCALE')               AS scale_count,
        COUNTIF(severity = 'CRITICAL')              AS critical_events,
        COUNTIF(severity = 'HIGH')                  AS high_events,
        COUNTIF(severity = 'MEDIUM')                AS medium_events,
        COUNTIF(severity = 'LOW')                   AS low_events,
        SAFE_DIVIDE(COUNTIF(event_type = 'DEPLOY'), 1.0) AS deployment_frequency,
        SAFE_DIVIDE(
            COUNTIF(event_type = 'DEPLOY' AND status = 'FAILURE'),
            NULLIF(COUNTIF(event_type = 'DEPLOY'), 0)
        )                                           AS change_failure_rate,
        AVG(CASE WHEN event_type IN ('INCIDENT', 'HEAL') THEN duration_ms / 60000.0 END)
                                                    AS mean_time_to_recovery_mins
    FROM events
    GROUP BY 1, 2, 3, 4, 5, 6
),

slo_agg AS (
    SELECT
        snapshot_date                   AS report_date,
        service_name,
        environment,
        AVG(availability)               AS p99_availability,
        AVG(error_budget_remaining)     AS error_budget_remaining,
        COUNTIF(is_breaching)           AS slo_breach_count
    FROM slo
    GROUP BY 1, 2, 3
),

cost_agg AS (
    SELECT
        usage_date                          AS report_date,
        COALESCE(team_label, 'unattributed') AS squad,
        SUM(cost_usd + total_credits_usd)   AS daily_cost_usd
    FROM cost
    GROUP BY 1, 2
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['e.report_date', 'e.service_name', 'e.environment']) }}
                                        AS fleet_health_key,
        e.report_date,
        e.service_name,
        e.environment,
        e.cluster_name,
        e.squad,
        e.cost_centre,
        e.total_events,
        e.deploy_count,
        e.incident_count,
        e.alert_count,
        e.heal_count,
        e.scale_count,
        e.critical_events,
        e.high_events,
        e.medium_events,
        e.low_events,
        e.deployment_frequency,
        e.change_failure_rate,
        e.mean_time_to_recovery_mins,
        s.p99_availability,
        s.error_budget_remaining,
        s.slo_breach_count,
        COALESCE(c.daily_cost_usd, 0)   AS daily_cost_usd,
        SAFE_DIVIDE(COALESCE(c.daily_cost_usd, 0), NULLIF(e.deploy_count, 0))
                                        AS cost_per_deploy_usd,
        FALSE                           AS incident_anomaly,
        FALSE                           AS cost_anomaly,
        CURRENT_TIMESTAMP()             AS dbt_updated_at,
        '{{ invocation_id }}'           AS dbt_run_id
    FROM event_agg          e
    LEFT JOIN slo_agg       s USING (report_date, service_name, environment)
    LEFT JOIN cost_agg      c ON c.report_date = e.report_date AND c.squad = e.squad
)

SELECT * FROM final
