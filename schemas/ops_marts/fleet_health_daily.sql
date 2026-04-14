-- =============================================================================
-- schemas/ops_marts/fleet_health_daily.sql
-- Aggregated mart: daily fleet health summary per service
-- Powers: Looker Studio Fleet Health dashboard
-- Updated: daily via dbt incremental model
-- =============================================================================

CREATE TABLE IF NOT EXISTS `ops_marts.fleet_health_daily`
(
    -- Surrogate key
    fleet_health_key    STRING      NOT NULL,

    -- Dimensions
    report_date         DATE        NOT NULL,
    service_name        STRING      NOT NULL,
    environment         STRING      NOT NULL,
    cluster_name        STRING,
    squad               STRING,
    cost_centre         STRING,

    -- Event counts
    total_events        INT64,
    deploy_count        INT64,
    incident_count      INT64,
    alert_count         INT64,
    heal_count          INT64,
    scale_count         INT64,

    -- Severity breakdown
    critical_events     INT64,
    high_events         INT64,
    medium_events       INT64,
    low_events          INT64,

    -- DORA metrics
    deployment_frequency         FLOAT64,
    change_failure_rate          FLOAT64,
    mean_time_to_recovery_mins   FLOAT64,
    lead_time_for_change_hours   FLOAT64,

    -- SLO summary
    p99_availability    FLOAT64,
    error_budget_remaining FLOAT64,
    slo_breach_count    INT64,

    -- Cost
    daily_cost_usd      NUMERIC,
    cost_per_deploy_usd NUMERIC,

    -- Anomaly flags (updated by downstream anomaly detection job)
    incident_anomaly    BOOL,
    cost_anomaly        BOOL,

    -- Lineage
    dbt_updated_at      TIMESTAMP   NOT NULL,
    dbt_run_id          STRING
)
PARTITION BY report_date
CLUSTER BY environment, service_name, squad
OPTIONS (
    description = 'Daily fleet health mart. Powers Looker Studio dashboards.'
);
