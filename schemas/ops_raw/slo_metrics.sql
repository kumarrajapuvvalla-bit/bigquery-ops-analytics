-- =============================================================================
-- schemas/ops_raw/slo_metrics.sql
-- SLO time-series snapshots from Prometheus / Cloud Monitoring
-- =============================================================================

CREATE TABLE IF NOT EXISTS `ops_raw.slo_metrics`
(
    metric_key          STRING      NOT NULL,
    service_name        STRING      NOT NULL,
    environment         STRING      NOT NULL,
    slo_name            STRING      NOT NULL,
    slo_target          FLOAT64     NOT NULL OPTIONS (description = 'Target ratio e.g. 0.999'),
    slo_window_days     INT64       NOT NULL,

    -- Observed values
    good_events         INT64,
    total_events        INT64,
    error_rate          FLOAT64,
    availability        FLOAT64,
    error_budget_remaining FLOAT64 OPTIONS (description = 'Remaining error budget as ratio'),
    burn_rate_1h        FLOAT64,
    burn_rate_6h        FLOAT64,
    burn_rate_24h       FLOAT64,
    burn_rate_72h       FLOAT64,

    -- Compliance flags
    is_breaching        BOOL,
    alert_tier          STRING      OPTIONS (description = 'P0|P1|P2 based on burn rate thresholds'),

    -- Timing
    snapshot_timestamp  TIMESTAMP   NOT NULL,
    snapshot_date       DATE        NOT NULL,

    -- Lineage
    ingested_at         TIMESTAMP   NOT NULL,
    source_system       STRING      NOT NULL
)
PARTITION BY snapshot_date
CLUSTER BY service_name, environment, slo_name
OPTIONS (
    description = 'SLO snapshot time-series. One row per (service, slo, snapshot_timestamp).',
    require_partition_filter = FALSE
);
