-- =============================================================================
-- schemas/ops_raw/fleet_events.sql
-- BigQuery DDL: raw fleet events table
--
-- Partitioned by event_date (DATE) — keeps query cost proportional to lookback
-- Clustered by source_system + event_type — hot-path filters for anomaly scans
-- require_partition_filter = TRUE blocks accidental full table scans
-- =============================================================================

CREATE TABLE IF NOT EXISTS `ops_raw.fleet_events`
(
    -- Surrogate key: SHA-256 hash of (source_system, event_id, event_timestamp)
    event_key          STRING        NOT NULL OPTIONS (description = 'Deterministic surrogate key'),

    -- Source provenance
    source_system      STRING        NOT NULL OPTIONS (description = 'Origin system: eks|ecs|gke|vm'),
    environment        STRING        NOT NULL OPTIONS (description = 'dev|staging|prod'),
    cluster_name       STRING        OPTIONS (description = 'K8s / ECS cluster identifier'),
    region             STRING        OPTIONS (description = 'Cloud region, e.g. eu-west-2'),

    -- Event identity
    event_id           STRING        NOT NULL OPTIONS (description = 'Source-system event UUID'),
    event_type         STRING        NOT NULL OPTIONS (description = 'DEPLOY|SCALE|INCIDENT|ALERT|HEAL|ROLLBACK'),
    event_subtype      STRING        OPTIONS (description = 'Fine-grained classification'),
    service_name       STRING        OPTIONS (description = 'Microservice / workload name'),

    -- Timing
    event_timestamp    TIMESTAMP     NOT NULL OPTIONS (description = 'UTC event occurrence time'),
    event_date         DATE          NOT NULL OPTIONS (description = 'Partition column (DATE(event_timestamp))'),
    duration_ms        INT64         OPTIONS (description = 'Event duration in milliseconds'),

    -- Payload
    severity           STRING        OPTIONS (description = 'CRITICAL|HIGH|MEDIUM|LOW|INFO'),
    status             STRING        OPTIONS (description = 'SUCCESS|FAILURE|IN_PROGRESS|UNKNOWN'),
    message            STRING        OPTIONS (description = 'Human-readable event description'),
    payload_json       JSON          OPTIONS (description = 'Full event payload (schemaless)'),

    -- SLO linkage
    slo_name           STRING        OPTIONS (description = 'Linked SLO identifier'),
    error_budget_burn  FLOAT64       OPTIONS (description = 'Error budget burn rate at event time'),

    -- Cost attribution
    cost_centre        STRING        OPTIONS (description = 'Team / cost centre label'),
    squad              STRING        OPTIONS (description = 'Owning squad'),

    -- Lineage
    ingested_at        TIMESTAMP     NOT NULL OPTIONS (description = 'Pipeline ingestion timestamp'),
    pipeline_version   STRING        OPTIONS (description = 'Dataflow / Spark job version'),
    source_file        STRING        OPTIONS (description = 'GCS source path if batch ingestion')
)
PARTITION BY event_date
CLUSTER BY source_system, event_type, environment, severity
OPTIONS (
    description          = 'Raw fleet and operational events. One row per event. Append-only.',
    require_partition_filter = TRUE,
    partition_expiration_days = 365
);
