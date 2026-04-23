-- =============================================================================
-- sql/schemas/infrastructure_events.sql
-- DDL: Infrastructure-level events that may affect workload performance
--
-- Includes node preemptions, spot instance interruptions, quota throttles,
-- network degradation events, and GCP/AWS service disruptions.
-- Timestamp skew vs. job_metadata is handled by the cleansing layer
-- using a ±30s normalisation window.
-- =============================================================================

CREATE TABLE IF NOT EXISTS `ai_workload_telemetry.infrastructure_events`
(
    event_id            STRING        NOT NULL,
    event_date          DATE          NOT NULL OPTIONS (description = 'Partition column'),
    event_timestamp     TIMESTAMP     NOT NULL OPTIONS (description = 'Infrastructure event occurrence — may skew ±30s vs. job_metadata'),

    -- Event classification
    event_type          STRING        NOT NULL OPTIONS (description = 'NODE_PREEMPTION|SPOT_INTERRUPT|QUOTA_THROTTLE|NETWORK_DEGRADATION|SERVICE_DISRUPTION'),
    severity            STRING        OPTIONS  (description = 'CRITICAL|HIGH|MEDIUM|LOW|INFO'),
    source_system       STRING        OPTIONS  (description = 'gke|gcp-compute|aws-ec2|cloud-monitoring'),

    -- Scope
    affected_node       STRING        OPTIONS  (description = 'Node hostname or instance ID'),
    affected_zone       STRING        OPTIONS  (description = 'GCP zone or AWS AZ'),
    affected_pool       STRING        OPTIONS  (description = 'Node pool or ASG name'),

    -- Resource impact
    resource_type       STRING        OPTIONS  (description = 'cpu|memory|gpu|network|disk'),
    impact_description  STRING        OPTIONS  (description = 'Human-readable impact description'),
    duration_seconds    INT64         OPTIONS  (description = 'Event duration in seconds'),

    -- Correlation
    correlated_job_ids  ARRAY<STRING> OPTIONS  (description = 'Job IDs affected by this event'),

    ingested_at         TIMESTAMP     NOT NULL
)
PARTITION BY event_date
CLUSTER BY event_type, severity, source_system
OPTIONS (
    description = 'Infrastructure events that may correlate with workload anomalies.',
    require_partition_filter = FALSE
);
