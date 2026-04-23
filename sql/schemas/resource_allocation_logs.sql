-- =============================================================================
-- sql/schemas/resource_allocation_logs.sql
-- DDL: Resource allocation and utilisation logs per job
--
-- Pushed by the monitoring agent every 60 seconds during job execution.
-- NULL values in resource fields occur when the agent misses a collection window
-- (handled by the pandas cleansing layer).
-- =============================================================================

CREATE TABLE IF NOT EXISTS `ai_workload_telemetry.resource_allocation_logs`
(
    log_id              STRING        NOT NULL,
    job_id              STRING        NOT NULL OPTIONS (description = 'FK to job_metadata.job_id'),
    log_date            DATE          NOT NULL OPTIONS (description = 'Partition column'),
    collected_at        TIMESTAMP     NOT NULL OPTIONS (description = 'Agent collection timestamp'),

    -- Compute allocation
    cpu_requested_cores FLOAT64       OPTIONS  (description = 'vCPU cores requested at job start'),
    cpu_used_cores      FLOAT64       OPTIONS  (description = 'Avg vCPU cores used during window — NULL if agent missed'),
    cpu_throttle_pct    FLOAT64       OPTIONS  (description = '% of time CPU was throttled'),

    -- Memory
    memory_requested_gb FLOAT64       OPTIONS  (description = 'RAM requested in GB'),
    memory_used_gb      FLOAT64       OPTIONS  (description = 'Peak RAM used — NULL if agent missed'),
    memory_limit_gb     FLOAT64       OPTIONS  (description = 'Hard memory limit'),

    -- GPU (training jobs only)
    gpu_count           INT64         OPTIONS  (description = 'Number of GPUs allocated'),
    gpu_utilisation_pct FLOAT64       OPTIONS  (description = 'Avg GPU utilisation % — NULL if no GPU'),
    gpu_memory_used_gb  FLOAT64       OPTIONS  (description = 'GPU VRAM used in GB'),

    -- Network / storage
    network_egress_gb   FLOAT64       OPTIONS  (description = 'Network egress in GB'),
    disk_read_gb        FLOAT64       OPTIONS  (description = 'Disk read in GB'),
    disk_write_gb       FLOAT64       OPTIONS  (description = 'Disk write in GB'),

    -- Cost
    compute_cost_usd    NUMERIC       OPTIONS  (description = 'Estimated compute cost for this window'),
    node_type           STRING        OPTIONS  (description = 'GCP/AWS machine type e.g. n1-standard-8'),

    ingested_at         TIMESTAMP     NOT NULL
)
PARTITION BY log_date
CLUSTER BY job_id, log_date
OPTIONS (
    description          = 'Per-job resource utilisation samples at 60-second intervals.',
    require_partition_filter = TRUE
);
