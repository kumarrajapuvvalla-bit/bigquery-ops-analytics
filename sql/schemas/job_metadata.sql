-- =============================================================================
-- sql/schemas/job_metadata.sql
-- DDL: AI workload job metadata table
--
-- Every AI workload (model training, inference pipeline, data processing run)
-- pushes execution metadata periodically to this table.
-- Structurally equivalent to ECU battery data pushed on a schedule to cloud.
-- Partitioned by execution_date for cost-proportional querying.
-- =============================================================================

CREATE TABLE IF NOT EXISTS `ai_workload_telemetry.job_metadata`
(
    -- Identity
    job_id              STRING        NOT NULL OPTIONS (description = 'Unique job execution UUID'),
    job_name            STRING        NOT NULL OPTIONS (description = 'Human-readable job name'),
    workload_type       STRING        NOT NULL OPTIONS (description = 'model_training|inference|data_processing|fine_tuning'),
    workload_class      STRING        OPTIONS  (description = 'Sub-classification: batch|streaming|interactive'),

    -- Execution context
    environment         STRING        NOT NULL OPTIONS (description = 'dev|staging|prod'),
    cloud_provider      STRING        NOT NULL OPTIONS (description = 'gcp|aws'),
    region              STRING        OPTIONS  (description = 'Cloud region, e.g. us-central1'),
    cluster_id          STRING        OPTIONS  (description = 'GKE/EKS cluster identifier'),
    team                STRING        OPTIONS  (description = 'Owning team / cost centre'),

    -- Timing
    submitted_at        TIMESTAMP     NOT NULL OPTIONS (description = 'Job submission timestamp'),
    started_at          TIMESTAMP     OPTIONS  (description = 'Execution start timestamp'),
    completed_at        TIMESTAMP     OPTIONS  (description = 'Execution completion timestamp'),
    execution_date      DATE          NOT NULL OPTIONS (description = 'Partition column: DATE(submitted_at)'),
    duration_seconds    FLOAT64       OPTIONS  (description = 'Total wall-clock duration in seconds'),

    -- Outcome
    status              STRING        NOT NULL OPTIONS (description = 'SUCCESS|FAILURE|TIMEOUT|CANCELLED|RETRYING'),
    exit_code           INT64         OPTIONS  (description = 'Process exit code; 0 = success'),
    retry_attempt       INT64         NOT NULL DEFAULT 0 OPTIONS (description = '0 for first attempt; >0 for retries'),
    parent_job_id       STRING        OPTIONS  (description = 'Original job_id for retry records — used for deduplication'),
    error_category      STRING        OPTIONS  (description = 'OOM|TIMEOUT|DEPENDENCY|INFRA|UNKNOWN'),
    error_message       STRING        OPTIONS  (description = 'Truncated error message from job runner'),

    -- Model / data context
    model_name          STRING        OPTIONS  (description = 'Model identifier for training/inference jobs'),
    model_version       STRING        OPTIONS  (description = 'Semantic version of the model'),
    dataset_size_gb     FLOAT64       OPTIONS  (description = 'Input dataset size in gigabytes'),
    checkpoint_path     STRING        OPTIONS  (description = 'GCS/S3 path to checkpoint, if applicable'),

    -- Lineage
    pipeline_name       STRING        OPTIONS  (description = 'Parent pipeline or DAG name'),
    pipeline_run_id     STRING        OPTIONS  (description = 'Parent pipeline execution ID'),
    commit_sha          STRING        OPTIONS  (description = 'Git commit SHA that triggered the job'),
    ingested_at         TIMESTAMP     NOT NULL OPTIONS (description = 'BQ ingestion timestamp')
)
PARTITION BY execution_date
CLUSTER BY workload_type, environment, status, team
OPTIONS (
    description          = 'AI workload job execution metadata. One row per job attempt. Append-only.',
    require_partition_filter = TRUE,
    partition_expiration_days = 365
);
