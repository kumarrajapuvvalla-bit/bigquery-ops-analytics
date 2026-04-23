-- =============================================================================
-- sql/queries/extract_job_metadata.sql
-- Core extraction: join job_metadata + resource_allocation_logs + infra events
-- on job_id and timestamp to get a complete picture of each workload run.
--
-- Design principles from the interview:
--   - CTEs for readability and maintainability
--   - Window functions for rolling averages and baseline deviation
--   - Version-controlled — every query lives in Git so output is reproducible
-- =============================================================================

DECLARE scan_date DATE DEFAULT @scan_date;

-- ── CTE 1: Base job records (deduplicated — keep final retry only) ───────────
WITH deduplicated_jobs AS (
    SELECT
        job_id,
        COALESCE(parent_job_id, job_id)     AS canonical_job_id,
        job_name,
        workload_type,
        workload_class,
        environment,
        cloud_provider,
        region,
        team,
        submitted_at,
        started_at,
        completed_at,
        execution_date,
        duration_seconds,
        status,
        exit_code,
        retry_attempt,
        error_category,
        error_message,
        model_name,
        dataset_size_gb,
        pipeline_name,
        pipeline_run_id
    FROM `ai_workload_telemetry.job_metadata`
    WHERE execution_date = scan_date
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY COALESCE(parent_job_id, job_id)
        ORDER BY retry_attempt DESC, ingested_at DESC
    ) = 1
),

-- ── CTE 2: Resource aggregates per job ───────────────────────────────────────
resource_summary AS (
    SELECT
        job_id,
        AVG(cpu_used_cores)             AS avg_cpu_cores,
        MAX(cpu_used_cores)             AS peak_cpu_cores,
        MAX(memory_used_gb)             AS peak_memory_gb,
        AVG(gpu_utilisation_pct)        AS avg_gpu_utilisation_pct,
        MAX(gpu_utilisation_pct)        AS peak_gpu_utilisation_pct,
        SUM(compute_cost_usd)           AS total_compute_cost_usd,
        AVG(cpu_throttle_pct)           AS avg_cpu_throttle_pct,
        -- Count missed collection windows (NULL = monitoring agent missed window)
        COUNTIF(cpu_used_cores IS NULL) AS missed_collection_windows,
        COUNT(*)                        AS total_collection_windows,
        ANY_VALUE(node_type)            AS node_type
    FROM `ai_workload_telemetry.resource_allocation_logs`
    WHERE log_date = scan_date
    GROUP BY job_id
),

-- ── CTE 3: Infrastructure events overlapping each job window ─────────────────
infra_impact AS (
    SELECT
        jm.job_id,
        COUNT(ie.event_id)              AS overlapping_infra_events,
        COUNTIF(ie.severity = 'CRITICAL') AS critical_infra_events,
        COUNTIF(ie.event_type = 'NODE_PREEMPTION') AS preemption_events,
        COUNTIF(ie.event_type = 'SPOT_INTERRUPT')  AS spot_interruptions,
        COUNTIF(ie.event_type = 'QUOTA_THROTTLE')  AS quota_throttle_events,
        -- Timestamp join: infra event within job execution window
        STRING_AGG(DISTINCT ie.event_type, ', ') AS impacting_event_types
    FROM deduplicated_jobs jm
    LEFT JOIN `ai_workload_telemetry.infrastructure_events` ie
        ON ie.event_date = scan_date
        -- ±30s window to account for clock skew between systems
        AND ie.event_timestamp BETWEEN
            TIMESTAMP_SUB(jm.started_at, INTERVAL 30 SECOND)
            AND TIMESTAMP_ADD(jm.completed_at, INTERVAL 30 SECOND)
    GROUP BY jm.job_id
),

-- ── CTE 4: Workload-type baseline (30-day rolling window) ────────────────────
-- This is what enables anomaly detection in downstream Python
workload_baseline AS (
    SELECT
        workload_type,
        AVG(duration_seconds)           AS baseline_mean_duration_s,
        STDDEV_POP(duration_seconds)    AS baseline_stddev_duration_s,
        AVG(CASE WHEN status = 'FAILURE' THEN 1.0 ELSE 0.0 END) AS baseline_failure_rate,
        COUNT(*)                        AS baseline_sample_count
    FROM `ai_workload_telemetry.job_metadata`
    WHERE execution_date BETWEEN
        DATE_SUB(scan_date, INTERVAL 30 DAY) AND DATE_SUB(scan_date, INTERVAL 1 DAY)
      AND status IN ('SUCCESS', 'FAILURE')
      AND retry_attempt = 0   -- baseline uses first-attempt only
    GROUP BY workload_type
    HAVING COUNT(*) >= 30     -- minimum sample size for statistical validity
),

-- ── CTE 5: Per-job Z-score against workload-type baseline ────────────────────
jobs_with_zscore AS (
    SELECT
        j.*,
        b.baseline_mean_duration_s,
        b.baseline_stddev_duration_s,
        b.baseline_failure_rate,
        b.baseline_sample_count,
        CASE
            WHEN b.baseline_stddev_duration_s = 0 OR b.baseline_stddev_duration_s IS NULL THEN NULL
            ELSE SAFE_DIVIDE(
                j.duration_seconds - b.baseline_mean_duration_s,
                b.baseline_stddev_duration_s
            )
        END                             AS duration_z_score,
        -- Flag: >2 standard deviations from workload-type mean (interview threshold)
        CASE
            WHEN b.baseline_stddev_duration_s > 0
             AND ABS(SAFE_DIVIDE(
                j.duration_seconds - b.baseline_mean_duration_s,
                b.baseline_stddev_duration_s
             )) > 2.0
            THEN TRUE ELSE FALSE
        END                             AS is_duration_anomaly
    FROM deduplicated_jobs j
    LEFT JOIN workload_baseline b USING (workload_type)
)

-- ── Final join ────────────────────────────────────────────────────────────────
SELECT
    jz.*,
    r.avg_cpu_cores,
    r.peak_cpu_cores,
    r.peak_memory_gb,
    r.avg_gpu_utilisation_pct,
    r.peak_gpu_utilisation_pct,
    r.total_compute_cost_usd,
    r.avg_cpu_throttle_pct,
    r.missed_collection_windows,
    r.total_collection_windows,
    r.node_type,
    ii.overlapping_infra_events,
    ii.critical_infra_events,
    ii.preemption_events,
    ii.spot_interruptions,
    ii.quota_throttle_events,
    ii.impacting_event_types,
    CURRENT_TIMESTAMP()             AS extracted_at
FROM jobs_with_zscore           jz
LEFT JOIN resource_summary      r   ON r.job_id   = jz.job_id
LEFT JOIN infra_impact          ii  ON ii.job_id  = jz.job_id
ORDER BY is_duration_anomaly DESC, ABS(duration_z_score) DESC NULLS LAST
