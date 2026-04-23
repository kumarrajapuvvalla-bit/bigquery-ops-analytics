-- =============================================================================
-- sql/queries/resource_utilisation.sql
-- Resource utilisation trends by workload type and team
-- Drives the "Workload Trends" and drill-down Grafana dashboard views
-- =============================================================================

DECLARE scan_date DATE DEFAULT @scan_date;
DECLARE lookback_days INT64 DEFAULT 30;

WITH daily_resource_summary AS (
    SELECT
        DATE(r.collected_at)            AS summary_date,
        j.workload_type,
        j.team,
        j.cloud_provider,
        j.environment,
        j.node_type,

        -- CPU metrics
        AVG(r.cpu_used_cores)           AS avg_cpu_used,
        AVG(r.cpu_requested_cores)      AS avg_cpu_requested,
        AVG(SAFE_DIVIDE(r.cpu_used_cores, r.cpu_requested_cores))
                                        AS avg_cpu_efficiency,
        AVG(r.cpu_throttle_pct)         AS avg_cpu_throttle_pct,

        -- Memory metrics
        AVG(r.memory_used_gb)           AS avg_memory_used_gb,
        MAX(r.memory_used_gb)           AS peak_memory_used_gb,
        AVG(SAFE_DIVIDE(r.memory_used_gb, r.memory_limit_gb))
                                        AS avg_memory_pressure,

        -- GPU metrics
        AVG(r.gpu_utilisation_pct)      AS avg_gpu_utilisation_pct,
        AVG(r.gpu_memory_used_gb)       AS avg_gpu_memory_used_gb,

        -- Cost
        SUM(r.compute_cost_usd)         AS daily_compute_cost_usd,

        COUNT(DISTINCT r.job_id)        AS distinct_jobs,
        COUNT(*)                        AS sample_count

    FROM `ai_workload_telemetry.resource_allocation_logs` r
    JOIN `ai_workload_telemetry.job_metadata` j
        ON j.job_id = r.job_id
        AND j.execution_date = DATE(r.collected_at)
    WHERE r.log_date BETWEEN DATE_SUB(scan_date, INTERVAL lookback_days DAY) AND scan_date
      AND r.cpu_used_cores IS NOT NULL   -- exclude missed collection windows
    GROUP BY 1, 2, 3, 4, 5, 6
),

-- WoW trend detection: compare this week vs. prior week
with_wow_trend AS (
    SELECT
        *,
        -- Detect workload classes where cost is growing week-over-week
        LAG(daily_compute_cost_usd) OVER (
            PARTITION BY workload_type, team
            ORDER BY summary_date
        )                               AS prev_day_cost_usd,
        SAFE_DIVIDE(
            daily_compute_cost_usd
            - LAG(daily_compute_cost_usd) OVER (
                PARTITION BY workload_type, team ORDER BY summary_date
            ),
            NULLIF(LAG(daily_compute_cost_usd) OVER (
                PARTITION BY workload_type, team ORDER BY summary_date
            ), 0)
        ) * 100                         AS cost_change_pct
    FROM daily_resource_summary
)

SELECT * FROM with_wow_trend
WHERE summary_date = scan_date
ORDER BY daily_compute_cost_usd DESC, workload_type, team
