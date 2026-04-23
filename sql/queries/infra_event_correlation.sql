-- =============================================================================
-- sql/queries/infra_event_correlation.sql
-- Correlates infrastructure events with job anomalies
-- Used in the investigation to identify the resource contention pattern
--
-- Key finding from this query: model training jobs running 40-60% longer
-- aligned precisely with a QUOTA_THROTTLE pattern during a specific daily window.
-- =============================================================================

DECLARE scan_date DATE DEFAULT @scan_date;
DECLARE anomaly_z_threshold FLOAT64 DEFAULT 2.0;

WITH anomalous_jobs AS (
    -- Pull jobs flagged as anomalous by the extraction query
    SELECT
        job_id,
        workload_type,
        team,
        started_at,
        completed_at,
        duration_seconds,
        duration_z_score,
        EXTRACT(HOUR FROM started_at)   AS start_hour
    FROM `ai_workload_telemetry.job_metadata` jm
    WHERE execution_date = scan_date
      AND status = 'SUCCESS'           -- anomaly is duration, not failure
      AND duration_seconds IS NOT NULL
    -- Subquery to get z_score; in production this reads from the extracted view
    QUALIFY SAFE_DIVIDE(
        duration_seconds - AVG(duration_seconds) OVER (
            PARTITION BY workload_type
            ORDER BY execution_date
            ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ),
        NULLIF(STDDEV_POP(duration_seconds) OVER (
            PARTITION BY workload_type
            ORDER BY execution_date
            ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ), 0)
    ) > anomaly_z_threshold
),

-- Infrastructure events during the same daily window
overlapping_events AS (
    SELECT
        ie.event_type,
        ie.severity,
        ie.affected_zone,
        EXTRACT(HOUR FROM ie.event_timestamp) AS event_hour,
        COUNT(DISTINCT aj.job_id)           AS affected_anomalous_jobs,
        COUNT(DISTINCT ie.event_id)         AS event_count,
        AVG(aj.duration_z_score)            AS avg_anomaly_zscore
    FROM anomalous_jobs aj
    JOIN `ai_workload_telemetry.infrastructure_events` ie
        ON ie.event_date = scan_date
        AND ie.event_timestamp BETWEEN
            TIMESTAMP_SUB(aj.started_at, INTERVAL 5 MINUTE)
            AND TIMESTAMP_ADD(aj.completed_at, INTERVAL 5 MINUTE)
    GROUP BY 1, 2, 3, 4
),

-- Hourly pattern: which hours see the most correlated anomalies?
hourly_pattern AS (
    SELECT
        start_hour,
        COUNT(DISTINCT job_id)  AS anomalous_job_count,
        AVG(duration_z_score)   AS avg_z_score,
        AVG(duration_seconds)   AS avg_duration_s
    FROM anomalous_jobs
    GROUP BY start_hour
)

SELECT
    oe.*,
    hp.anomalous_job_count,
    hp.avg_z_score,
    hp.avg_duration_s
FROM overlapping_events oe
JOIN hourly_pattern hp ON hp.start_hour = oe.event_hour
ORDER BY oe.affected_anomalous_jobs DESC, oe.avg_anomaly_zscore DESC
