-- sql/queries/deployment_trends.sql
-- =============================================================================
-- DORA metrics trend analysis: WoW comparison across 8-week rolling window
-- Joined with DORA Elite/High/Medium/Low classification
-- =============================================================================

WITH weekly AS (
  SELECT
    DATE_TRUNC(report_date, WEEK)     AS report_week,
    service_name,
    environment,
    squad,
    SUM(deploy_count)                 AS weekly_deploys,
    AVG(change_failure_rate)          AS avg_cfr,
    AVG(mean_time_to_recovery_mins)   AS avg_mttr_mins,
    SUM(incident_count)               AS weekly_incidents,
    AVG(p99_availability)             AS avg_availability,
    AVG(error_budget_remaining)       AS avg_error_budget
  FROM `ops_marts.fleet_health_daily`
  WHERE report_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 8 WEEK)
  GROUP BY 1, 2, 3, 4
),

with_wow AS (
  SELECT
    *,
    -- WoW changes
    SAFE_DIVIDE(
      weekly_deploys - LAG(weekly_deploys) OVER w,
      NULLIF(LAG(weekly_deploys) OVER w, 0)
    ) * 100                           AS deploy_wow_pct,

    SAFE_DIVIDE(
      avg_mttr_mins - LAG(avg_mttr_mins) OVER w,
      NULLIF(LAG(avg_mttr_mins) OVER w, 0)
    ) * 100                           AS mttr_wow_pct,

    -- DORA tier classification
    CASE
      WHEN weekly_deploys >= 7 AND avg_cfr <= 0.05 AND avg_mttr_mins <= 60    THEN 'Elite'
      WHEN weekly_deploys >= 1 AND avg_cfr <= 0.10 AND avg_mttr_mins <= 1440  THEN 'High'
      WHEN weekly_deploys >= 1 AND avg_cfr <= 0.15                            THEN 'Medium'
      ELSE 'Low'
    END                               AS dora_tier

  FROM weekly
  WINDOW w AS (
    PARTITION BY service_name, environment, squad
    ORDER BY report_week
    ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
  )
)

SELECT
  report_week,
  service_name,
  environment,
  squad,
  weekly_deploys,
  ROUND(avg_cfr * 100, 2)          AS cfr_pct,
  ROUND(avg_mttr_mins, 1)          AS mttr_mins,
  ROUND(avg_availability * 100, 4) AS availability_pct,
  ROUND(avg_error_budget * 100, 2) AS error_budget_pct,
  weekly_incidents,
  ROUND(deploy_wow_pct, 1)         AS deploy_wow_pct,
  ROUND(mttr_wow_pct, 1)           AS mttr_wow_pct,
  dora_tier
FROM with_wow
WHERE report_week >= DATE_SUB(CURRENT_DATE(), INTERVAL 4 WEEK)
ORDER BY report_week DESC, squad, service_name
