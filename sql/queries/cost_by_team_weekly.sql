-- cost_by_team_weekly.sql
-- Weekly infrastructure cost breakdown by team and environment.

SELECT
    DATE_TRUNC(cost_date, WEEK) AS week_start,
    team,
    environment,
    cost_category,
    ROUND(SUM(total_cost_usd), 2)              AS weekly_cost_usd,
    ROUND(AVG(rolling_30d_avg_daily_cost), 2)  AS avg_daily_cost_usd,
    SUM(CASE WHEN is_cost_spike THEN 1 ELSE 0 END) AS spike_days
FROM `${GCP_PROJECT}.ops_analytics.infra_cost_by_team`
WHERE cost_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY 1, 2, 3, 4
ORDER BY week_start DESC, weekly_cost_usd DESC
;
