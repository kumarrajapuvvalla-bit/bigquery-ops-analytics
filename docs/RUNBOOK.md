# Runbook — bigquery-ops-analytics

## On-call Procedures

### Alert: Critical Anomaly Detected

**Trigger:** PagerDuty P1 or Slack `#ops-alerts` message with `Fleet Anomaly Alert`

1. Check the Looker **Fleet Health** dashboard for affected cluster(s).
2. Query `anomaly_log` for the last hour:
   ```sql
   SELECT *
   FROM `project.ops_analytics.anomaly_log`
   WHERE event_date = CURRENT_DATE()
     AND anomaly_severity = 'critical'
   ORDER BY event_hour DESC
   LIMIT 20;
   ```
3. Cross-reference with `ops-platform` Grafana to confirm the raw metrics.
4. If confirmed infrastructure issue: escalate to infra on-call.
5. If data pipeline issue: check Airflow DAG `fleet_analytics_daily` for task failures.

---

### Alert: Cost Spike Detected

**Trigger:** `cost_rollup_nightly` DAG logs a cost spike warning.

1. Query `infra_cost_by_team` for yesterday's spikes:
   ```sql
   SELECT team, environment, cost_category,
          total_cost_usd, dod_cost_delta_usd
   FROM `project.ops_analytics.infra_cost_by_team`
   WHERE cost_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
     AND is_cost_spike = TRUE
   ORDER BY dod_cost_delta_usd DESC;
   ```
2. Identify the responsible team and service.
3. Check for unexpected resource scaling events in the relevant AWS/GCP console.
4. Notify the team lead via Slack `#finops-alerts`.

---

### dbt Run Failure

1. Check Airflow task logs: `fleet_analytics_daily > dbt_run_marts`.
2. Run locally:
   ```bash
   cd dbt
   dbt run --select <failing_model> --target prod
   dbt test --select <failing_model> --target prod
   ```
3. Common causes:
   - Source table not refreshed (check `dbt source freshness`)
   - Schema change in upstream raw table
   - BigQuery slot exhaustion (check GCP Quota page)

---

### CI Failures

| Job | Failure | Fix |
|-----|---------|-----|
| `dbt-compile` | Jinja/SQL syntax error | `dbt compile --select <model>` locally |
| `sqlfluff` | SQL style violation | `sqlfluff fix dbt/models/ --dialect bigquery` |
| `pytest` | Python unit test failure | `pytest tests/ -v` locally |
