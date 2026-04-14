"""
fleet_analytics_dag.py
──────────────────────
Airflow DAG that orchestrates the daily fleet analytics pipeline:

  1. Check BigQuery source freshness (ops_raw tables)
  2. Run dbt staging models (stg_fleet_events, stg_infra_metrics)
  3. Run dbt intermediate models (int_fleet_health_scores)
  4. Run dbt mart models (fleet_readiness_daily, anomaly_log)
  5. Run anomaly detection scan
  6. Send Slack / PagerDuty alert if critical anomalies detected
  7. Refresh Looker dashboard cache

Schedule: daily at 01:00 UTC
Owner:    platform-eng
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from airflow.utils.task_group import TaskGroup

log = logging.getLogger(__name__)

# ── Default args ────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "platform-eng",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

GCP_PROJECT = "{{ var.value.gcp_project }}"
BQ_DATASET  = "ops_analytics"
DBT_DIR     = "/opt/airflow/dbt"


# ── Python callables ─────────────────────────────────────────────────────────
def check_critical_anomalies(**context):
    """Query anomaly_log for any critical anomalies in the last 24 h."""
    from google.cloud import bigquery
    client = bigquery.Client(project=GCP_PROJECT)
    query = f"""
        SELECT COUNT(*) AS cnt
        FROM `{GCP_PROJECT}.{BQ_DATASET}.anomaly_log`
        WHERE event_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
          AND anomaly_severity = 'critical'
    """
    result = client.query(query).result()
    cnt = next(result).cnt
    log.info("Critical anomalies yesterday: %d", cnt)
    context["task_instance"].xcom_push(key="critical_count", value=cnt)
    return "send_alert" if cnt > 0 else "skip_alert"


def send_slack_alert(**context):
    """Post a Slack message when critical anomalies are detected."""
    import os, json
    import urllib.request

    cnt = context["task_instance"].xcom_pull(
        task_ids="branch_on_anomalies", key="critical_count"
    )
    webhook = os.environ["SLACK_OPS_WEBHOOK_URL"]
    payload = json.dumps({
        "text": (
            f":rotating_light: *Fleet Anomaly Alert* — "
            f"{cnt} critical anomaly event(s) detected yesterday.\n"
            f"<https://lookerstudio.google.com|View Dashboard>"
        )
    }).encode()
    req = urllib.request.Request(
        webhook, data=payload,
        headers={"Content-Type": "application/json"}
    )
    urllib.request.urlopen(req, timeout=10)
    log.info("Slack alert sent (critical_count=%d)", cnt)


# ── DAG definition ───────────────────────────────────────────────────────────
with DAG(
    dag_id="fleet_analytics_daily",
    description="Daily fleet KPI pipeline: ingest → dbt → anomaly detection → alert",
    schedule_interval="0 1 * * *",  # 01:00 UTC daily
    start_date=datetime(2026, 1, 1),
    default_args=DEFAULT_ARGS,
    catchup=False,
    max_active_runs=1,
    tags=["fleet", "analytics", "dbt", "bigquery"],
    doc_md=__doc__,
) as dag:

    # ── 1. Source freshness check ──────────────────────────────────────────
    with TaskGroup(group_id="source_checks") as source_checks:
        check_fleet_events = BigQueryCheckOperator(
            task_id="check_fleet_events_fresh",
            sql=f"""
                SELECT COUNT(*) > 0
                FROM `{GCP_PROJECT}.ops_raw.fleet_events`
                WHERE event_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 25 HOUR)
            """,
            use_legacy_sql=False,
            gcp_conn_id="google_cloud_default",
        )
        check_infra_metrics = BigQueryCheckOperator(
            task_id="check_infra_metrics_fresh",
            sql=f"""
                SELECT COUNT(*) > 0
                FROM `{GCP_PROJECT}.ops_raw.infra_metrics`
                WHERE scraped_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 25 HOUR)
            """,
            use_legacy_sql=False,
            gcp_conn_id="google_cloud_default",
        )

    # ── 2. dbt staging ────────────────────────────────────────────────────
    dbt_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt run --select tag:staging --target prod "
            "--no-partial-parse"
        ),
        env={"GCP_PROJECT": GCP_PROJECT, "BQ_DATASET": BQ_DATASET},
    )

    # ── 3. dbt intermediate ───────────────────────────────────────────────
    with TaskGroup(group_id="dbt_intermediate") as dbt_int:
        dbt_fleet_scores = BashOperator(
            task_id="dbt_run_int_fleet_scores",
            bash_command=(
                f"cd {DBT_DIR} && "
                "dbt run --select int_fleet_health_scores --target prod"
            ),
        )
        dbt_anomaly_candidates = BashOperator(
            task_id="dbt_run_int_anomaly_candidates",
            bash_command=(
                f"cd {DBT_DIR} && "
                "dbt run --select int_anomaly_candidates --target prod"
            ),
        )
        dbt_fleet_scores >> dbt_anomaly_candidates

    # ── 4. dbt marts ──────────────────────────────────────────────────────
    dbt_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt run --select tag:mart --target prod"
        ),
    )

    # ── 5. dbt test ───────────────────────────────────────────────────────
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt test --target prod --store-failures"
        ),
    )

    # ── 6. Branch on anomaly count ────────────────────────────────────────
    branch_on_anomalies = BranchPythonOperator(
        task_id="branch_on_anomalies",
        python_callable=check_critical_anomalies,
    )

    send_alert = PythonOperator(
        task_id="send_alert",
        python_callable=send_slack_alert,
    )

    skip_alert = BashOperator(
        task_id="skip_alert",
        bash_command="echo 'No critical anomalies — skipping alert'",
    )

    # ── DAG wiring ────────────────────────────────────────────────────────
    source_checks >> dbt_staging >> dbt_int >> dbt_marts >> dbt_test
    dbt_test >> branch_on_anomalies >> [send_alert, skip_alert]
