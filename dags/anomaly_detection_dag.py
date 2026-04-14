"""
Anomaly detection DAG.
Runs the int_anomaly_candidates dbt model every hour and writes
any new anomalies to the anomaly_log mart.
Fires a PagerDuty incident if anomaly_severity = 'critical'.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    "owner": "platform-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}

GCP_PROJECT = "{{ var.value.gcp_project }}"
BQ_DATASET  = "ops_analytics"
DBT_DIR     = "/opt/airflow/dbt"


def trigger_pagerduty_if_critical(**context):
    """Fire a PagerDuty event if critical anomalies exist in the last hour."""
    import os, json
    import urllib.request
    from google.cloud import bigquery

    client = bigquery.Client(project=GCP_PROJECT)
    result = client.query(f"""
        SELECT COUNT(*) AS cnt
        FROM `{GCP_PROJECT}.{BQ_DATASET}.anomaly_log`
        WHERE event_hour >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
          AND anomaly_severity = 'critical'
    """).result()
    cnt = next(result).cnt
    if cnt == 0:
        return

    pd_key = os.environ.get("PAGERDUTY_ROUTING_KEY", "")
    if not pd_key:
        return

    payload = json.dumps({
        "routing_key": pd_key,
        "event_action": "trigger",
        "payload": {
            "summary": f"Fleet anomaly: {cnt} critical event(s) in the last hour",
            "severity": "critical",
            "source": "bigquery-ops-analytics",
            "custom_details": {"anomaly_count": cnt},
        },
    }).encode()

    req = urllib.request.Request(
        "https://events.pagerduty.com/v2/enqueue",
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    urllib.request.urlopen(req, timeout=10)


with DAG(
    dag_id="anomaly_detection_hourly",
    description="Hourly anomaly detection scan on fleet health scores",
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    default_args=DEFAULT_ARGS,
    catchup=False,
    max_active_runs=1,
    tags=["anomaly", "fleet", "bigquery"],
) as dag:

    run_anomaly_model = BashOperator(
        task_id="dbt_run_anomaly_candidates",
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt run --select int_anomaly_candidates anomaly_log "
            "--target prod"
        ),
    )

    check_pagerduty = PythonOperator(
        task_id="trigger_pagerduty_if_critical",
        python_callable=trigger_pagerduty_if_critical,
    )

    run_anomaly_model >> check_pagerduty
