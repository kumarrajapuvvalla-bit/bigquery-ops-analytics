"""
cost_rollup_dag.py
──────────────────
Nightly cost aggregation DAG.
Runs the infra_cost_by_team dbt model and flags any cost spikes
in a BigQuery cost alert table.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    "owner": "platform-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}

GCP_PROJECT = "{{ var.value.gcp_project }}"
BQ_DATASET  = "ops_analytics"
DBT_DIR     = "/opt/airflow/dbt"


def alert_cost_spikes(**context):
    """Log cost spikes to BigQuery cost_alerts table."""
    from google.cloud import bigquery
    client = bigquery.Client(project=GCP_PROJECT)
    result = client.query(f"""
        SELECT team, environment, cost_category,
               total_cost_usd, dod_cost_delta_usd
        FROM `{GCP_PROJECT}.{BQ_DATASET}.infra_cost_by_team`
        WHERE cost_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
          AND is_cost_spike = TRUE
        ORDER BY dod_cost_delta_usd DESC
        LIMIT 10
    """).result()
    spikes = [dict(row) for row in result]
    if spikes:
        import logging
        logging.getLogger(__name__).warning(
            "Cost spikes detected: %s", spikes
        )


with DAG(
    dag_id="cost_rollup_nightly",
    description="Nightly cost aggregation and spike detection",
    schedule_interval="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    default_args=DEFAULT_ARGS,
    catchup=False,
    max_active_runs=1,
    tags=["cost", "finops", "bigquery"],
) as dag:

    dbt_cost = BashOperator(
        task_id="dbt_run_cost_models",
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt run --select stg_cost_events infra_cost_by_team "
            "--target prod"
        ),
    )

    alert_spikes = PythonOperator(
        task_id="alert_cost_spikes",
        python_callable=alert_cost_spikes,
    )

    dbt_cost >> alert_spikes
