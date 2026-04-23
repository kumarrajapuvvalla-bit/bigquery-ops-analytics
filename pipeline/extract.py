"""extract.py — BigQuery extraction task (Step 1 of the DAG pipeline)."""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s  %(message)s",
                    handlers=[logging.StreamHandler(sys.stdout)])


def extract(run_date: str, output_path: Path) -> None:
    """Pull job metadata, resource logs, and infra events from BigQuery.

    In production: uses google-cloud-bigquery client with service account auth.
    The SQL is loaded from sql/queries/ so queries are version-controlled and
    every output dataset is reproducible from a known query + date.
    """
    project_id = os.environ.get("GCP_PROJECT_ID")
    dataset    = os.environ.get("BQ_DATASET", "ai_workload_telemetry")

    if not project_id:
        raise EnvironmentError("GCP_PROJECT_ID environment variable not set")

    try:
        from google.cloud import bigquery
        client = bigquery.Client(project=project_id)
    except ImportError:
        logger.warning("google-cloud-bigquery not installed. Generating synthetic data for demo.")
        _generate_synthetic(run_date, output_path)
        return

    # Load the version-controlled SQL query
    sql_path = Path("sql/queries/extract_job_metadata.sql")
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL query not found: {sql_path}")

    query = sql_path.read_text().replace("@scan_date", f"DATE('{run_date}')")
    logger.info("Running BigQuery extraction for date=%s, project=%s, dataset=%s",
                run_date, project_id, dataset)

    df = client.query(query).to_dataframe()
    logger.info("Extracted %d rows from BigQuery", len(df))

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)
    logger.info("Saved to %s", output_path)


def _generate_synthetic(run_date: str, output_path: Path) -> None:
    """Generate a synthetic dataset for local development and testing."""
    import numpy as np
    import pandas as pd

    rng = np.random.default_rng(42)
    n = 500
    workload_types = ["model_training", "inference", "data_processing", "fine_tuning"]
    statuses = ["SUCCESS"] * 85 + ["FAILURE"] * 10 + ["TIMEOUT"] * 5

    df = pd.DataFrame({
        "job_id":          [f"job_{i:06d}" for i in range(n)],
        "canonical_job_id":[f"job_{i:06d}" for i in range(n)],
        "job_name":        [f"workload_{i % 20}" for i in range(n)],
        "workload_type":   rng.choice(workload_types, n),
        "environment":     rng.choice(["prod", "staging"], n, p=[0.8, 0.2]),
        "cloud_provider":  rng.choice(["gcp", "aws"], n, p=[0.7, 0.3]),
        "team":            rng.choice(["ml-platform", "data-eng", "inference-team"], n),
        "status":          rng.choice(statuses, n),
        "retry_attempt":   rng.choice([0, 0, 0, 0, 1, 2], n),
        "execution_date":  run_date,
        "duration_seconds": rng.normal(3600, 600, n).clip(60, 86400),
        "cpu_used_cores":  rng.uniform(1, 32, n),
        "memory_used_gb":  rng.uniform(2, 128, n),
        "gpu_utilisation_pct": rng.uniform(0, 100, n),
        "total_compute_cost_usd": rng.uniform(0.5, 50, n),
        "overlapping_infra_events": rng.choice([0, 0, 0, 1, 2, 3], n),
        "is_duration_anomaly": rng.choice([False, False, False, False, True], n),
        "ingested_at":     pd.Timestamp.now(tz="UTC").isoformat(),
    })

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)
    logger.info("Synthetic data written to %s (%d rows)", output_path, len(df))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date",   required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    extract(args.date, Path(args.output))


if __name__ == "__main__":
    main()
