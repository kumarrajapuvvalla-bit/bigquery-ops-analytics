"""baseline_calculator.py
==============================================================================
Computes and persists rolling baseline statistics per workload type.

Used as a prerequisite by the anomaly detector. Baseline is computed over
a configurable rolling window (default: 30 days of successful first-attempt jobs).

Run daily to keep the baseline current:
    python anomaly_detection/baseline_calculator.py \
        --input data/clean/ \
        --output data/baselines/ \
        --window-days 30
==============================================================================
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s  %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)


def compute_baseline(
    df: pd.DataFrame,
    window_days: int = 30,
    reference_date: date | None = None,
) -> pd.DataFrame:
    """Compute workload-type baseline stats from a historical DataFrame.

    Returns a DataFrame with one row per (workload_type) containing:
      - baseline_mean_duration_s
      - baseline_stddev_duration_s
      - baseline_median_duration_s
      - baseline_p95_duration_s
      - baseline_failure_rate
      - sample_count
      - window_start, window_end
    """
    if reference_date is None:
        reference_date = date.today()

    window_start = reference_date - timedelta(days=window_days)

    if "execution_date" in df.columns:
        df["execution_date"] = pd.to_datetime(df["execution_date"]).dt.date
        df = df[
            (df["execution_date"] >= window_start) &
            (df["execution_date"] < reference_date)
        ]

    # Clean subset: successful, non-retry jobs only
    mask = pd.Series(True, index=df.index)
    if "status" in df.columns:
        mask &= df["status"] == "SUCCESS"
    if "retry_attempt" in df.columns:
        mask &= df["retry_attempt"] == 0

    df_clean = df[mask].copy()

    if df_clean.empty:
        logger.warning("No qualifying jobs for baseline computation in window %s to %s",
                       window_start, reference_date)
        return pd.DataFrame()

    baseline = (
        df_clean.groupby("workload_type")
        .agg(
            baseline_mean_duration_s=("duration_seconds", "mean"),
            baseline_stddev_duration_s=("duration_seconds", "std"),
            baseline_median_duration_s=("duration_seconds", "median"),
            baseline_p95_duration_s=("duration_seconds", lambda x: x.quantile(0.95)),
            baseline_p99_duration_s=("duration_seconds", lambda x: x.quantile(0.99)),
            sample_count=("duration_seconds", "count"),
        )
        .reset_index()
    )

    # Failure rate (computed on ALL jobs in window, not just successful)
    all_mask = pd.Series(True, index=df.index)
    if "execution_date" in df.columns:
        all_mask &= (df["execution_date"] >= window_start) & (df["execution_date"] < reference_date)

    failure_rates = (
        df[all_mask]
        .groupby("workload_type")
        .apply(lambda g: (g["status"] == "FAILURE").mean() if "status" in g.columns else 0.0)
        .reset_index()
        .rename(columns={0: "baseline_failure_rate"})
    )

    baseline = baseline.merge(failure_rates, on="workload_type", how="left")
    baseline["window_start"] = str(window_start)
    baseline["window_end"]   = str(reference_date)
    baseline["window_days"]  = window_days

    # Filter out types with insufficient data (< 30 samples unreliable for Z-score)
    before = len(baseline)
    baseline = baseline[baseline["sample_count"] >= 30]
    if len(baseline) < before:
        logger.warning("Filtered %d workload types with <30 samples", before - len(baseline))

    return baseline.round(4)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",       required=True, help="Directory of clean parquet files")
    parser.add_argument("--output",      required=True, help="Output baseline parquet path")
    parser.add_argument("--window-days", type=int, default=30)
    args = parser.parse_args()

    input_dir = Path(args.input)
    files = sorted(input_dir.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet files found in {input_dir}")

    logger.info("Loading %d files from %s", len(files), input_dir)
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)

    baseline = compute_baseline(df, window_days=args.window_days)

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    baseline.to_parquet(out, index=False)
    logger.info("Baseline written to %s (%d workload types)", out, len(baseline))
    print(baseline.to_string())


if __name__ == "__main__":
    main()
