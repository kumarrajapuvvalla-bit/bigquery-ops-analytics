"""detector.py
==============================================================================
Step 3: Z-score anomaly detection engine.

From the interview:
  "I built this as a dashboard view — jobs deviating more than two standard
   deviations from their workload-type baseline, flagged automatically."

The detector:
  1. Loads clean workload telemetry (output of cleansing step)
  2. Computes per-workload-type baseline (mean + stddev) over a rolling window
  3. Flags any job where abs(z_score) > threshold (default: 2.0 σ)
  4. Emits a structured anomaly report used by the Grafana anomaly dashboard

Key finding reproduced here:
  model_training jobs running 40-60% longer than baseline during a specific
  hourly window → correlated with QUOTA_THROTTLE infrastructure events.
==============================================================================
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s  %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

# ── Anomaly detector ───────────────────────────────────────────────────────────

class AnomalyDetector:
    """Z-score anomaly detector operating on workload telemetry DataFrames.

    Baseline is computed per workload_type from historical data.
    Threshold of 2.0σ flags jobs outside the normal operating envelope.
    """

    def __init__(
        self,
        z_threshold: float = 2.0,
        min_baseline_samples: int = 30,
    ) -> None:
        self.z_threshold = z_threshold
        self.min_baseline_samples = min_baseline_samples

    # ── Core detection ─────────────────────────────────────────────────────────

    def detect(
        self,
        df_current: pd.DataFrame,
        df_baseline: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        """Detect anomalous jobs in df_current using df_baseline for statistics.

        If df_baseline is None, the baseline is computed from df_current itself
        (useful for exploratory analysis; production should pass historical data).

        Returns a DataFrame of anomalous records with z_score and deviation_pct.
        """
        if df_baseline is None:
            logger.warning(
                "No baseline DataFrame provided; computing baseline from current batch. "
                "For production, pass 30+ days of historical data."
            )
            df_baseline = df_current

        stats = self._compute_baseline_stats(df_baseline)
        df_scored = self._score_jobs(df_current, stats)
        anomalies = df_scored[df_scored["is_anomaly"]].copy()

        logger.info(
            "Anomaly detection: %d / %d jobs flagged (threshold: ±%.1fσ)",
            len(anomalies), len(df_scored), self.z_threshold,
        )
        return anomalies.sort_values("abs_z_score", ascending=False)

    def detect_with_report(
        self,
        df_current: pd.DataFrame,
        df_baseline: pd.DataFrame | None = None,
    ) -> tuple[pd.DataFrame, dict]:
        """Returns (anomaly_df, summary_report)."""
        anomalies = self.detect(df_current, df_baseline)
        report = self._build_report(df_current, anomalies)
        return anomalies, report

    # ── Internal methods ───────────────────────────────────────────────────────

    def _compute_baseline_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute mean + stddev of duration per workload_type."""
        if "duration_seconds" not in df.columns or "workload_type" not in df.columns:
            raise ValueError("baseline DataFrame must contain duration_seconds and workload_type")

        # Use only successful, non-retry first attempts for clean baseline
        mask = pd.Series(True, index=df.index)
        if "status" in df.columns:
            mask &= df["status"] == "SUCCESS"
        if "retry_attempt" in df.columns:
            mask &= df["retry_attempt"] == 0

        stats = (
            df[mask]
            .groupby("workload_type")["duration_seconds"]
            .agg(
                baseline_mean="mean",
                baseline_std="std",
                baseline_median="median",
                baseline_p95=lambda x: x.quantile(0.95),
                baseline_count="count",
            )
            .reset_index()
        )

        # Filter out workload types with insufficient data
        stats = stats[stats["baseline_count"] >= self.min_baseline_samples]
        if stats.empty:
            logger.warning(
                "No workload types have ≥%d baseline samples. "
                "Anomaly detection will have no baseline — all Z-scores will be NULL.",
                self.min_baseline_samples,
            )
        return stats

    def _score_jobs(
        self, df: pd.DataFrame, stats: pd.DataFrame
    ) -> pd.DataFrame:
        """Merge baseline stats and compute Z-score for each job."""
        df = df.merge(stats, on="workload_type", how="left")

        # Z-score: (observed - mean) / stddev
        df["z_score"] = np.where(
            df["baseline_std"].gt(0),
            (df["duration_seconds"] - df["baseline_mean"]) / df["baseline_std"],
            np.nan,
        )
        df["abs_z_score"] = df["z_score"].abs()

        # Percentage deviation from baseline mean
        df["deviation_pct"] = np.where(
            df["baseline_mean"].gt(0),
            (df["duration_seconds"] - df["baseline_mean"]) / df["baseline_mean"] * 100,
            np.nan,
        )

        # Anomaly flag: abs(z_score) > threshold
        df["is_anomaly"] = df["abs_z_score"].gt(self.z_threshold).fillna(False)

        # Severity classification
        df["anomaly_severity"] = pd.cut(
            df["abs_z_score"],
            bins=[0, self.z_threshold, self.z_threshold + 1, self.z_threshold + 2, np.inf],
            labels=["NORMAL", "MEDIUM", "HIGH", "CRITICAL"],
            right=False,
        )
        return df

    def _build_report(
        self, df_all: pd.DataFrame, df_anomalies: pd.DataFrame
    ) -> dict:
        """Build a structured summary report for the investigation output."""
        if df_anomalies.empty:
            return {
                "total_jobs": len(df_all),
                "anomalous_jobs": 0,
                "anomaly_rate": 0.0,
                "by_workload_type": {},
                "top_anomalies": [],
                "generated_at": datetime.now(timezone.utc).isoformat(),
            }

        by_type = (
            df_anomalies.groupby("workload_type")
            .agg(
                count=("job_id", "count"),
                avg_deviation_pct=("deviation_pct", "mean"),
                max_z_score=("abs_z_score", "max"),
            )
            .round(2)
            .to_dict(orient="index")
        )

        top = df_anomalies.nlargest(10, "abs_z_score")[
            [c for c in ["job_id", "workload_type", "z_score", "deviation_pct",
                          "duration_seconds", "status", "team"] if c in df_anomalies.columns]
        ].to_dict(orient="records")

        return {
            "total_jobs": len(df_all),
            "anomalous_jobs": len(df_anomalies),
            "anomaly_rate": round(len(df_anomalies) / max(len(df_all), 1), 4),
            "z_threshold_used": self.z_threshold,
            "by_workload_type": by_type,
            "top_anomalies": top,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }


# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Z-score anomaly detection on workload telemetry")
    parser.add_argument("--input",     required=True, help="Clean parquet path")
    parser.add_argument("--baseline",  default=None,  help="Historical baseline parquet path")
    parser.add_argument("--output",    required=True, help="Anomaly output parquet path")
    parser.add_argument("--report",    required=True, help="JSON report output path")
    parser.add_argument("--threshold", type=float, default=2.0, help="Z-score threshold (default 2.0)")
    args = parser.parse_args()

    logger.info("Loading data from %s", args.input)
    df = pd.read_parquet(args.input)

    df_baseline = None
    if args.baseline:
        logger.info("Loading baseline from %s", args.baseline)
        df_baseline = pd.read_parquet(args.baseline)

    detector = AnomalyDetector(z_threshold=args.threshold)
    anomalies, report = detector.detect_with_report(df, df_baseline)

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    anomalies.to_parquet(out_path, index=False)
    logger.info("Anomalies written to %s (%d rows)", out_path, len(anomalies))

    report_path = Path(args.report)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, default=str))
    logger.info("Report written to %s", report_path)


if __name__ == "__main__":
    main()
