"""cleanse_workload_telemetry.py
==============================================================================
Step 2 of the AI Workload Analytics pipeline.

Handles the three raw data quality issues documented in production:

  1. Duplicate job records  — retries logged as new entries, not as retries.
     Fix: keep the final retry attempt per canonical_job_id.

  2. NULL resource fields  — monitoring agent misses collection windows.
     Fix: forward-fill within a job window; impute from workload-type median
     where the gap is too large.

  3. Timestamp mismatches — job_metadata and infrastructure_events clocks
     diverge by up to ±30 seconds.
     Fix: normalise both to UTC; apply ±30s tolerance in join queries.

Usage:
    python cleansing/cleanse_workload_telemetry.py \
        --input  data/raw/jobs_2026-04-22.parquet \
        --output data/clean/jobs_2026-04-22.parquet \
        --date   2026-04-22
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s  %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

# ── Constants ──────────────────────────────────────────────────────────────────

TIMESTAMP_COLS = ["submitted_at", "started_at", "completed_at", "ingested_at"]
RESOURCE_NUMERIC_COLS = [
    "cpu_used_cores", "cpu_throttle_pct",
    "memory_used_gb", "gpu_utilisation_pct", "gpu_memory_used_gb",
    "network_egress_gb", "disk_read_gb", "disk_write_gb",
]
REQUIRED_COLS = [
    "job_id", "workload_type", "status",
    "submitted_at", "execution_date",
]
VALID_STATUSES = {"SUCCESS", "FAILURE", "TIMEOUT", "CANCELLED", "RETRYING"}
VALID_WORKLOAD_TYPES = {"model_training", "inference", "data_processing", "fine_tuning"}


# ── Main cleansing class ───────────────────────────────────────────────────────

class WorkloadTelemetryCleaner:
    """Stateful cleansing pipeline for AI workload telemetry.

    Each clean() call returns a new DataFrame and a QualityReport.
    The original DataFrame is never mutated.
    """

    def __init__(self, null_impute_strategy: str = "workload_median") -> None:
        self.null_impute_strategy = null_impute_strategy
        self._report: dict[str, Any] = {}

    # ── Public API ─────────────────────────────────────────────────────────────

    def clean(self, df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
        """Run full cleansing pipeline. Returns (clean_df, quality_report)."""
        self._report = {
            "input_rows": len(df),
            "started_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        logger.info("Starting cleansing — input rows: %d", len(df))

        df = df.copy()
        df = self._validate_schema(df)
        df = self._normalise_timestamps(df)
        df = self._deduplicate_retries(df)
        df = self._impute_nulls(df)
        df = self._validate_enum_fields(df)
        df = self._add_derived_fields(df)

        self._report.update({
            "output_rows": len(df),
            "rows_dropped": self._report["input_rows"] - len(df),
            "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        })
        logger.info(
            "Cleansing complete — output: %d rows, dropped: %d",
            self._report["output_rows"],
            self._report["rows_dropped"],
        )
        return df, self._report

    # ── Step 2a: Schema validation ─────────────────────────────────────────────

    def _validate_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        missing = [c for c in REQUIRED_COLS if c not in df.columns]
        if missing:
            raise ValueError(f"Required columns missing: {missing}")

        # Drop rows where required identity fields are null
        before = len(df)
        df = df.dropna(subset=["job_id", "workload_type", "status", "submitted_at"])
        dropped = before - len(df)
        self._report["dropped_missing_required"] = dropped
        if dropped:
            logger.warning("Dropped %d rows missing required fields", dropped)
        return df

    # ── Step 2b: Timestamp normalisation ──────────────────────────────────────
    # Problem: job_metadata and infrastructure_events are on slightly different
    # clocks. Normalise all timestamps to UTC so downstream joins are accurate.

    def _normalise_timestamps(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in TIMESTAMP_COLS:
            if col not in df.columns:
                continue
            # Parse strings; coerce errors to NaT
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
            # Localise naive timestamps to UTC
            if hasattr(df[col].dtype, "tz") and df[col].dtype.tz is None:
                df[col] = df[col].dt.tz_localize("UTC")
            elif df[col].dtype == "object":
                df[col] = df[col].dt.tz_convert("UTC")

        # Compute duration if missing
        if "duration_seconds" not in df.columns or df["duration_seconds"].isna().any():
            if "started_at" in df.columns and "completed_at" in df.columns:
                df["duration_seconds"] = df["duration_seconds"].combine_first(
                    (df["completed_at"] - df["started_at"]).dt.total_seconds()
                )

        # Flag negative durations (clock skew artefacts)
        negative_dur = df["duration_seconds"].lt(0) & df["duration_seconds"].notna()
        if negative_dur.any():
            logger.warning("Found %d negative durations — setting to NaN", negative_dur.sum())
            df.loc[negative_dur, "duration_seconds"] = np.nan

        self._report["timestamp_skew_corrections"] = int(negative_dur.sum())
        return df

    # ── Step 2c: Deduplication — retries logged as new entries ────────────────
    # Problem: when a job retries, the retry is logged as a completely new record.
    # We keep only the final attempt per canonical_job_id so metrics aren't inflated.

    def _deduplicate_retries(self, df: pd.DataFrame) -> pd.DataFrame:
        before = len(df)

        # canonical_job_id: use parent_job_id if it exists, else job_id
        if "parent_job_id" in df.columns:
            df["canonical_job_id"] = df["parent_job_id"].combine_first(df["job_id"])
        else:
            df["canonical_job_id"] = df["job_id"]

        if "retry_attempt" not in df.columns:
            df["retry_attempt"] = 0

        # Keep the row with the highest retry_attempt per canonical_job_id
        # (ties broken by ingested_at DESC to get the latest record)
        if "ingested_at" in df.columns:
            df = df.sort_values(
                ["canonical_job_id", "retry_attempt", "ingested_at"],
                ascending=[True, False, False],
            )
        else:
            df = df.sort_values(
                ["canonical_job_id", "retry_attempt"],
                ascending=[True, False],
            )

        df = df.drop_duplicates(subset=["canonical_job_id"], keep="first")
        duplicates_removed = before - len(df)
        self._report["duplicate_retry_records_removed"] = duplicates_removed
        logger.info("Deduplication: removed %d retry duplicates", duplicates_removed)
        return df

    # ── Step 2d: NULL imputation for resource fields ───────────────────────────
    # Problem: monitoring agent misses collection windows, leaving NULL in
    # cpu_used_cores, memory_used_gb, etc.
    # Strategy: impute with workload-type median from the current batch.

    def _impute_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        imputed_total = 0
        for col in RESOURCE_NUMERIC_COLS:
            if col not in df.columns:
                continue
            null_mask = df[col].isna()
            null_count = null_mask.sum()
            if null_count == 0:
                continue

            if self.null_impute_strategy == "workload_median":
                # Compute median per workload_type in this batch
                medians = df.groupby("workload_type")[col].transform("median")
                df[col] = df[col].fillna(medians)
            elif self.null_impute_strategy == "zero":
                df[col] = df[col].fillna(0.0)
            else:
                df[col] = df[col].fillna(df[col].median())

            # Any still-null (no workload_type peers) fall back to global median
            df[col] = df[col].fillna(df[col].median())
            imputed_total += null_count
            logger.debug("Imputed %d NULLs in %s", null_count, col)

        self._report["null_values_imputed"] = imputed_total
        logger.info("NULL imputation: filled %d values", imputed_total)
        return df

    # ── Step 2e: Enum validation ───────────────────────────────────────────────

    def _validate_enum_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        before = len(df)
        df = df[df["status"].isin(VALID_STATUSES)]
        df = df[df["workload_type"].isin(VALID_WORKLOAD_TYPES)]
        invalid_dropped = before - len(df)
        if invalid_dropped:
            logger.warning("Dropped %d rows with invalid enum values", invalid_dropped)
        self._report["invalid_enum_rows_dropped"] = invalid_dropped
        return df

    # ── Step 2f: Derived fields ────────────────────────────────────────────────

    def _add_derived_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        # Boolean success flag for aggregation
        df["is_success"] = df["status"] == "SUCCESS"
        df["is_failure"] = df["status"] == "FAILURE"

        # Duration bucket for histogram grouping in Grafana
        def _bucket(s: float | None) -> str:
            if pd.isna(s):
                return "unknown"
            if s < 60:     return "<1m"
            if s < 300:    return "1-5m"
            if s < 900:    return "5-15m"
            if s < 3600:   return "15-60m"
            return ">60m"

        if "duration_seconds" in df.columns:
            df["duration_bucket"] = df["duration_seconds"].apply(_bucket)

        df["cleansed_at"] = datetime.now(timezone.utc).isoformat()
        return df


# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Cleanse AI workload telemetry")
    parser.add_argument("--input",  required=True, help="Input parquet path")
    parser.add_argument("--output", required=True, help="Output parquet path")
    parser.add_argument("--date",   required=True, help="Processing date YYYY-MM-DD")
    parser.add_argument(
        "--impute", default="workload_median",
        choices=["workload_median", "zero", "global_median"],
        help="NULL imputation strategy",
    )
    args = parser.parse_args()

    logger.info("Loading raw data from %s", args.input)
    df_raw = pd.read_parquet(args.input)

    cleaner = WorkloadTelemetryCleaner(null_impute_strategy=args.impute)
    df_clean, report = cleaner.clean(df_raw)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df_clean.to_parquet(output_path, index=False)

    logger.info("Quality report: %s", report)
    logger.info("Clean data written to %s", output_path)


if __name__ == "__main__":
    main()
