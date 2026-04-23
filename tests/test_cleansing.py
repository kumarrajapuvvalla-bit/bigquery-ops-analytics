"""Tests for the cleansing pipeline — deduplication, NULL imputation, timestamps."""

import numpy as np
import pandas as pd
import pytest

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from cleansing.cleanse_workload_telemetry import WorkloadTelemetryCleaner


def make_df(**overrides):
    base = {
        "job_id":        ["job_001", "job_001", "job_002", "job_003"],
        "workload_type": ["model_training", "model_training", "inference", "data_processing"],
        "status":        ["SUCCESS", "SUCCESS", "FAILURE", "SUCCESS"],
        "submitted_at":  ["2026-04-22T08:00:00Z"] * 4,
        "started_at":    ["2026-04-22T08:01:00Z"] * 4,
        "completed_at":  ["2026-04-22T09:01:00Z"] * 4,
        "execution_date":["2026-04-22"] * 4,
        "retry_attempt": [0, 1, 0, 0],
        "parent_job_id": [None, "job_001", None, None],
        "cpu_used_cores":[4.0, 4.2, None, 2.1],
        "memory_used_gb":[16.0, 16.5, None, 8.0],
        "gpu_utilisation_pct": [85.0, 87.0, None, None],
    }
    base.update(overrides)
    return pd.DataFrame(base)


class TestDeduplication:
    def test_keeps_highest_retry(self):
        df = make_df()
        cleaner = WorkloadTelemetryCleaner()
        df_clean, report = cleaner.clean(df)
        # job_001 has two rows (retry_attempt 0 and 1) — should keep only 1
        assert df_clean["canonical_job_id"].duplicated().sum() == 0

    def test_dedup_count_in_report(self):
        df = make_df()
        _, report = WorkloadTelemetryCleaner().clean(df)
        assert report["duplicate_retry_records_removed"] == 1


class TestNullImputation:
    def test_no_nulls_after_cleansing(self):
        df = make_df()
        df_clean, _ = WorkloadTelemetryCleaner().clean(df)
        # Resource columns should have no NULLs after imputation
        for col in ["cpu_used_cores", "memory_used_gb"]:
            assert df_clean[col].isna().sum() == 0, f"{col} still has NULLs"

    def test_imputed_count_positive(self):
        df = make_df()
        _, report = WorkloadTelemetryCleaner().clean(df)
        assert report["null_values_imputed"] > 0


class TestTimestampNormalisation:
    def test_timestamps_are_utc(self):
        df = make_df()
        df_clean, _ = WorkloadTelemetryCleaner().clean(df)
        for col in ["submitted_at", "started_at", "completed_at"]:
            if col in df_clean.columns:
                assert str(df_clean[col].dtype) in ("datetime64[ns, UTC]", "datetime64[us, UTC]"), col

    def test_duration_computed(self):
        df = make_df()
        df_clean, _ = WorkloadTelemetryCleaner().clean(df)
        assert "duration_seconds" in df_clean.columns
        assert df_clean["duration_seconds"].notna().any()


class TestEnumValidation:
    def test_invalid_status_dropped(self):
        df = make_df(status=["SUCCESS", "SUCCESS", "INVALID_STATUS", "SUCCESS"])
        df_clean, _ = WorkloadTelemetryCleaner().clean(df)
        assert "INVALID_STATUS" not in df_clean["status"].values

    def test_invalid_workload_type_dropped(self):
        df = make_df(workload_type=["model_training", "model_training", "unknown_type", "inference"])
        df_clean, _ = WorkloadTelemetryCleaner().clean(df)
        assert "unknown_type" not in df_clean["workload_type"].values
