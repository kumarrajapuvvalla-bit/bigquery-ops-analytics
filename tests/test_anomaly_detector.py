"""Tests for the Z-score anomaly detector."""

import numpy as np
import pandas as pd
import pytest

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from anomaly_detection.detector import AnomalyDetector


def make_baseline_df(n=100, mean=3600, std=300):
    rng = np.random.default_rng(42)
    return pd.DataFrame({
        "job_id":          [f"base_{i}" for i in range(n)],
        "workload_type":   ["model_training"] * n,
        "status":          ["SUCCESS"] * n,
        "retry_attempt":   [0] * n,
        "duration_seconds": rng.normal(mean, std, n).clip(0),
    })


def make_current_df(n=20, normal_mean=3600, normal_std=200, anomaly_multiplier=1.7):
    rng = np.random.default_rng(99)
    durations = list(rng.normal(normal_mean, normal_std, n - 3).clip(0))
    # 3 anomalous jobs — 70% longer than baseline (matching the interview finding)
    durations += [normal_mean * anomaly_multiplier] * 3
    return pd.DataFrame({
        "job_id":          [f"cur_{i}" for i in range(n)],
        "workload_type":   ["model_training"] * n,
        "status":          ["SUCCESS"] * n,
        "retry_attempt":   [0] * n,
        "duration_seconds": durations,
    })


class TestAnomalyDetector:
    def test_flags_anomalous_jobs(self):
        detector = AnomalyDetector(z_threshold=2.0)
        df_base = make_baseline_df()
        df_curr = make_current_df()
        anomalies = detector.detect(df_curr, df_base)
        # The 3 injected anomalous jobs should all be flagged
        assert len(anomalies) >= 3

    def test_no_false_positives_on_normal_data(self):
        rng = np.random.default_rng(0)
        df = pd.DataFrame({
            "job_id": [f"j{i}" for i in range(50)],
            "workload_type": ["inference"] * 50,
            "status": ["SUCCESS"] * 50,
            "retry_attempt": [0] * 50,
            "duration_seconds": rng.normal(1800, 100, 50).clip(0),
        })
        detector = AnomalyDetector(z_threshold=2.0)
        anomalies = detector.detect(df, df)
        # With normal distribution data, expect very few (if any) flags
        assert len(anomalies) <= 3

    def test_report_contains_expected_keys(self):
        detector = AnomalyDetector()
        df_base = make_baseline_df()
        df_curr = make_current_df()
        _, report = detector.detect_with_report(df_curr, df_base)
        for key in ["total_jobs", "anomalous_jobs", "anomaly_rate", "by_workload_type"]:
            assert key in report

    def test_deviation_pct_matches_interview_finding(self):
        """The interview finding was 40-60% longer than baseline.
        Verify the detector correctly computes deviation_pct."""
        detector = AnomalyDetector(z_threshold=2.0, min_baseline_samples=5)
        df_base = make_baseline_df(n=50, mean=3600, std=200)
        # Anomalous job: 50% longer than 3600s baseline = 5400s
        df_curr = pd.DataFrame({
            "job_id": ["anomaly_job"],
            "workload_type": ["model_training"],
            "status": ["SUCCESS"],
            "retry_attempt": [0],
            "duration_seconds": [5400.0],
        })
        anomalies = detector.detect(df_curr, df_base)
        assert len(anomalies) == 1
        pct = anomalies.iloc[0]["deviation_pct"]
        assert 40 <= pct <= 60, f"Expected 40-60% deviation, got {pct:.1f}%"
