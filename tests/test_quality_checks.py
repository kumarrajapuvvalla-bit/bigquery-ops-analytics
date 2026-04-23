"""Tests for data quality checks."""

import pandas as pd
import pytest

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from cleansing.quality_checks import DataQualityChecker, Severity


def make_clean_df(n=100):
    import numpy as np
    rng = np.random.default_rng(42)
    return pd.DataFrame({
        "job_id":        [f"job_{i:04d}" for i in range(n)],
        "workload_type": rng.choice(["model_training", "inference", "data_processing"], n),
        "status":        rng.choice(["SUCCESS", "FAILURE"], n, p=[0.9, 0.1]),
        "submitted_at":  ["2026-04-22T08:00:00+00:00"] * n,
        "duration_seconds": rng.uniform(60, 7200, n),
        "cpu_used_cores": rng.uniform(1, 16, n),
        "memory_used_gb": rng.uniform(2, 64, n),
        "gpu_utilisation_pct": rng.uniform(0, 100, n),
    })


class TestQualityChecker:
    def test_clean_df_passes_all_checks(self):
        checker = DataQualityChecker()
        results = checker.run_all(make_clean_df())
        critical_failures = [r for r in results if not r.passed and r.severity == Severity.CRITICAL]
        assert len(critical_failures) == 0

    def test_empty_df_fails_critical(self):
        checker = DataQualityChecker()
        results = checker.run_all(pd.DataFrame())
        critical_failures = [r for r in results if not r.passed and r.severity == Severity.CRITICAL]
        assert any(r.name == "not_empty" for r in critical_failures)

    def test_assert_raises_on_critical_failure(self):
        checker = DataQualityChecker()
        results = checker.run_all(pd.DataFrame())
        with pytest.raises(RuntimeError, match="CRITICAL"):
            checker.assert_no_critical_failures(results)

    def test_missing_required_field_is_critical(self):
        df = make_clean_df()
        df = df.drop(columns=["job_id"])
        checker = DataQualityChecker()
        results = checker.run_all(df)
        # Missing job_id should trigger required_fields_not_null or not_empty
        critical = [r for r in results if not r.passed and r.severity == Severity.CRITICAL]
        # job_id is required — check that something critical failed
        assert len(critical) > 0
