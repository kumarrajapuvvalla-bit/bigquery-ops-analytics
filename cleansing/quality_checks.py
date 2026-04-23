"""quality_checks.py
==============================================================================
Pre-pipeline data quality gates.

These checks run BEFORE cleansing and AFTER cleansing.
If any CRITICAL check fails, the pipeline aborts — the dashboard refresh
never fires, so dashboard numbers are always derived from trustworthy data.

This mirrors the interview description:
  "I wrote data quality checks that ran at the start of every pipeline run
   and flagged anomalies in the raw data before they could contaminate
   the output. That way the dashboard numbers were always trustworthy."
==============================================================================
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


class Severity(str, Enum):
    CRITICAL = "CRITICAL"   # Pipeline aborts
    WARNING  = "WARNING"    # Pipeline continues, alert is raised
    INFO     = "INFO"       # Logged only


@dataclass
class CheckResult:
    name: str
    passed: bool
    severity: Severity
    message: str
    detail: dict[str, Any] = field(default_factory=dict)


class DataQualityChecker:
    """Runs a battery of quality checks on a DataFrame.

    Usage:
        checker = DataQualityChecker()
        results = checker.run_all(df, stage="pre_cleanse")
        checker.assert_no_critical_failures(results)
    """

    def run_all(self, df: pd.DataFrame, stage: str = "pre_cleanse") -> list[CheckResult]:
        checks = [
            self._check_not_empty(df),
            self._check_no_duplicate_job_ids(df),
            self._check_required_fields_not_null(df),
            self._check_timestamp_range(df),
            self._check_duration_plausibility(df),
            self._check_status_distribution(df),
            self._check_workload_type_coverage(df),
            self._check_resource_value_bounds(df),
            self._check_null_rate_per_column(df),
            self._check_future_timestamps(df),
        ]
        passed = sum(1 for r in checks if r.passed)
        logger.info("[%s] Quality checks: %d/%d passed", stage, passed, len(checks))
        for r in checks:
            level = logging.WARNING if not r.passed else logging.DEBUG
            logger.log(level, "[%s] %s: %s — %s", stage, r.severity.value, r.name, r.message)
        return checks

    def assert_no_critical_failures(self, results: list[CheckResult]) -> None:
        """Raise if any CRITICAL check failed. Call before advancing pipeline stages."""
        failures = [r for r in results if not r.passed and r.severity == Severity.CRITICAL]
        if failures:
            msgs = "; ".join(f"{r.name}: {r.message}" for r in failures)
            raise RuntimeError(f"CRITICAL quality checks failed — pipeline aborted: {msgs}")

    # ── Individual checks ──────────────────────────────────────────────────────

    def _check_not_empty(self, df: pd.DataFrame) -> CheckResult:
        passed = len(df) > 0
        return CheckResult(
            name="not_empty",
            passed=passed,
            severity=Severity.CRITICAL,
            message=f"{len(df)} rows" if passed else "DataFrame is empty",
        )

    def _check_no_duplicate_job_ids(self, df: pd.DataFrame) -> CheckResult:
        if "job_id" not in df.columns:
            return CheckResult("no_duplicate_job_ids", True, Severity.INFO, "job_id column absent")
        dup_count = df["job_id"].duplicated().sum()
        passed = dup_count == 0
        return CheckResult(
            name="no_duplicate_job_ids",
            passed=passed,
            severity=Severity.WARNING,  # duplication handled by cleansing, not fatal
            message=f"{dup_count} duplicate job_ids detected" if not passed else "No duplicates",
            detail={"duplicate_count": int(dup_count)},
        )

    def _check_required_fields_not_null(self, df: pd.DataFrame) -> CheckResult:
        required = ["job_id", "workload_type", "status", "submitted_at"]
        null_counts = {c: int(df[c].isna().sum()) for c in required if c in df.columns}
        any_null = any(v > 0 for v in null_counts.values())
        return CheckResult(
            name="required_fields_not_null",
            passed=not any_null,
            severity=Severity.CRITICAL,
            message="All required fields populated" if not any_null else f"NULLs in required: {null_counts}",
            detail=null_counts,
        )

    def _check_timestamp_range(self, df: pd.DataFrame) -> CheckResult:
        if "submitted_at" not in df.columns:
            return CheckResult("timestamp_range", True, Severity.INFO, "submitted_at absent")
        ts = pd.to_datetime(df["submitted_at"], utc=True, errors="coerce")
        min_ts, max_ts = ts.min(), ts.max()
        # Data should not be older than 90 days or in the future
        import pandas as pd as _pd
        now = _pd.Timestamp.now(tz="UTC")
        stale = (now - min_ts).days > 90 if pd.notna(min_ts) else False
        future = max_ts > now if pd.notna(max_ts) else False
        passed = not (stale or future)
        return CheckResult(
            name="timestamp_range",
            passed=passed,
            severity=Severity.WARNING,
            message=f"min={min_ts}, max={max_ts}" + (" STALE" if stale else "") + (" FUTURE" if future else ""),
            detail={"min": str(min_ts), "max": str(max_ts), "stale": stale, "future": future},
        )

    def _check_duration_plausibility(self, df: pd.DataFrame) -> CheckResult:
        if "duration_seconds" not in df.columns:
            return CheckResult("duration_plausible", True, Severity.INFO, "duration_seconds absent")
        non_null = df["duration_seconds"].dropna()
        if len(non_null) == 0:
            return CheckResult("duration_plausible", True, Severity.INFO, "No non-null durations")
        # Durations over 48 hours are almost certainly errors
        extreme = (non_null > 172800).sum()
        negative = (non_null < 0).sum()
        passed = extreme == 0 and negative == 0
        return CheckResult(
            name="duration_plausible",
            passed=passed,
            severity=Severity.WARNING,
            message=f"{extreme} durations >48h, {negative} negative durations",
            detail={"extreme_count": int(extreme), "negative_count": int(negative)},
        )

    def _check_status_distribution(self, df: pd.DataFrame) -> CheckResult:
        if "status" not in df.columns:
            return CheckResult("status_distribution", True, Severity.INFO, "status absent")
        failure_rate = (df["status"] == "FAILURE").mean()
        # Alert if >50% failure rate — likely a data quality issue not a real outage
        passed = failure_rate <= 0.5
        return CheckResult(
            name="status_distribution",
            passed=passed,
            severity=Severity.WARNING,
            message=f"Failure rate: {failure_rate:.1%}",
            detail={"failure_rate": float(failure_rate), "counts": df["status"].value_counts().to_dict()},
        )

    def _check_workload_type_coverage(self, df: pd.DataFrame) -> CheckResult:
        if "workload_type" not in df.columns:
            return CheckResult("workload_type_coverage", True, Severity.INFO, "workload_type absent")
        expected = {"model_training", "inference", "data_processing"}
        present = set(df["workload_type"].dropna().unique())
        missing = expected - present
        passed = len(missing) == 0
        return CheckResult(
            name="workload_type_coverage",
            passed=passed,
            severity=Severity.WARNING,
            message=f"Missing workload types: {missing}" if missing else f"All types present: {present}",
            detail={"missing": list(missing), "present": list(present)},
        )

    def _check_resource_value_bounds(self, df: pd.DataFrame) -> CheckResult:
        issues = []
        bounds = {
            "cpu_used_cores": (0, 512),
            "memory_used_gb": (0, 4096),
            "gpu_utilisation_pct": (0, 100),
        }
        for col, (lo, hi) in bounds.items():
            if col not in df.columns:
                continue
            out = df[col].dropna()
            bad = ((out < lo) | (out > hi)).sum()
            if bad:
                issues.append(f"{col}: {bad} out-of-bounds")
        passed = len(issues) == 0
        return CheckResult(
            name="resource_value_bounds",
            passed=passed,
            severity=Severity.WARNING,
            message="; ".join(issues) if issues else "All resource values within bounds",
        )

    def _check_null_rate_per_column(
        self, df: pd.DataFrame, threshold: float = 0.3
    ) -> CheckResult:
        high_null = {
            col: float(df[col].isna().mean())
            for col in df.columns
            if df[col].isna().mean() > threshold
        }
        passed = len(high_null) == 0
        return CheckResult(
            name="null_rate_per_column",
            passed=passed,
            severity=Severity.WARNING,
            message=f"Columns >30% null: {high_null}" if high_null else "Null rates acceptable",
            detail=high_null,
        )

    def _check_future_timestamps(self, df: pd.DataFrame) -> CheckResult:
        import pandas as _pd
        now = _pd.Timestamp.now(tz="UTC")
        future_count = 0
        for col in ["submitted_at", "started_at", "completed_at"]:
            if col not in df.columns:
                continue
            ts = _pd.to_datetime(df[col], utc=True, errors="coerce")
            future_count += int((ts > now).sum())
        passed = future_count == 0
        return CheckResult(
            name="no_future_timestamps",
            passed=passed,
            severity=Severity.WARNING,
            message=f"{future_count} future timestamps detected" if not passed else "No future timestamps",
            detail={"future_count": future_count},
        )
