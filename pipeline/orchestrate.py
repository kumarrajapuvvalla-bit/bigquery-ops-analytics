"""orchestrate.py
==============================================================================
Step 5: DAG-style pipeline orchestrator.

Mirrors the Airflow DAG pattern described in the interview:
  "I designed it as a DAG — extraction first, cleansing second, quality check
   third, dashboard refresh last. Each task had a dependency on the previous one
   so if cleansing failed, the dashboard wouldn't refresh with bad data."

This GitLab CI implementation runs each stage as a separate CI job with
explicit needs: dependencies, providing the same DAG guarantee without
requiring a running Airflow scheduler.
==============================================================================
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Callable

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s  %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

# ── Task definitions ───────────────────────────────────────────────────────────

class PipelineTask:
    def __init__(
        self,
        name: str,
        fn: Callable,
        depends_on: list[str] | None = None,
    ) -> None:
        self.name = name
        self.fn = fn
        self.depends_on = depends_on or []
        self.status: str = "pending"   # pending | running | success | failed
        self.started_at: datetime | None = None
        self.completed_at: datetime | None = None
        self.error: str | None = None

    def run(self) -> bool:
        self.status = "running"
        self.started_at = datetime.now(timezone.utc)
        logger.info("[%s] Starting task", self.name)
        try:
            self.fn()
            self.status = "success"
            logger.info("[%s] Task succeeded", self.name)
            return True
        except Exception as exc:
            self.status = "failed"
            self.error = str(exc)
            logger.error("[%s] Task FAILED: %s", self.name, exc)
            return False
        finally:
            self.completed_at = datetime.now(timezone.utc)


class DAGRunner:
    """Executes a list of PipelineTask objects in dependency order.

    If a task fails, all downstream tasks are skipped.
    This ensures the dashboard is never refreshed with bad data.
    """

    def __init__(self, tasks: list[PipelineTask]) -> None:
        self._tasks = {t.name: t for t in tasks}

    def run(self) -> dict[str, str]:
        """Execute all tasks in order. Returns task_name → status map."""
        results: dict[str, str] = {}

        for name, task in self._tasks.items():
            # Check if any upstream dependency failed
            upstream_failed = any(
                self._tasks[dep].status == "failed"
                for dep in task.depends_on
                if dep in self._tasks
            )
            if upstream_failed:
                task.status = "skipped"
                logger.warning("[%s] Skipped — upstream dependency failed", name)
                results[name] = "skipped"
                continue

            success = task.run()
            results[name] = task.status

            if not success:
                logger.error(
                    "Pipeline halted at [%s]. Downstream tasks will be skipped.", name
                )

        self._log_summary(results)
        return results

    def _log_summary(self, results: dict[str, str]) -> None:
        logger.info("=== Pipeline Summary ===")
        for name, status in results.items():
            icon = {"success": "✓", "failed": "✗", "skipped": "⏭"}.get(status, "?")
            duration = ""
            task = self._tasks[name]
            if task.started_at and task.completed_at:
                secs = (task.completed_at - task.started_at).total_seconds()
                duration = f" ({secs:.1f}s)"
            logger.info("  %s %s: %s%s", icon, name, status.upper(), duration)


# ── Task implementations ───────────────────────────────────────────────────────

def make_tasks(run_date: str, data_dir: Path) -> list[PipelineTask]:
    raw_path     = data_dir / "raw"    / f"jobs_{run_date}.parquet"
    clean_path   = data_dir / "clean"  / f"jobs_{run_date}.parquet"
    anomaly_path = data_dir / "anomalies" / f"jobs_{run_date}.parquet"
    report_path  = data_dir / "reports"   / f"anomaly_report_{run_date}.json"
    baseline_path = data_dir / "baselines" / "latest.parquet"

    def task_extract():
        """Extract from BigQuery into local parquet."""
        logger.info("Extracting BigQuery data for %s → %s", run_date, raw_path)
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        _run_script("pipeline/extract.py", ["--date", run_date, "--output", str(raw_path)])

    def task_cleanse():
        """Run pandas cleansing layer."""
        _run_script(
            "cleansing/cleanse_workload_telemetry.py",
            ["--input", str(raw_path), "--output", str(clean_path), "--date", run_date],
        )

    def task_quality_check():
        """Run data quality gates — abort if CRITICAL failures."""
        _run_script(
            "pipeline/run_quality_checks.py",
            ["--input", str(clean_path), "--date", run_date],
        )

    def task_anomaly_scan():
        """Run Z-score anomaly detection."""
        baseline_arg = ["--baseline", str(baseline_path)] if baseline_path.exists() else []
        _run_script(
            "anomaly_detection/detector.py",
            ["--input", str(clean_path), "--output", str(anomaly_path),
             "--report", str(report_path), "--threshold", "2.0"] + baseline_arg,
        )

    def task_dashboard_refresh():
        """Refresh Grafana dashboard data source cache."""
        _run_script("pipeline/refresh_dashboards.py", ["--date", run_date])

    return [
        PipelineTask("extract",           task_extract),
        PipelineTask("cleanse",           task_cleanse,           depends_on=["extract"]),
        PipelineTask("quality_check",     task_quality_check,     depends_on=["cleanse"]),
        PipelineTask("anomaly_scan",      task_anomaly_scan,      depends_on=["quality_check"]),
        PipelineTask("dashboard_refresh", task_dashboard_refresh, depends_on=["quality_check"]),
    ]


def _run_script(script: str, args: list[str]) -> None:
    cmd = [sys.executable, script] + args
    logger.info("Running: %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=False, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"{script} exited with code {result.returncode}")


# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Run the workload analytics DAG pipeline")
    parser.add_argument("--date",     required=True, help="Processing date YYYY-MM-DD")
    parser.add_argument("--data-dir", default="data",  help="Root data directory")
    parser.add_argument(
        "--from-stage", default=None,
        choices=["extract", "cleanse", "quality_check", "anomaly_scan", "dashboard_refresh"],
        help="Resume pipeline from a specific stage",
    )
    args = parser.parse_args()

    tasks = make_tasks(args.date, Path(args.data_dir))
    runner = DAGRunner(tasks)
    results = runner.run()

    failed = [name for name, status in results.items() if status == "failed"]
    if failed:
        logger.error("Pipeline failed at: %s", failed)
        sys.exit(1)

    logger.info("Pipeline completed successfully for %s", args.date)


if __name__ == "__main__":
    main()
