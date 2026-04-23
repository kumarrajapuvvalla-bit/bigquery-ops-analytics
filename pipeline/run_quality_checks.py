"""run_quality_checks.py — Quality gate task (Step 5c of the DAG pipeline).

Loads the clean DataFrame and runs all quality checks.
Exits with code 1 if any CRITICAL check fails, halting the CI pipeline.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from cleansing.quality_checks import DataQualityChecker

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s  %(message)s")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--date",  required=True)
    args = parser.parse_args()

    df = pd.read_parquet(args.input)
    checker = DataQualityChecker()
    results = checker.run_all(df, stage=f"post_cleanse_{args.date}")

    report = {
        "date": args.date,
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "total_checks": len(results),
        "passed": sum(1 for r in results if r.passed),
        "failed": sum(1 for r in results if not r.passed),
        "checks": [
            {"name": r.name, "passed": r.passed, "severity": r.severity.value,
             "message": r.message}
            for r in results
        ],
    }

    report_dir = Path("data/quality_reports")
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"quality_{args.date}.json"
    report_path.write_text(json.dumps(report, indent=2))
    logger.info("Quality report written to %s", report_path)

    try:
        checker.assert_no_critical_failures(results)
    except RuntimeError as exc:
        logger.critical("Quality gate FAILED: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
