"""refresh_dashboards.py — Grafana dashboard refresh (final DAG stage).

Calls the Grafana HTTP API to flush panel query caches so dashboards
reflect the latest data from the completed pipeline run.

Only runs after quality_check passes — never called with bad data.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

import requests

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s  %(message)s")

DASHBOARD_UIDS = [
    "platform-health",
    "anomaly-flags",
    "workload-trends",
    "drill-down-investigation",
]


def refresh_dashboards(run_date: str) -> None:
    grafana_url = os.environ.get("GRAFANA_URL", "http://localhost:3000")
    api_key     = os.environ.get("GRAFANA_API_KEY", "")

    if not api_key:
        logger.warning("GRAFANA_API_KEY not set — skipping dashboard refresh (dry run)")
        for uid in DASHBOARD_UIDS:
            logger.info("  [DRY RUN] Would refresh dashboard: %s", uid)
        return

    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    for uid in DASHBOARD_UIDS:
        url = f"{grafana_url}/api/datasources/proxy/1/api/v1/query"
        try:
            # In production: hit Grafana's Admin API to flush panel cache
            resp = requests.post(
                f"{grafana_url}/api/admin/cache/{}".format(uid),
                headers=headers,
                timeout=10,
            )
            if resp.status_code in (200, 204):
                logger.info("Refreshed dashboard: %s", uid)
            else:
                logger.warning("Dashboard %s returned %d", uid, resp.status_code)
        except requests.RequestException as exc:
            logger.warning("Failed to refresh dashboard %s: %s", uid, exc)

    logger.info("Dashboard refresh complete for run_date=%s", run_date)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    args = parser.parse_args()
    refresh_dashboards(args.date)


if __name__ == "__main__":
    main()
