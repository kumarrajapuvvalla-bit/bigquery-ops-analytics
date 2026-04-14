"""
dataflow_ingest.py
──────────────────
Apache Beam pipeline that reads flight events from a Pub/Sub topic
and writes them to BigQuery (ops_raw.fleet_events).

Deploy to Cloud Dataflow:

    python dataflow_ingest.py \\
        --runner DataflowRunner \\
        --project $GCP_PROJECT \\
        --region eu-west2 \\
        --temp_location gs://$GCS_BUCKET/tmp \\
        --streaming
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

log = logging.getLogger(__name__)

GCP_PROJECT    = os.getenv("GCP_PROJECT", "my-gcp-project")
PUBSUB_TOPIC   = os.getenv("PUBSUB_TOPIC",  f"projects/{GCP_PROJECT}/topics/fleet-events")
BQ_TABLE       = os.getenv("BQ_TABLE",       f"{GCP_PROJECT}:ops_raw.fleet_events")

BQ_SCHEMA = {
    "fields": [
        {"name": "event_id",       "type": "STRING",    "mode": "REQUIRED"},
        {"name": "cluster_name",   "type": "STRING",    "mode": "REQUIRED"},
        {"name": "node_group",     "type": "STRING",    "mode": "NULLABLE"},
        {"name": "environment",    "type": "STRING",    "mode": "REQUIRED"},
        {"name": "event_type",     "type": "STRING",    "mode": "REQUIRED"},
        {"name": "service_name",   "type": "STRING",    "mode": "NULLABLE"},
        {"name": "aws_region",     "type": "STRING",    "mode": "NULLABLE"},
        {"name": "readiness_score","type": "FLOAT64",   "mode": "NULLABLE"},
        {"name": "node_count",     "type": "INT64",     "mode": "NULLABLE"},
        {"name": "healthy_nodes",  "type": "INT64",     "mode": "NULLABLE"},
        {"name": "desired_tasks",  "type": "INT64",     "mode": "NULLABLE"},
        {"name": "running_tasks",  "type": "INT64",     "mode": "NULLABLE"},
        {"name": "event_ts",       "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}


class ParseMessage(beam.DoFn):
    """Parses a Pub/Sub message (JSON bytes) into a BigQuery row dict."""

    def process(self, message: bytes):
        try:
            row: dict[str, Any] = json.loads(message.decode("utf-8"))
            # Basic validation
            if not row.get("event_id") or not row.get("event_ts"):
                log.warning("Skipping invalid message: %s", message[:200])
                return
            yield row
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            log.error("Failed to parse message: %s — %s", message[:200], exc)


def run():
    opts = PipelineOptions()
    opts.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=opts) as pipeline:
        (
            pipeline
            | "ReadFromPubSub"  >> ReadFromPubSub(topic=PUBSUB_TOPIC)
            | "ParseMessages"   >> beam.ParDo(ParseMessage())
            | "WriteToBigQuery" >> WriteToBigQuery(
                table=BQ_TABLE,
                schema=BQ_SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )


if __name__ == "__main__":
    run()
