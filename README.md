# ai-workload-ops-analytics

[![Pipeline](https://img.shields.io/badge/pipeline-GitLab%20CI%2FCD-FC6D26?logo=gitlab)](https://gitlab.com)
[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org)
[![BigQuery](https://img.shields.io/badge/data--store-BigQuery-4285F4?logo=google-cloud)](https://cloud.google.com/bigquery)
[![Grafana](https://img.shields.io/badge/dashboards-Grafana-F46800?logo=grafana)](https://grafana.com)
[![Airflow Pattern](https://img.shields.io/badge/orchestration-DAG%20pattern-017CEE?logo=apache-airflow)](https://airflow.apache.org)

> **Analytics layer for enterprise AI workload telemetry.**  
> Hundreds of model training jobs, inference pipelines, and data processing runs generate continuous operational metrics into BigQuery. This platform makes those patterns visible — job health, anomaly detection, workload-type trends, and failure investigation — all surfaced through structured dashboards and a CI-orchestrated DAG pipeline.

---

## The Problem This Solves

Running enterprise AI workloads at scale creates a visibility gap. Individual job failures are logged, but:

- No cross-workload pattern recognition across hundreds of concurrent jobs
- No trend detection — teams can't see that a workload *class* is degrading over time
- No anomaly surfacing — jobs running 40–60% longer than baseline go unnoticed
- No structured investigation path — on-call engineers have raw logs, not answers

This platform closes that gap. It sits on top of BigQuery telemetry that every workload pushes periodically — execution metadata, resource consumption, error logs, performance timestamps — and turns it into actionable operational intelligence.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│  AI WORKLOADS (GCP + AWS)                                                │
│  Model Training  │  Inference Pipelines  │  Data Processing Runs        │
│  Each job pushes metrics periodically → BigQuery tables                  │
└──────────────────────────────┬───────────────────────────────────────────┘
                               │  Structured telemetry
                               ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  STEP 1 — BigQuery Data Source                                           │
│  job_metadata  │  resource_allocation_logs  │  infrastructure_events    │
│  Joined on job_id + timestamp using CTEs + window functions              │
│  Version-controlled SQL queries in sql/queries/                          │
└──────────────────────────────┬───────────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  STEP 2 — Python Cleansing Layer (pandas)                                │
│  Deduplication (retry records)  │  NULL imputation (missed collection)   │
│  Timestamp normalisation (clock skew)  │  Pre-run quality checks        │
│  cleansing/cleanse_workload_telemetry.py                                 │
└──────────────────────────────┬───────────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  STEP 3 — Anomaly Detection                                              │
│  Z-score deviation from workload-type baseline (±2σ threshold)           │
│  Per-workload-class rolling mean + stddev                                │
│  anomaly_detection/detector.py                                           │
└──────────────────────────────┬───────────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  STEP 4 — Grafana Dashboards (4 views)                                   │
│  Platform Health  │  Anomaly Flags  │  Workload Trends  │  Drill-Down   │
│  dashboards/grafana/                                                     │
└──────────────────────────────┬───────────────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  STEP 5 — GitLab CI/CD Pipeline (DAG Orchestration)                     │
│  extract → cleanse → quality_check → anomaly_scan → dashboard_refresh   │
│  Each stage depends on the prior — bad data never reaches dashboards     │
│  .gitlab-ci.yml                                                          │
└──────────────────────────────┴───────────────────────────────────────────┘
```

---

## Repository Map

```
ai-workload-ops-analytics/
├── sql/
│   ├── queries/
│   │   ├── extract_job_metadata.sql          # Step 1 — core extraction CTE
│   │   ├── resource_utilisation.sql          # Step 1 — resource allocation join
│   │   └── infra_event_correlation.sql       # Step 1 — infrastructure event join
│   └── schemas/
│       ├── job_metadata.sql                  # DDL for raw telemetry table
│       ├── resource_allocation_logs.sql      # DDL for resource table
│       └── infrastructure_events.sql         # DDL for infra event table
├── cleansing/
│   ├── cleanse_workload_telemetry.py         # Step 2 — full pandas cleansing layer
│   └── quality_checks.py                    # Step 2 — pre-pipeline data quality gates
├── anomaly_detection/
│   ├── detector.py                          # Step 3 — Z-score anomaly engine
│   └── baseline_calculator.py              # Step 3 — rolling baseline per workload type
├── dashboards/
│   └── grafana/
│       ├── platform_health.json             # Step 4 — job success/failure/duration
│       ├── anomaly_flags.json               # Step 4 — deviation alerts
│       ├── workload_trends.json             # Step 4 — per-class trend analysis
│       └── drill_down.json                 # Step 4 — investigation view
├── pipeline/
│   ├── extract.py                           # Step 5 — BigQuery extraction task
│   ├── orchestrate.py                       # Step 5 — DAG-style task runner
│   └── refresh_dashboards.py               # Step 5 — Grafana cache refresh
├── reports/
│   └── resource_contention_finding.md      # Step 6 — structured investigation report
├── tests/
│   ├── test_cleansing.py
│   ├── test_anomaly_detector.py
│   └── test_quality_checks.py
├── .gitlab-ci.yml                           # GitLab CI/CD DAG pipeline
├── requirements.txt
└── docs/
    ├── ARCHITECTURE.md
    └── INVESTIGATION_RUNBOOK.md
```

---

## Key Findings

The platform identified that a class of **model training jobs** was consistently running **40–60% longer than baseline** during a specific daily window. Anomaly detection flagged the deviation; SQL correlation traced it to a resource contention pattern — a competing workload type was consuming the same GCP compute tier during that window. The finding was packaged as a structured report (SQL + clean dataset + Grafana visualisation + recommendation), presented to platform leadership, and resolved by rescheduling the competing workload. Training times normalised within one week.

See `reports/resource_contention_finding.md` for the full investigation structure.

---

## Quick Start

```bash
git clone https://github.com/kumarrajapuvvalla-bit/ai-workload-ops-analytics
cd ai-workload-ops-analytics
pip install -r requirements.txt

# Set GCP credentials
export GOOGLE_APPLICATION_CREDENTIALS=~/.config/gcloud/application_default_credentials.json
export GCP_PROJECT_ID=your-project-id
export BQ_DATASET=ai_workload_telemetry

# Run the full pipeline locally (mirrors the GitLab CI DAG)
python pipeline/orchestrate.py --date 2026-04-22

# Run individual steps
python pipeline/extract.py --date 2026-04-22
python cleansing/cleanse_workload_telemetry.py --input data/raw/ --output data/clean/
python anomaly_detection/detector.py --input data/clean/ --threshold 2.0
```

---

## Dashboard Views

| Dashboard | Engineering Question | Key Metrics |
|---|---|---|
| Platform Health | Is the platform healthy right now? | Job success rate, failure rate, avg duration over time |
| Anomaly Flags | Which jobs are behaving abnormally? | Jobs >2σ from workload-type baseline, flagged by class |
| Workload Trends | Which job categories are degrading? | Duration trend by workload type, failure rate WoW |
| Drill-Down | Why did this specific job fail? | Filter by job_id or time window, event correlation |

---

## Pipeline DAG

```
extract_from_bigquery
        │
        ▼
cleanse_and_deduplicate
        │
        ▼
run_quality_checks  ──── FAIL → abort (dashboard not refreshed)
        │
        ▼
run_anomaly_scan
        │
        ▼
refresh_grafana_dashboards
```

Each stage in `.gitlab-ci.yml` depends on the previous. If `quality_check` fails, the pipeline halts — dashboard data is never contaminated by bad upstream records.

---

*Part of [kumarrajapuvvalla-bit](https://github.com/kumarrajapuvvalla-bit)'s DevOps & Platform Engineering portfolio.*
