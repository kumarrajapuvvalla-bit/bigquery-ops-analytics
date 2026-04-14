# bigquery-ops-analytics

[![CI](https://github.com/kumarrajapuvvalla-bit/bigquery-ops-analytics/actions/workflows/ci.yml/badge.svg)](https://github.com/kumarrajapuvvalla-bit/bigquery-ops-analytics/actions/workflows/ci.yml)
[![dbt](https://img.shields.io/badge/dbt-1.8-orange)](https://docs.getdbt.com)
[![BigQuery](https://img.shields.io/badge/BigQuery-analytics-blue)](https://cloud.google.com/bigquery)
[![Airflow](https://img.shields.io/badge/Airflow-2.9-green)](https://airflow.apache.org)
[![Python](https://img.shields.io/badge/Python-3.12-yellow)](https://python.org)

A **production-grade BigQuery analytics platform** for fleet and operational data.
Covers end-to-end: raw ingestion → dbt transformation → mart layer → trend dashboards → anomaly detection → alerting.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Data Sources                                  │
│  Fleet Telemetry │ Flight Ops │ ECS/EKS Metrics │ Cost Events   │
└──────────┬──────────────┬──────────────┬──────────────┬─────────┘
           │              │              │              │
           ▼              ▼              ▼              ▼
┌─────────────────────────────────────────────────────────────────┐
│              Ingestion Layer (Python / Pub/Sub)                  │
│        dataflow_ingest.py  │  pubsub_subscriber.py               │
└─────────────────────────────┬───────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                  BigQuery Raw Layer                              │
│   ops_raw.fleet_events │ ops_raw.flight_routes                   │
│   ops_raw.infra_metrics │ ops_raw.cost_events                    │
└─────────────────────────────┬───────────────────────────────────┘
                              │  dbt run
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│              dbt Transformation Layers                           │
│  staging/  →  intermediate/  →  marts/                          │
│  (clean)      (business logic)   (aggregated KPIs)              │
└─────────────────────────────┬───────────────────────────────────┘
                              │
           ┌──────────────────┼──────────────────┐
           ▼                  ▼                  ▼
    ┌─────────────┐   ┌─────────────┐   ┌─────────────────┐
    │ Trend Dash  │   │  Anomaly    │   │  Cost / FinOps  │
    │ (Looker)    │   │  Detection  │   │  Reports        │
    └─────────────┘   └─────────────┘   └─────────────────┘
                              │
                              ▼
                    ┌─────────────────┐
                    │  Alerting       │
                    │  (PagerDuty /   │
                    │   Slack)        │
                    └─────────────────┘
```

---

## Repository Layout

```
bigquery-ops-analytics/
├── dbt/                          # dbt project (transformation layer)
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── staging/              # stg_* — light cleaning, type casting
│   │   ├── intermediate/         # int_* — joins, business rules
│   │   └── marts/                # fleet_*, ops_*, cost_* KPI tables
│   ├── macros/                   # reusable Jinja macros
│   ├── tests/                    # singular + generic dbt tests
│   └── analyses/                 # ad-hoc explorations
├── dags/                         # Airflow DAGs (orchestration)
│   ├── fleet_analytics_dag.py    # daily fleet KPI pipeline
│   ├── anomaly_detection_dag.py  # hourly anomaly scan
│   └── cost_rollup_dag.py        # nightly cost aggregation
├── ingestion/                    # Python data ingestion utilities
│   ├── dataflow_ingest.py        # Apache Beam / Dataflow pipeline
│   └── pubsub_subscriber.py      # Pub/Sub → BigQuery streaming
├── analytics/                    # Python analytics utilities
│   ├── anomaly_detector.py       # Z-score + IQR anomaly detection
│   ├── trend_calculator.py       # rolling averages + forecasting
│   └── dataset_curator.py        # traceable dataset curation
├── sql/                          # Raw BigQuery SQL (DDL + ad-hoc)
│   ├── schemas/                  # table/view definitions
│   └── queries/                  # reusable analytical queries
├── tests/                        # pytest test suite
│   ├── test_anomaly_detector.py
│   ├── test_trend_calculator.py
│   └── test_dataset_curator.py
├── .github/workflows/
│   └── ci.yml                    # dbt compile + sqlfluff + pytest
├── docs/
│   ├── ARCHITECTURE.md
│   └── RUNBOOK.md
├── pyproject.toml
└── requirements.txt
```

---

## Quick Start

```bash
# Clone
git clone https://github.com/kumarrajapuvvalla-bit/bigquery-ops-analytics
cd bigquery-ops-analytics

# Python environment
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Configure GCP credentials
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
export GCP_PROJECT=your-gcp-project
export BQ_DATASET=ops_analytics

# dbt
cd dbt
dbt deps
dbt compile          # validate SQL without running
dbt run              # materialize all models
dbt test             # run data quality tests
dbt docs generate && dbt docs serve  # browse lineage

# Run Airflow locally
export AIRFLOW_HOME=$(pwd)/airflow
airflow standalone   # starts scheduler + webserver

# Run pytest
pytest tests/ -v
```

---

## Data Models

### Staging Layer (`ops_analytics.stg_*`)

| Model | Source | Description |
|-------|--------|-------------|
| `stg_fleet_events` | `ops_raw.fleet_events` | Cleaned fleet telemetry events |
| `stg_flight_routes` | `ops_raw.flight_routes` | Normalised flight route records |
| `stg_infra_metrics` | `ops_raw.infra_metrics` | EKS/ECS resource utilisation |
| `stg_cost_events` | `ops_raw.cost_events` | Cloud cost allocation events |

### Intermediate Layer (`ops_analytics.int_*`)

| Model | Description |
|-------|-------------|
| `int_fleet_health_scores` | Weighted fleet readiness score per cluster |
| `int_route_performance` | On-time, delay, cancellation metrics per route |
| `int_infra_cost_allocation` | Cost split by team, service, environment |
| `int_anomaly_candidates` | Events flagged by Z-score / IQR detectors |

### Mart Layer (`ops_analytics.*`)

| Model | Grain | Description |
|-------|-------|-------------|
| `fleet_readiness_daily` | cluster × day | SLO compliance, readiness score trend |
| `flight_ops_summary` | route × day | OTP, CSAT, cancellation rate |
| `infra_cost_by_team` | team × day | FinOps allocation and variance |
| `anomaly_log` | event × hour | Timestamped anomaly detections |
| `ops_kpi_dashboard` | day | Rollup KPI table powering Looker dashboard |

---

## CI / CD

Every push and pull request triggers:

1. **`dbt compile`** — validates all SQL without BigQuery access
2. **`sqlfluff lint`** — enforces BigQuery SQL style (Jinja-aware)
3. **`pytest`** — unit tests for Python analytics modules
4. **`dbt test` (on merge)** — schema + data quality tests

---

## Key Technologies

| Layer | Technology |
|-------|------------|
| Warehouse | Google BigQuery |
| Transformation | dbt-bigquery 1.8 |
| Orchestration | Apache Airflow 2.9 (Cloud Composer) |
| Ingestion | Apache Beam / Dataflow, Pub/Sub |
| Anomaly Detection | Python (scipy, pandas) |
| SQL Linting | SQLFluff (BigQuery dialect) |
| Testing | pytest, dbt tests |
| CI/CD | GitHub Actions |
| Dashboards | Looker / Looker Studio |
| Alerting | PagerDuty, Slack webhooks |

---

*Part of [kumarrajapuvvalla-bit](https://github.com/kumarrajapuvvalla-bit)'s DevOps & Platform Engineering portfolio.*
