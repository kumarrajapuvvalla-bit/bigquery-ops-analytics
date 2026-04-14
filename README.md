# bigquery-ops-analytics

[![CI](https://github.com/kumarrajapuvvalla-bit/bigquery-ops-analytics/actions/workflows/ci.yml/badge.svg)](https://github.com/kumarrajapuvvalla-bit/bigquery-ops-analytics/actions/workflows/ci.yml)
[![dbt](https://github.com/kumarrajapuvvalla-bit/bigquery-ops-analytics/actions/workflows/dbt-test.yml/badge.svg)](https://github.com/kumarrajapuvvalla-bit/bigquery-ops-analytics/actions/workflows/dbt-test.yml)

> **BigQuery SQL analytics layer for fleet and operational data.** Covers DAG-based orchestration, trend dashboards, anomaly detection, and traceable dataset curation.

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────────┐
│  Raw Sources                                                              │
│  Pub/Sub → Dataflow  │  Cloud Storage (GCS)  │  Kafka Connect            │
└──────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  BigQuery — Raw Layer (ops_raw)                                           │
│  Partitioned by ingestion_date, clustered by source + event_type         │
└──────────────────────────────────────────────────────────────────────────┘
                                    │
                    ┌───────────────┼───────────────┐
                    ▼                               ▼
        ┌───────────────────┐           ┌───────────────────┐
        │ dbt Staging Layer │           │ dbt Staging Layer │
        │ ops_staging       │           │ ops_staging       │
        │ (cleanse + cast)  │           │ (cleanse + cast)  │
        └───────────────────┘           └───────────────────┘
                    │                               │
                    └───────────────┬───────────────┘
                                    ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  dbt Marts Layer (ops_marts)                                              │
│  fleet_health │ cost_attribution │ incident_trends │ slo_compliance      │
└──────────────────────────────────────────────────────────────────────────┘
                                    │
                    ┌───────────────┼───────────────┐
                    ▼                               ▼
        ┌───────────────────┐           ┌───────────────────┐
        │ Looker Studio     │           │ Anomaly Detection │
        │ Trend Dashboards  │           │ + Alerting (GCP)  │
        └───────────────────┘           └───────────────────┘
```

---

## Repository Map

```
bigquery-ops-analytics/
├── schemas/                      # BigQuery DDL — raw + curated table definitions
│   ├── ops_raw/                  # Ingestion-layer partitioned tables
│   └── ops_marts/                # Aggregated mart tables
├── dbt/                          # dbt project (transformation layer)
│   ├── models/
│   │   ├── staging/              # stg_* models — cleanse & cast raw data
│   │   ├── marts/                # fact/dim models consumed by dashboards
│   │   └── metrics/              # dbt metrics layer (semantic layer)
│   ├── tests/                    # custom dbt tests
│   ├── macros/                   # reusable Jinja macros
│   └── dbt_project.yml
├── dags/                         # Airflow DAGs — orchestration + data quality
│   ├── fleet_analytics_dag.py
│   ├── cost_attribution_dag.py
│   └── anomaly_detection_dag.py
├── sql/                          # Ad-hoc & scheduled BigQuery SQL
│   ├── anomaly_detection/
│   ├── trend_analysis/
│   └── data_quality/
├── great_expectations/           # Data quality expectations suites
├── looker/                       # Looker Studio dashboard config
├── terraform/                    # GCP infra — BigQuery datasets, IAM, scheduler
├── .github/workflows/            # CI: SQL lint, dbt compile/test, GE validation
└── docs/                         # Architecture, runbooks, lineage diagrams
```

---

## Key Capabilities

| Capability | Tools | Description |
|---|---|---|
| **Ingestion** | Pub/Sub → Dataflow, GCS → BQ Transfer | Streaming + batch raw event ingestion |
| **Transformation** | dbt Core on BigQuery | Staging → marts → metrics layered modelling |
| **Orchestration** | Cloud Composer (Airflow 2.x) | DAG-driven scheduling with SLA monitoring |
| **Anomaly Detection** | BigQuery ML (ARIMA_PLUS), Z-score SQL | Statistical outlier detection on time-series |
| **Data Quality** | Great Expectations + dbt tests | Schema, freshness, null, referential checks |
| **Dashboards** | Looker Studio, Looker (LookML) | Fleet health, cost, incident trend visualisations |
| **Cost Governance** | FinOps labels + slot reservations | Per-team BigQuery cost attribution |
| **CI/CD** | GitHub Actions + dbt Cloud | SQL lint, dbt compile, GE validation on every PR |
| **IaC** | Terraform + `google` provider | Datasets, IAM, scheduled queries, alerts |

---

## Quick Start

```bash
git clone https://github.com/kumarrajapuvvalla-bit/bigquery-ops-analytics
cd bigquery-ops-analytics

# Install dbt + BigQuery adapter
pip install dbt-bigquery==1.8.0 sqlfluff sqlfluff-templater-dbt

# Configure GCP credentials
export GOOGLE_APPLICATION_CREDENTIALS=~/.config/gcloud/application_default_credentials.json
export GCP_PROJECT_ID=your-project-id

# Run dbt
cd dbt
dbt deps
dbt debug          # verify BQ connection
dbt run            # materialise all models
dbt test           # run schema + custom tests
dbt docs generate && dbt docs serve

# Run Airflow locally (Docker)
cd ..
docker-compose -f docker/docker-compose-airflow.yml up
```

---

## Dataset Lineage

```
ops_raw.fleet_events          →  ops_staging.stg_fleet_events
ops_raw.cost_billing_export   →  ops_staging.stg_cost_billing
ops_raw.incident_logs         →  ops_staging.stg_incident_logs
ops_raw.slo_metrics           →  ops_staging.stg_slo_metrics

ops_staging.*                 →  ops_marts.fleet_health_daily
                              →  ops_marts.cost_attribution
                              →  ops_marts.incident_trend_weekly
                              →  ops_marts.slo_compliance

ops_marts.*                   →  Looker Studio dashboards
                              →  Anomaly detection queries
                              →  Scheduled BigQuery ML jobs
```

---

## DORA Metrics

| Metric | Formula | DORA Elite Target |
|---|---|---|
| Deployment Frequency | deploys / day | ≥ 1/day |
| Change Failure Rate | failed_deploys / total | < 5% |
| MTTR | avg(incident_duration_mins) | < 60 mins |
| Lead Time for Change | avg(commit_to_prod_hours) | < 1 hour |

---

*Part of [kumarrajapuvvalla-bit](https://github.com/kumarrajapuvvalla-bit)'s DevOps portfolio.*
