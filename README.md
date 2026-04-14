# bigquery-ops-analytics

[![CI](https://github.com/kumarrajapuvvalla-bit/bigquery-ops-analytics/actions/workflows/ci.yml/badge.svg)](https://github.com/kumarrajapuvvalla-bit/bigquery-ops-analytics/actions/workflows/ci.yml)
[![dbt](https://github.com/kumarrajapuvvalla-bit/bigquery-ops-analytics/actions/workflows/dbt-test.yml/badge.svg)](https://github.com/kumarrajapuvvalla-bit/bigquery-ops-analytics/actions/workflows/dbt-test.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/downloads/)
[![dbt](https://img.shields.io/badge/dbt-1.8-orange.svg)](https://docs.getdbt.com)
[![BigQuery](https://img.shields.io/badge/BigQuery-SQL-4285F4?logo=google-cloud)](https://cloud.google.com/bigquery)
[![Airflow](https://img.shields.io/badge/Airflow-2.x-017CEE?logo=apache-airflow)](https://airflow.apache.org)
[![Terraform](https://img.shields.io/badge/Terraform-1.7%2B-7B42BC?logo=terraform)](https://www.terraform.io)

> **Production-grade BigQuery analytics platform for fleet and operational intelligence.**  
> End-to-end coverage: streaming ingestion → dbt transformation → anomaly detection → Looker dashboards → Airflow orchestration — all governed by data contracts, GE quality gates, and CI-enforced SQL linting.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  INGESTION LAYER                                                             │
│  Pub/Sub → Dataflow  │  GCS → BQ Transfer  │  Prometheus remote-write      │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  RAW LAYER  (ops_raw — BigQuery)                                            │
│  fleet_events   │  slo_metrics   │  cost_billing_export   │  infra_metrics  │
│  Partitioned by date · Clustered by source+type · require_partition_filter  │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │  dbt
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  STAGING LAYER  (ops_staging — dbt views)                                   │
│  stg_fleet_events  │  stg_slo_metrics  │  stg_cost_billing  │  stg_infra   │
│  Cleanse · Cast · Deduplicate · Validate against sources.yml freshness      │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │  dbt
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  INTERMEDIATE LAYER  (dbt ephemeral / views)                                │
│  int_fleet_health_scores  │  int_anomaly_candidates  │  int_route_perf      │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │  dbt incremental
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  MARTS LAYER  (ops_marts — dbt incremental tables)                          │
│  fleet_health_daily │ slo_compliance │ cost_attribution │ anomaly_log       │
│  ops_kpi_dashboard  │ infra_cost_by_team │ fleet_readiness_daily            │
└──────────┬──────────────────────┬──────────────────────┬────────────────────┘
           │                      │                      │
           ▼                      ▼                      ▼
  ┌──────────────┐      ┌──────────────────┐    ┌─────────────────────┐
  │ Looker /     │      │ Anomaly Detection │    │ Cloud Monitoring    │
  │ LookML       │      │ Z-score + ARIMA+  │    │ Alerts → PagerDuty │
  │ Dashboards   │      │ (BQ scheduled SQL)│    │ / Slack             │
  └──────────────┘      └──────────────────┘    └─────────────────────┘
```

---

## Repository Map

```
bigquery-ops-analytics/
├── schemas/                       # BigQuery DDL — raw + mart table definitions
│   ├── ops_raw/                   # fleet_events, slo_metrics, cost_billing_export
│   └── ops_marts/                 # fleet_health_daily
├── dbt/                           # dbt Core project
│   ├── models/
│   │   ├── staging/               # stg_* views: cleanse, cast, deduplicate
│   │   ├── intermediate/          # int_* ephemeral/views: reusable business logic
│   │   ├── marts/                 # Incremental fact tables for dashboards
│   │   └── metrics/               # dbt semantic layer metric definitions
│   ├── macros/                    # zscore(), dora_tier(), safe_divide(), etc.
│   ├── tests/                     # Custom SQL data tests
│   └── dbt_project.yml
├── dags/                          # Apache Airflow 2.x DAGs (Cloud Composer)
│   ├── fleet_analytics_dag.py     # Daily 01:00 UTC: freshness → dbt → anomalies
│   ├── anomaly_detection_dag.py   # Every 4h: Z-score + ARIMA_PLUS scans
│   └── cost_rollup_dag.py         # Weekly cost attribution refresh
├── sql/                           # Standalone BigQuery SQL
│   ├── queries/                   # Ad-hoc analytics + scheduled queries
│   └── schemas/                   # Raw DDL reference
├── analytics/                     # Python analytics utilities
│   ├── anomaly_detector.py        # Z-score statistical detection engine
│   ├── trend_calculator.py        # DORA metrics + WoW trend computation
│   └── dataset_curator.py         # Lineage tracing + schema validation
├── ingestion/                     # Dataflow + streaming ingestion
│   └── dataflow_ingest.py
├── great_expectations/            # GE data quality suites
│   ├── expectations/
│   └── great_expectations.yml
├── looker/                        # Looker LookML views + dashboards
│   ├── views/
│   └── dashboards/
├── terraform/                     # GCP IaC — datasets, IAM, scheduled queries
│   ├── main.tf
│   ├── variables.tf
│   └── outputs.tf
├── tests/                         # Python unit tests
├── .github/workflows/             # CI: SQL lint, dbt compile/test, GE validation
├── pyproject.toml
└── docs/
    ├── ARCHITECTURE.md
    └── RUNBOOK.md
```

---

## Key Capabilities

| Capability | Stack | Detail |
|---|---|---|
| **Streaming Ingestion** | Pub/Sub → Dataflow → BigQuery | Sub-minute latency for fleet events |
| **Batch Ingestion** | GCS → BQ Transfer Service | Daily cost billing export |
| **Transformation** | dbt Core 1.8 on BigQuery | 3-layer medallion: staging → intermediate → marts |
| **Orchestration** | Cloud Composer 2 (Airflow 2.x) | DAG-driven, SLA-monitored, with retry + alerting |
| **Anomaly Detection** | Z-score SQL + BigQuery ML ARIMA_PLUS | Statistical + ML-based outlier detection |
| **Data Quality** | Great Expectations 0.18 + dbt tests | Schema, freshness, null %, referential integrity |
| **Dashboards** | Looker (LookML) + Looker Studio | Fleet health, SLO compliance, FinOps views |
| **Cost Governance** | FinOps labels + slot reservations | Per-team attribution, `require_partition_filter` |
| **CI/CD** | GitHub Actions | SQL lint (sqlfluff), dbt compile, GE on every PR |
| **Infrastructure** | Terraform + `google` provider 5.x | Datasets, IAM, BigQuery scheduled queries |

---

## Quick Start

```bash
git clone https://github.com/kumarrajapuvvalla-bit/bigquery-ops-analytics
cd bigquery-ops-analytics

# 1 — Python dependencies
pip install -r requirements.txt

# 2 — dbt setup
export GCP_PROJECT_ID=your-project-id
export GOOGLE_APPLICATION_CREDENTIALS=~/.config/gcloud/application_default_credentials.json

cd dbt
dbt deps                     # install dbt_utils, dbt_expectations, etc.
dbt debug                    # verify BigQuery connection
dbt run                      # materialise all models
dbt test                     # schema + custom SQL tests
dbt docs generate && dbt docs serve   # browse lineage graph

# 3 — Run analytics locally
cd ..
python analytics/anomaly_detector.py
python analytics/trend_calculator.py

# 4 — Airflow (local dev)
docker-compose -f docker/docker-compose.yml up
```

---

## Data Quality Gates

Every PR runs:

1. **sqlfluff lint** — BigQuery dialect, dbt-templater
2. **dbt compile** — validates all Jinja + SQL syntax against CI dataset
3. **dbt test** — schema tests (not_null, unique, accepted_values) + custom SQL tests
4. **Great Expectations** — `stg_fleet_events` suite: uniqueness, value sets, freshness
5. **Source freshness** — `dbt source freshness` errors if raw tables are stale

---

## DORA Metrics Definitions

| Metric | BigQuery Expression | Elite Target |
|---|---|---|
| Deployment Frequency | `COUNTIF(event_type='DEPLOY') / days_in_window` | ≥ 1/day |
| Change Failure Rate | `failed_deploys / total_deploys` | < 5% |
| Mean Time to Recovery | `AVG(duration_ms) / 60000` (INCIDENT+HEAL) | < 60 min |
| Lead Time for Change | Computed in `stg_fleet_events` via `commit_to_deploy_hours` | < 1 hour |

---

## Dataset Lineage

```
ops_raw.fleet_events        → stg_fleet_events   → int_fleet_health_scores → fleet_health_daily
ops_raw.slo_metrics         → stg_slo_metrics    → int_anomaly_candidates  → slo_compliance
ops_raw.cost_billing_export → stg_cost_billing                             → cost_attribution
ops_raw.infra_metrics       → stg_infra_metrics  → int_route_performance   → fleet_readiness_daily

ops_marts.*  → Looker LookML views
             → BigQuery scheduled anomaly detection queries
             → Cloud Monitoring custom metrics → PagerDuty / Slack
```

---

## Terraform Infrastructure

```bash
cd terraform
terraform init
terraform plan -var="gcp_project_id=your-project" -var="environment=prod"
terraform apply
```

Provisions: BigQuery datasets (`ops_raw`, `ops_staging`, `ops_marts`, `ops_ml`, `ops_ci`), IAM bindings for Looker SA / Composer SA / dbt SA, and BigQuery scheduled queries for anomaly detection (every 4h) and freshness checks (every 15min).

---

*Part of [kumarrajapuvvalla-bit](https://github.com/kumarrajapuvvalla-bit)'s DevOps & Platform Engineering portfolio.*
