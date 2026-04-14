# Architecture — bigquery-ops-analytics

## Overview

This platform ingests operational telemetry from three source domains — **fleet infrastructure** (EKS/ECS/ALB/RDS), **flight operations** (the flight-ingest microservice), and **cloud cost** (AWS Cost Explorer / GCP Billing) — transforms it through a dbt-managed SQL pipeline in BigQuery, and surfaces KPIs via Looker dashboards and PagerDuty/Slack alerts.

---

## Data Flow

```
1. Sources
   Fleet Telemetry (ops-platform exporter)
   Flight Events   (flight-ingest FastAPI service)
   Infra Metrics   (Prometheus remote-write / CloudWatch export)
   Cost Events     (AWS Cost Explorer API / GCP Billing export)

2. Ingestion
   Streaming: Pub/Sub → Dataflow (Apache Beam) → BigQuery append
   Batch:     Cloud Scheduler → Cloud Function → BigQuery load job

3. Raw Layer  (ops_raw.*)
   Append-only partitioned tables
   No transformations — raw bytes land here

4. Staging Layer  (ops_analytics.stg_*)
   dbt views — light cleaning, type-casting, null handling
   Source freshness checks

5. Intermediate Layer  (ops_analytics.int_*)
   dbt tables — business logic joins, scoring, anomaly flagging

6. Mart Layer  (ops_analytics.*)
   Partitioned + clustered tables — aggregated KPIs
   Powers dashboards and downstream consumers

7. Serving
   Looker / Looker Studio dashboards
   BigQuery scheduled queries (SLO reports)
   PagerDuty incidents (critical anomalies)
   Slack channel alerts (daily digest)
```

---

## dbt Lineage

```
ops_raw.fleet_events
    └── stg_fleet_events
            └── int_fleet_health_scores
                    ├── int_anomaly_candidates → anomaly_log
                    └── fleet_readiness_daily
                              └── ops_kpi_dashboard

ops_raw.flight_routes
    └── stg_flight_routes
            └── int_route_performance
                    └── flight_ops_summary
                              └── ops_kpi_dashboard

ops_raw.infra_metrics
    └── stg_infra_metrics
            └── int_fleet_health_scores (joined)

ops_raw.cost_events
    └── stg_cost_events
            └── infra_cost_by_team
                      └── ops_kpi_dashboard
```

---

## Anomaly Detection

Two complementary strategies are applied at the `int_anomaly_candidates` layer:

| Method | Algorithm | Threshold | Use Case |
|--------|-----------|-----------|----------|
| Z-score | `(x - μ) / σ` over rolling 30-day window | \|z\| ≥ 3.0 | Normal distributions (fleet scores) |
| IQR | `Q1 - 1.5×IQR` / `Q3 + 1.5×IQR` | k = 1.5 | Skewed / heavy-tailed distributions |

Severity mapping:

| Z-score range | Severity |
|--------------|----------|
| ≥ 5.0        | critical |
| 4.0 – 4.99   | high     |
| 3.0 – 3.99   | medium   |
| IQR only     | low      |

---

## Orchestration

| DAG | Schedule | SLA |
|-----|----------|-----|
| `fleet_analytics_daily` | `0 1 * * *` | 03:00 UTC |
| `anomaly_detection_hourly` | `@hourly` | +30 min |
| `cost_rollup_nightly` | `0 2 * * *` | 04:00 UTC |

---

## BigQuery Cost Optimisation

- All mart tables **partitioned** by date, **clustered** by high-cardinality filter columns
- Staging models materialised as **views** (no storage cost)
- Intermediate models use `require_partition_filter = TRUE` on raw tables
- dbt incremental models used where backfill is rare
- Scheduled queries run during **off-peak** hours (batch priority)
