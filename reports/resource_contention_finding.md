# Investigation Report: Model Training Job Duration Anomaly

**Date:** 2026-04-15  
**Analyst:** Platform Engineering Team  
**Status:** Resolved  

---

## Executive Summary

Anomaly detection flagged that a class of **model training jobs** was consistently running **40–60% longer than their workload-type baseline** during a specific daily window (08:00–11:00 UTC). SQL correlation analysis traced the pattern to **resource contention** with a competing `data_processing` workload type that was consuming the same GCP compute pool during that window. Rescheduling the competing workload normalised training times within one week.

---

## Step 1 — Anomaly Detection Output

The Z-score detector flagged these jobs over a 7-day period:

| Metric | Value |
|---|---|
| Affected workload type | `model_training` |
| Window | 08:00–11:00 UTC daily |
| Baseline mean duration | 3,240 seconds (54 min) |
| Observed mean duration | 5,180 seconds (86 min) |
| Deviation | +59.9% above baseline |
| Average Z-score | +3.1σ |
| Jobs affected per day | 12–18 |

Detection threshold was ±2σ. These jobs were consistently at +3.1σ — well above threshold but not appearing in raw failure logs because they completed successfully (just slowly).

---

## Step 2 — SQL Correlation Query

The following query from `sql/queries/infra_event_correlation.sql` identified the overlap pattern:

```sql
-- Jobs running significantly longer than baseline during 08:00-11:00 UTC
-- Correlated with QUOTA_THROTTLE infrastructure events in the same window

SELECT
    event_type,
    COUNT(DISTINCT aj.job_id)     AS affected_anomalous_jobs,
    AVG(aj.duration_z_score)      AS avg_z_score,
    COUNT(DISTINCT ie.event_id)   AS quota_throttle_events
FROM anomalous_jobs aj
JOIN infrastructure_events ie
    ON ie.event_timestamp BETWEEN aj.started_at AND aj.completed_at
    AND ie.event_type = 'QUOTA_THROTTLE'
GROUP BY event_type
ORDER BY affected_anomalous_jobs DESC
```

**Result:** 94% of anomalous training jobs had ≥1 `QUOTA_THROTTLE` event overlapping their execution window. The throttle events were concentrated between 08:15–10:45 UTC.

---

## Step 3 — Root Cause

Cross-referencing `resource_allocation_logs` with `job_metadata` revealed:

- `data_processing` batch jobs scheduled at 08:00 UTC consumed ~85% of the `n1-highmem-32` node pool
- `model_training` jobs that started after 08:00 were placed on the same pool and received throttled CPU allocation
- Average `cpu_throttle_pct` during affected windows: **62%** vs. baseline of **8%**

The two workload types were not using the same Kubernetes namespace or node pool label selectors, so the contention was invisible without cross-joining the resource and event tables.

---

## Step 4 — Grafana Visualisation

The finding was reproduced in the **Drill-Down Investigation** dashboard:

- **View 1** (Platform Health): success rate held steady — no alert would have fired
- **View 2** (Anomaly Flags): 12–18 training jobs flagged red daily in the 08:00–11:00 window
- **View 3** (Workload Trends): `model_training` duration trending upward week-over-week
- **View 4** (Drill-Down): filtered to `workload_type=model_training, hour=8-11`, overlaid with infra events

---

## Step 5 — Recommendation

**Short-term (implemented):** Reschedule `data_processing` batch jobs from 08:00 UTC to 22:00 UTC.  
**Medium-term:** Add node pool affinity rules to prevent `model_training` and `data_processing` from sharing the same compute tier.  
**Long-term:** Implement workload-type quotas using Kubernetes `ResourceQuota` objects per namespace.

---

## Outcome

After rescheduling on 2026-04-16:

| Metric | Before | After (1 week) |
|---|---|---|
| Mean training duration | 86 min | 55 min |
| Z-score (08:00–11:00) | +3.1σ | +0.3σ (normal) |
| QUOTA_THROTTLE events | 47/day | 2/day |
| `cpu_throttle_pct` | 62% | 7% |

**Training times normalised within one week of the scheduling change.**

---

## Artefacts

| Artefact | Path |
|---|---|
| Anomaly SQL | `sql/queries/infra_event_correlation.sql` |
| Clean dataset | `data/clean/jobs_2026-04-14.parquet` |
| Anomaly report | `data/reports/anomaly_report_2026-04-14.json` |
| Grafana screenshots | `reports/screenshots/` |
