view: fleet_health_daily {
  sql_table_name: `ops_marts.fleet_health_daily` ;;
  label: "Fleet Health (Daily)"

  # ── Dimensions ─────────────────────────────────────────────────────────────

  dimension: fleet_health_key {
    type: string
    primary_key: yes
    sql: ${TABLE}.fleet_health_key ;;
    hidden: yes
  }

  dimension_group: report {
    type: time
    timeframes: [raw, date, week, month, quarter, year]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.report_date ;;
    label: "Report"
  }

  dimension: service_name {
    type: string
    sql: ${TABLE}.service_name ;;
    label: "Service"
    tags: ["service", "ownership"]
  }

  dimension: environment {
    type: string
    sql: ${TABLE}.environment ;;
    label: "Environment"
    suggestions: ["dev", "staging", "prod"]
  }

  dimension: squad {
    type: string
    sql: ${TABLE}.squad ;;
    label: "Squad"
    tags: ["ownership", "team"]
  }

  dimension: dora_tier {
    type: string
    sql:
      CASE
        WHEN ${deployment_frequency} >= 1
         AND ${change_failure_rate} <= 0.05
         AND ${mean_time_to_recovery_mins} <= 60    THEN "Elite"
        WHEN ${deployment_frequency} >= 1/7.0
         AND ${change_failure_rate} <= 0.10
         AND ${mean_time_to_recovery_mins} <= 1440  THEN "High"
        WHEN ${deployment_frequency} >= 1/30.0
         AND ${change_failure_rate} <= 0.15         THEN "Medium"
        ELSE "Low"
      END ;;
    label: "DORA Tier"
    tags: ["dora", "reliability"]
  }

  dimension: has_incident_anomaly {
    type: yesno
    sql: ${TABLE}.incident_anomaly ;;
    label: "Incident Anomaly"
  }

  dimension: has_cost_anomaly {
    type: yesno
    sql: ${TABLE}.cost_anomaly ;;
    label: "Cost Anomaly"
  }

  # ── Measures ───────────────────────────────────────────────────────────────

  measure: total_deploys {
    type: sum
    sql: ${TABLE}.deploy_count ;;
    label: "Total Deployments"
    value_format_name: decimal_0
    tags: ["dora", "deploy"]
  }

  measure: total_incidents {
    type: sum
    sql: ${TABLE}.incident_count ;;
    label: "Total Incidents"
    value_format_name: decimal_0
    tags: ["reliability", "incident"]
  }

  measure: deployment_frequency {
    type: average
    sql: ${TABLE}.deployment_frequency ;;
    label: "Deployment Frequency (avg/day)"
    value_format_name: decimal_2
    tags: ["dora"]
  }

  measure: change_failure_rate {
    type: average
    sql: ${TABLE}.change_failure_rate ;;
    label: "Change Failure Rate (%)"
    value_format: "0.00%"
    tags: ["dora"]
  }

  measure: mean_time_to_recovery_mins {
    type: average
    sql: ${TABLE}.mean_time_to_recovery_mins ;;
    label: "MTTR (avg mins)"
    value_format_name: decimal_1
    tags: ["dora", "reliability"]
  }

  measure: avg_availability {
    type: average
    sql: ${TABLE}.p99_availability ;;
    label: "P99 Availability (%)"
    value_format: "0.0000%"
    tags: ["slo", "reliability"]
  }

  measure: total_daily_cost_usd {
    type: sum
    sql: ${TABLE}.daily_cost_usd ;;
    label: "Total Cost (USD)"
    value_format_name: usd
    tags: ["finops", "cost"]
  }

  measure: avg_cost_per_deploy {
    type: average
    sql: ${TABLE}.cost_per_deploy_usd ;;
    label: "Avg Cost / Deploy (USD)"
    value_format_name: usd
    tags: ["finops", "cost", "dora"]
  }

  measure: critical_event_count {
    type: sum
    sql: ${TABLE}.critical_events ;;
    label: "Critical Events"
    value_format_name: decimal_0
    tags: ["severity", "reliability"]
  }

  measure: services_with_anomalies {
    type: count_distinct
    sql: CASE WHEN ${TABLE}.incident_anomaly OR ${TABLE}.cost_anomaly
              THEN ${TABLE}.service_name END ;;
    label: "Services with Anomalies"
    tags: ["anomaly"]
  }
}
