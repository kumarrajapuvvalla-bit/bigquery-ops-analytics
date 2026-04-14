view: slo_compliance {
  sql_table_name: `ops_marts.slo_compliance` ;;
  label: "SLO Compliance (Weekly)"

  dimension: slo_key {
    type: string
    primary_key: yes
    sql: ${TABLE}.slo_key ;;
    hidden: yes
  }

  dimension_group: report_week {
    type: time
    timeframes: [raw, week, month, quarter]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.report_week ;;
    label: "Report Week"
  }

  dimension: service_name {
    type: string
    sql: ${TABLE}.service_name ;;
    label: "Service"
  }

  dimension: environment {
    type: string
    sql: ${TABLE}.environment ;;
    label: "Environment"
  }

  dimension: slo_name {
    type: string
    sql: ${TABLE}.slo_name ;;
    label: "SLO Name"
  }

  dimension: slo_target {
    type: number
    sql: ${TABLE}.slo_target ;;
    label: "SLO Target"
    value_format: "0.000%"
  }

  dimension: is_compliant {
    type: yesno
    sql: ${TABLE}.is_compliant ;;
    label: "Compliant?"
  }

  dimension: alert_tier {
    type: string
    sql: ${TABLE}.alert_tier ;;
    label: "Alert Tier"
    suggestions: ["P0", "P1", "P2", "OK"]
  }

  measure: services_compliant_pct {
    type: average
    sql: CAST(${TABLE}.is_compliant AS FLOAT64) ;;
    label: "Compliance Rate (%)"
    value_format: "0.0%"
    tags: ["slo"]
  }

  measure: avg_error_budget_remaining {
    type: average
    sql: ${TABLE}.avg_error_budget_remaining ;;
    label: "Avg Error Budget Remaining (%)"
    value_format: "0.00%"
    tags: ["slo", "error_budget"]
  }

  measure: total_breach_count {
    type: sum
    sql: ${TABLE}.breach_count ;;
    label: "Total SLO Breaches"
    value_format_name: decimal_0
    tags: ["slo", "reliability"]
  }

  measure: peak_burn_rate {
    type: max
    sql: ${TABLE}.peak_burn_rate_1h ;;
    label: "Peak 1h Burn Rate"
    value_format_name: decimal_2
    tags: ["slo", "burn_rate"]
  }
}
