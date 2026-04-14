- dashboard: fleet_ops_overview
  title: "Fleet Operations Overview"
  layout: newspaper
  preferred_viewer: dashboards-next
  description: "Daily fleet health, DORA metrics, SLO compliance, and FinOps cost attribution"

  filters:
  - name: date_range
    title: "Date Range"
    type: date_filter
    default_value: "28 days"
    allow_multiple_values: false
    required: false
    ui_config:
      type: relative_timeframes
      display: inline
      options: ["7 days", "28 days", "90 days"]

  - name: environment
    title: "Environment"
    type: field_filter
    default_value: "prod"
    allow_multiple_values: true
    required: false
    explore: fleet_health_daily
    field: fleet_health_daily.environment

  - name: squad
    title: "Squad"
    type: field_filter
    allow_multiple_values: true
    required: false
    explore: fleet_health_daily
    field: fleet_health_daily.squad

  elements:
  - name: kpi_deployment_frequency
    title: "Deployment Frequency (deploys/day)"
    type: single_value
    explore: fleet_health_daily
    measures: [fleet_health_daily.deployment_frequency]
    filters:
      fleet_health_daily.report_date: "28 days"
    custom_color_enabled: true
    show_comparison: true
    comparison_type: change

  - name: kpi_change_failure_rate
    title: "Change Failure Rate"
    type: single_value
    explore: fleet_health_daily
    measures: [fleet_health_daily.change_failure_rate]
    filters:
      fleet_health_daily.report_date: "28 days"

  - name: kpi_mttr
    title: "MTTR (mins)"
    type: single_value
    explore: fleet_health_daily
    measures: [fleet_health_daily.mean_time_to_recovery_mins]

  - name: trend_deploys_over_time
    title: "Deployments Over Time"
    type: looker_line
    explore: fleet_health_daily
    dimensions: [fleet_health_daily.report_date]
    measures: [fleet_health_daily.total_deploys, fleet_health_daily.total_incidents]
    sorts: [fleet_health_daily.report_date asc]
    limit: 90

  - name: slo_compliance_table
    title: "SLO Compliance by Service"
    type: looker_grid
    explore: slo_compliance
    dimensions: [slo_compliance.service_name, slo_compliance.slo_name, slo_compliance.is_compliant, slo_compliance.alert_tier]
    measures: [slo_compliance.avg_error_budget_remaining, slo_compliance.total_breach_count]

  - name: cost_by_squad
    title: "Infrastructure Cost by Squad (USD)"
    type: looker_bar
    explore: fleet_health_daily
    dimensions: [fleet_health_daily.squad]
    measures: [fleet_health_daily.total_daily_cost_usd]
    sorts: [fleet_health_daily.total_daily_cost_usd desc]
    limit: 15
