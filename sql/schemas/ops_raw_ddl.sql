-- ops_raw DDL
-- Raw layer tables. Append-only. Partitioned by ingest date for cost efficiency.
-- Deploy: bq query --use_legacy_sql=false < sql/schemas/ops_raw_ddl.sql

-- Fleet events from the ops-platform exporter
CREATE TABLE IF NOT EXISTS `${GCP_PROJECT}.ops_raw.fleet_events`
(
    event_id        STRING    NOT NULL OPTIONS(description='Unique event identifier'),
    cluster_name    STRING    NOT NULL OPTIONS(description='EKS cluster name'),
    node_group      STRING             OPTIONS(description='EKS managed node group'),
    environment     STRING    NOT NULL OPTIONS(description='dev | staging | prod'),
    event_type      STRING    NOT NULL OPTIONS(description='NODE_HEALTH | ECS_TASK | ALB_HEALTH | RDS_CONN'),
    service_name    STRING             OPTIONS(description='ECS service or K8s deployment name'),
    aws_region      STRING             OPTIONS(description='AWS region, e.g. eu-west-2'),
    readiness_score FLOAT64            OPTIONS(description='Fleet readiness score 0–100'),
    node_count      INT64              OPTIONS(description='Total nodes in node group'),
    healthy_nodes   INT64              OPTIONS(description='Nodes with no health issues'),
    desired_tasks   INT64              OPTIONS(description='ECS desired task count'),
    running_tasks   INT64              OPTIONS(description='ECS running task count'),
    event_ts        TIMESTAMP NOT NULL OPTIONS(description='Event timestamp (UTC)')
)
PARTITION BY DATE(event_ts)
CLUSTER BY cluster_name, environment
OPTIONS(
    description     = 'Raw fleet telemetry events. Append-only.',
    require_partition_filter = TRUE
);

-- Flight route events from the flight-ingest service
CREATE TABLE IF NOT EXISTS `${GCP_PROJECT}.ops_raw.flight_routes`
(
    flight_id               STRING    NOT NULL,
    route_code              STRING    NOT NULL,
    origin                  STRING    NOT NULL OPTIONS(description='IATA origin code'),
    dest                    STRING    NOT NULL OPTIONS(description='IATA destination code'),
    airline                 STRING    NOT NULL,
    aircraft_type           STRING,
    cabin_class             STRING,
    event_type              STRING    NOT NULL OPTIONS(description='DEPARTURE | ARRIVAL | DELAY | CANCEL | DIVERT'),
    priority                STRING             OPTIONS(description='LOW | NORMAL | HIGH | CRITICAL'),
    scheduled_departure_ts  TIMESTAMP NOT NULL,
    actual_departure_ts     TIMESTAMP,
    actual_arrival_ts       TIMESTAMP,
    is_cancelled            BOOL      NOT NULL DEFAULT FALSE,
    is_diverted             BOOL      NOT NULL DEFAULT FALSE
)
PARTITION BY DATE(scheduled_departure_ts)
CLUSTER BY route_code, airline;

-- Infrastructure metrics from Prometheus / CloudWatch
CREATE TABLE IF NOT EXISTS `${GCP_PROJECT}.ops_raw.infra_metrics`
(
    metric_id       STRING    NOT NULL,
    service_name    STRING    NOT NULL,
    cluster_name    STRING    NOT NULL,
    namespace       STRING,
    environment     STRING    NOT NULL,
    metric_name     STRING    NOT NULL,
    metric_source   STRING    NOT NULL OPTIONS(description='prometheus | cloudwatch'),
    metric_value    FLOAT64,
    cpu_utilisation FLOAT64,
    mem_utilisation FLOAT64,
    request_count   INT64,
    error_rate      FLOAT64,
    latency_p99_ms  FLOAT64,
    scraped_ts      TIMESTAMP NOT NULL
)
PARTITION BY DATE(scraped_ts)
CLUSTER BY cluster_name, environment;

-- Cloud cost line items
CREATE TABLE IF NOT EXISTS `${GCP_PROJECT}.ops_raw.cost_events`
(
    cost_event_id   STRING  NOT NULL,
    team            STRING  NOT NULL,
    service_name    STRING  NOT NULL,
    environment     STRING  NOT NULL,
    cost_category   STRING  NOT NULL OPTIONS(description='compute | storage | network | managed'),
    cloud_provider  STRING  NOT NULL OPTIONS(description='aws | gcp'),
    aws_region      STRING,
    resource_id     STRING,
    cost_usd        FLOAT64 NOT NULL,
    usage_amount    FLOAT64,
    usage_unit      STRING,
    cost_date       DATE    NOT NULL
)
PARTITION BY cost_date
CLUSTER BY team, environment;
