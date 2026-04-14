-- =============================================================================
-- schemas/ops_raw/cost_billing_export.sql
-- Mirrors the schema exported by GCP Cloud Billing → BigQuery
-- Ref: https://cloud.google.com/billing/docs/how-to/export-data-bigquery-tables/detailed-usage
-- =============================================================================

CREATE TABLE IF NOT EXISTS `ops_raw.cost_billing_export`
(
    billing_account_id  STRING      NOT NULL,
    service             STRUCT<id STRING, description STRING>,
    sku                 STRUCT<id STRING, description STRING>,
    usage_start_time    TIMESTAMP   NOT NULL,
    usage_end_time      TIMESTAMP   NOT NULL,
    usage_date          DATE        NOT NULL,   -- partition column

    project             STRUCT<
                            id          STRING,
                            name        STRING,
                            labels      ARRAY<STRUCT<key STRING, value STRING>>,
                            ancestry_numbers STRING
                        >,

    labels              ARRAY<STRUCT<key STRING, value STRING>>,
    system_labels       ARRAY<STRUCT<key STRING, value STRING>>,
    location            STRUCT<location STRING, country STRING, region STRING, zone STRING>,
    resource            STRUCT<name STRING, global_name STRING>,

    cost                NUMERIC     NOT NULL,
    currency            STRING      NOT NULL,
    currency_conversion_rate FLOAT64,
    usage               STRUCT<amount FLOAT64, unit STRING, amount_in_pricing_units FLOAT64, pricing_unit STRING>,

    credits             ARRAY<STRUCT<name STRING, amount NUMERIC, full_name STRING, id STRING, type STRING>>,
    invoice             STRUCT<month STRING>,
    cost_type           STRING,
    adjustment_info     STRUCT<id STRING, description STRING, mode STRING, type STRING>,

    -- Lineage
    ingested_at         TIMESTAMP   NOT NULL,
    pipeline_run_id     STRING
)
PARTITION BY usage_date
CLUSTER BY billing_account_id, (service).id, (project).id
OPTIONS (
    description = 'GCP billing export (detailed usage cost). One row per billing line item.',
    require_partition_filter = TRUE
);
