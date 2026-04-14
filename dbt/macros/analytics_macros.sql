{% macro rolling_zscore(column, partition_by, order_by, window_rows=30) %}
-- Computes rolling Z-score for anomaly detection
    CASE
        WHEN STDDEV_POP({{ column }}) OVER (
            PARTITION BY {{ partition_by }}
            ORDER BY {{ order_by }}
            ROWS BETWEEN {{ window_rows }} PRECEDING AND 1 PRECEDING
        ) = 0 THEN NULL
        ELSE SAFE_DIVIDE(
            {{ column }} - AVG({{ column }}) OVER (
                PARTITION BY {{ partition_by }}
                ORDER BY {{ order_by }}
                ROWS BETWEEN {{ window_rows }} PRECEDING AND 1 PRECEDING
            ),
            STDDEV_POP({{ column }}) OVER (
                PARTITION BY {{ partition_by }}
                ORDER BY {{ order_by }}
                ROWS BETWEEN {{ window_rows }} PRECEDING AND 1 PRECEDING
            )
        )
    END
{% endmacro %}


{% macro dora_tier(deploy_freq, cfr, mttr_mins) %}
-- Classifies a service into DORA performance tier
    CASE
        WHEN {{ deploy_freq }} >= 1
         AND {{ cfr }} <= 0.05
         AND {{ mttr_mins }} <= 60     THEN 'Elite'
        WHEN {{ deploy_freq }} >= 1/7.0
         AND {{ cfr }} <= 0.10
         AND {{ mttr_mins }} <= 1440   THEN 'High'
        WHEN {{ deploy_freq }} >= 1/30.0
         AND {{ cfr }} <= 0.15         THEN 'Medium'
        ELSE 'Low'
    END
{% endmacro %}


{% macro safe_divide(numerator, denominator, fallback='NULL') %}
-- Null-safe division
    CASE
        WHEN COALESCE(CAST({{ denominator }} AS FLOAT64), 0) = 0
            THEN {{ fallback }}
        ELSE SAFE_DIVIDE(CAST({{ numerator }} AS FLOAT64), CAST({{ denominator }} AS FLOAT64))
    END
{% endmacro %}
