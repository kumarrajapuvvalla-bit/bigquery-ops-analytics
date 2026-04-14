{% macro zscore(metric_col, partition_cols) %}
    {#
      Computes a population Z-score for metric_col partitioned by partition_cols.
      Usage: {{ zscore('fleet_health_score', ['cluster_name', 'environment']) }}
    #}
    safe_divide(
        {{ metric_col }} - avg({{ metric_col }}) over (
            partition by {{ partition_cols | join(', ') }}
        ),
        nullif(
            stddev_pop({{ metric_col }}) over (
                partition by {{ partition_cols | join(', ') }}
            ), 0
        )
    )
{% endmacro %}
