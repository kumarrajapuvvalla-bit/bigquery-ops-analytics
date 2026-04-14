{% macro date_trunc_safe(date_part, ts_col) %}
    {#
      Safe wrapper around BigQuery's DATE_TRUNC / TIMESTAMP_TRUNC.
      Returns null for null inputs without throwing.
      Usage: {{ date_trunc_safe('day', 'event_ts') }}
    #}
    case
        when {{ ts_col }} is null then null
        else date_trunc(cast({{ ts_col }} as date), {{ date_part }})
    end
{% endmacro %}
