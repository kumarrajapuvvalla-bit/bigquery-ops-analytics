{%- macro generate_schema_name(custom_schema_name, node) -%}
    {#
      Override dbt's default schema naming.
      In prod: use the custom schema directly (e.g., stg, int, ops_analytics).
      In dev:  prefix with the developer's target dataset to avoid collisions.
    #}
    {%- set default_schema = target.dataset -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- elif target.name == 'prod' -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro -%}
