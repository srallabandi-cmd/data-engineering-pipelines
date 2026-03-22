{#
    Custom schema name generation.

    Overrides the default dbt schema naming to support environment-aware
    schema naming with optional prefixes.

    Behavior:
    - In production (target.name == 'prod'): use the custom_schema_name directly
      (e.g., 'staging', 'marts') without any prefix.
    - In development: prefix with the target schema to namespace per developer
      (e.g., 'dbt_dev_staging', 'dbt_dev_marts').
    - If no custom schema is specified: use the default target schema.

    This allows production schemas to be clean (staging, intermediate, marts)
    while dev schemas are namespaced to avoid collisions.
#}

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}

        {# No custom schema specified -- use the target default #}
        {{ default_schema }}

    {%- elif target.name == 'prod' -%}

        {# Production: use the custom schema directly (clean names) #}
        {{ custom_schema_name | trim }}

    {%- elif target.name == 'staging' -%}

        {# Staging environment: prefix with 'stg_' for clarity #}
        stg_{{ custom_schema_name | trim }}

    {%- else -%}

        {# Dev / other: namespace with the target schema #}
        {{ default_schema }}_{{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
