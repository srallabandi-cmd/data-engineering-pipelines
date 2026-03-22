{#
    Custom generic test macros for data quality enforcement.

    Usage in schema.yml:
        columns:
          - name: revenue
            tests:
              - positive_values
              - within_range:
                  min_val: 0
                  max_val: 1000000
          - name: email
            tests:
              - valid_email_format
          - name: updated_at
            tests:
              - not_stale:
                  max_hours: 24
#}


-- ==========================================================================
-- test_positive_values
-- Fails if any non-null value in the column is less than or equal to zero.
-- ==========================================================================
{% test positive_values(model, column_name) %}

    select
        {{ column_name }}
    from {{ model }}
    where {{ column_name }} is not null
      and {{ column_name }} <= 0

{% endtest %}


-- ==========================================================================
-- test_not_stale
-- Fails if the most recent value in the column is older than max_hours.
-- Useful for freshness checks on timestamp columns.
-- ==========================================================================
{% test not_stale(model, column_name, max_hours=24) %}

    {% set max_age_interval = max_hours ~ " hours" %}

    select
        max({{ column_name }}) as most_recent,
        current_timestamp as check_time,
        current_timestamp - interval '{{ max_age_interval }}' as threshold
    from {{ model }}
    having max({{ column_name }}) < current_timestamp - interval '{{ max_age_interval }}'

{% endtest %}


-- ==========================================================================
-- test_valid_email_format
-- Fails if any non-null value does not match a basic email pattern.
-- Uses a regex compatible with PostgreSQL, Snowflake, and BigQuery.
-- ==========================================================================
{% test valid_email_format(model, column_name) %}

    select
        {{ column_name }}
    from {{ model }}
    where {{ column_name }} is not null
      and not (
          {{ column_name }} ~ '^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$'
      )

{% endtest %}


-- ==========================================================================
-- test_within_range
-- Fails if any non-null value is outside the [min_val, max_val] range.
-- Both bounds are optional; omit to check only one direction.
-- ==========================================================================
{% test within_range(model, column_name, min_val=none, max_val=none) %}

    select
        {{ column_name }}
    from {{ model }}
    where {{ column_name }} is not null
    {% if min_val is not none %}
      and {{ column_name }} < {{ min_val }}
    {% endif %}
    {% if max_val is not none %}
      {% if min_val is not none %}
        or ({{ column_name }} is not null and {{ column_name }} > {{ max_val }})
      {% else %}
        and {{ column_name }} > {{ max_val }}
      {% endif %}
    {% endif %}

{% endtest %}


-- ==========================================================================
-- test_no_orphaned_keys
-- Fails if any value in the column does not exist in the reference table.
-- A reusable referential integrity check.
-- ==========================================================================
{% test no_orphaned_keys(model, column_name, reference_model, reference_column) %}

    select
        s.{{ column_name }}
    from {{ model }} s
    left join {{ reference_model }} r
        on s.{{ column_name }} = r.{{ reference_column }}
    where r.{{ reference_column }} is null
      and s.{{ column_name }} is not null

{% endtest %}


-- ==========================================================================
-- test_consistent_row_count
-- Fails if the current row count deviates more than pct_threshold % from
-- the expected count (e.g., previous day's count from a reference table).
-- ==========================================================================
{% test consistent_row_count(model, expected_count, pct_threshold=20) %}

    {% set lower = (expected_count * (100 - pct_threshold) / 100) | int %}
    {% set upper = (expected_count * (100 + pct_threshold) / 100) | int %}

    select
        count(*) as actual_count
    from {{ model }}
    having count(*) < {{ lower }} or count(*) > {{ upper }}

{% endtest %}
