/*
    Custom test macros for data quality validation in dbt.
*/

-- test_positive_value
-- Asserts that all non-null values in a column are strictly positive (> 0).
{% test positive_value(model, column_name) %}

    select {{ column_name }}
    from {{ model }}
    where {{ column_name }} is not null
      and {{ column_name }} <= 0

{% endtest %}


-- test_valid_email
-- Asserts that all non-null values match a basic email pattern.
{% test valid_email(model, column_name) %}

    select {{ column_name }}
    from {{ model }}
    where {{ column_name }} is not null
      and not regexp_like(
          {{ column_name }},
          '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
      )

{% endtest %}


-- test_no_future_dates
-- Asserts that no date/timestamp values are in the future.
{% test no_future_dates(model, column_name) %}

    select {{ column_name }}
    from {{ model }}
    where {{ column_name }} is not null
      and {{ column_name }} > current_timestamp()

{% endtest %}


-- test_row_count_threshold
-- Asserts that the model has at least `min_rows` rows.
-- Usage in schema.yml:
--   tests:
--     - row_count_threshold:
--         min_rows: 1000
{% test row_count_threshold(model, min_rows=1) %}

    select 1
    from (
        select count(*) as row_count
        from {{ model }}
    ) t
    where t.row_count < {{ min_rows }}

{% endtest %}
