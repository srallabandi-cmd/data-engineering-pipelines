{{
    config(
        materialized='table',
        unique_key='user_sk',
        tags=['marts', 'dimension', 'users'],
        post_hook=[
            "create index if not exists idx_dim_users_user_id on {{ this }} (user_id)",
            "create index if not exists idx_dim_users_current on {{ this }} (is_current) where is_current = true"
        ]
    )
}}

/*
    Dimension: Users (SCD Type 2)

    Implements Slowly Changing Dimension Type 2 to track historical changes
    in user attributes. Each change creates a new version with:
    - valid_from / valid_to date range
    - is_current flag for the active record
    - surrogate key (user_sk) for fact table joins

    Tracked attributes: user_status, user_segment, country, lifetime_value, is_active.
    Non-tracked attributes (always latest): username, first_name, last_name.
*/

{% set tracked_columns = [
    'user_status',
    'user_segment',
    'country',
    'lifetime_value',
    'is_active',
    'is_verified',
] %}

with staged_users as (

    select * from {{ ref('stg_users') }}

),

-- Generate a hash of the tracked columns for change detection
current_with_hash as (

    select
        *,
        {{ dbt_utils.generate_surrogate_key(tracked_columns) }} as _row_hash
    from staged_users

),

-- Get the existing dimension records (if any)
{% if is_incremental() %}

existing_dimension as (

    select * from {{ this }}
    where is_current = true

),

existing_with_hash as (

    select
        user_id,
        _row_hash as existing_hash,
        user_sk
    from existing_dimension

),

-- Detect changes: compare new hash with existing hash
changes as (

    select
        c.*,
        e.existing_hash,
        e.user_sk as existing_sk,
        case
            when e.existing_hash is null then 'new'
            when c._row_hash != e.existing_hash then 'changed'
            else 'unchanged'
        end as _change_type

    from current_with_hash c
    left join existing_with_hash e
        on c.user_id = e.user_id

),

-- New and changed records get inserted as new versions
new_versions as (

    select
        {{ dbt_utils.generate_surrogate_key(['user_id', '_row_hash', 'updated_at']) }} as user_sk,
        user_id,
        username,
        first_name,
        last_name,
        email_hash,
        phone_hash,
        user_status,
        user_segment,
        country,
        preferred_language,
        user_timezone,
        is_verified,
        lifetime_value,
        total_orders,
        is_active,
        days_since_last_login,
        created_at,
        updated_at,
        last_login_at,
        _row_hash,
        current_timestamp as valid_from,
        cast(null as timestamp) as valid_to,
        true as is_current

    from changes
    where _change_type in ('new', 'changed')

)

select * from new_versions

{% else %}

-- Initial full load: all records are the first version
final as (

    select
        {{ dbt_utils.generate_surrogate_key(['user_id', '_row_hash', 'updated_at']) }} as user_sk,
        user_id,
        username,
        first_name,
        last_name,
        email_hash,
        phone_hash,
        user_status,
        user_segment,
        country,
        preferred_language,
        user_timezone,
        is_verified,
        lifetime_value,
        total_orders,
        is_active,
        days_since_last_login,
        created_at,
        updated_at,
        last_login_at,
        _row_hash,
        created_at as valid_from,
        cast(null as timestamp) as valid_to,
        true as is_current

    from current_with_hash

)

select * from final

{% endif %}
