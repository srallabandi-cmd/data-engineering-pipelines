{{
    config(
        materialized='incremental',
        unique_key='user_id',
        incremental_strategy='merge',
        on_schema_change='append_new_columns',
        tags=['staging', 'users', 'daily']
    )
}}

/*
    Staging model for raw users.

    - Selects from the raw users source
    - Hashes PII fields (email, phone) for privacy compliance
    - Maps status codes to human-readable labels
    - Standardizes created/updated timestamps
    - Deduplicates by user_id keeping the most recent record
*/

with source as (

    select * from {{ source('raw', 'users') }}

    {% if is_incremental() %}
        where updated_at > (
            select coalesce(max(updated_at), '1900-01-01'::timestamp)
            from {{ this }}
        )
    {% endif %}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by user_id
            order by updated_at desc
        ) as _row_num

    from source

),

renamed as (

    select
        -- Primary key
        cast(user_id as varchar(64))                                as user_id,

        -- Profile (non-PII)
        cast(coalesce(username, 'unknown') as varchar(100))         as username,
        cast(first_name as varchar(100))                            as first_name,
        cast(last_name as varchar(100))                             as last_name,

        -- PII: hashed for privacy
        {{ dbt_utils.generate_surrogate_key(['email', var('pii_hash_salt')]) }}
                                                                    as email_hash,
        {{ dbt_utils.generate_surrogate_key(['phone', var('pii_hash_salt')]) }}
                                                                    as phone_hash,

        -- Status mapping
        case
            when status = 'A'   then 'active'
            when status = 'I'   then 'inactive'
            when status = 'S'   then 'suspended'
            when status = 'D'   then 'deleted'
            when status = 'P'   then 'pending'
            else coalesce(lower(status), 'unknown')
        end                                                         as user_status,

        -- Attributes
        cast(coalesce(user_segment, 'unassigned') as varchar(50))   as user_segment,
        cast(coalesce(country, 'unknown') as varchar(100))          as country,
        cast(coalesce(language, 'en') as varchar(10))               as preferred_language,
        cast(coalesce(timezone, 'UTC') as varchar(50))              as user_timezone,
        cast(coalesce(is_verified, false) as boolean)               as is_verified,

        -- Value metrics
        cast(coalesce(lifetime_value, 0.0) as numeric(18, 4))      as lifetime_value,
        cast(coalesce(total_orders, 0) as integer)                  as total_orders,

        -- Timestamps
        cast(created_at as timestamp)                               as created_at,
        cast(updated_at as timestamp)                               as updated_at,
        cast(last_login_at as timestamp)                            as last_login_at,

        -- Computed
        case
            when last_login_at is not null
            then extract(day from (current_timestamp - cast(last_login_at as timestamp)))
            else null
        end                                                         as days_since_last_login,

        cast(coalesce(is_active, true) as boolean)                  as is_active

    from deduplicated
    where _row_num = 1

)

select * from renamed
