with source as (
    select * from {{ source('raw', 'users') }}
),

renamed as (
    select
        user_id,
        lower(trim(email)) as email,
        coalesce(first_name, '') as first_name,
        coalesce(last_name, '') as last_name,
        concat(coalesce(first_name, ''), ' ', coalesce(last_name, '')) as full_name,
        cast(signup_date as date) as signed_up_at,
        lower(trim(plan_type)) as plan_type,
        lower(trim(country)) as country,
        lower(trim(language)) as language,
        cast(is_active as boolean) as is_active,
        cast(is_verified as boolean) as is_verified,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at,
        _loaded_at as loaded_at
    from source
    where user_id is not null
)

select * from renamed
