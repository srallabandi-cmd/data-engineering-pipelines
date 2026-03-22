with source as (
    select * from {{ source('raw', 'events') }}
),

renamed as (
    select
        event_id,
        user_id,
        event_type,
        cast(event_timestamp as timestamp) as event_at,
        json_extract_scalar(event_properties, '$.page') as page_name,
        json_extract_scalar(event_properties, '$.referrer') as referrer,
        json_extract_scalar(event_properties, '$.device') as device_type,
        cast(json_extract_scalar(event_properties, '$.duration_seconds') as float) as duration_seconds,
        session_id,
        platform,
        app_version,
        _loaded_at as loaded_at
    from source
    where event_id is not null
)

select * from renamed
