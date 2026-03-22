{{
    config(
        materialized='incremental',
        unique_key='event_id',
        incremental_strategy='merge',
        on_schema_change='append_new_columns',
        partition_by={
            'field': 'event_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by=['event_type', 'user_id'],
        tags=['staging', 'events', 'daily']
    )
}}

/*
    Staging model for raw events.

    - Selects from the raw events source
    - Applies column aliasing and type casting
    - Parses timestamps
    - Handles nulls with coalesce defaults
    - Deduplicates using row_number (keeps latest ingested record per event_id)
    - Incremental: only processes new/updated records
*/

with source as (

    select * from {{ source('raw', 'events') }}

    {% if is_incremental() %}
        where ingested_at > (
            select coalesce(max(ingested_at), '1900-01-01'::timestamp)
            from {{ this }}
        )
    {% endif %}

),

-- Deduplicate: keep the most recently ingested record per event_id
deduplicated as (

    select
        *,
        row_number() over (
            partition by event_id
            order by ingested_at desc
        ) as _row_num

    from source

),

renamed as (

    select
        -- Primary key
        cast(event_id as varchar(64))                               as event_id,

        -- Dimensions
        cast(event_type as varchar(50))                             as event_type,
        cast(coalesce(user_id, 'anonymous') as varchar(64))         as user_id,
        cast(session_id as varchar(64))                             as session_id,

        -- Timestamps
        cast(event_timestamp as timestamp)                          as event_timestamp,
        cast(event_timestamp as date)                               as event_date,
        extract(hour from cast(event_timestamp as timestamp))       as event_hour,
        extract(dow from cast(event_timestamp as timestamp))        as event_day_of_week,

        -- Page / Referrer
        cast(page_url as varchar(2048))                             as page_url,
        cast(referrer_url as varchar(2048))                         as referrer_url,

        -- Device
        cast(lower(coalesce(device_type, 'unknown')) as varchar(50))  as device_type,
        cast(lower(coalesce(os, 'unknown')) as varchar(100))          as os,
        cast(lower(coalesce(browser, 'unknown')) as varchar(100))     as browser,

        -- Geography
        cast(coalesce(country, 'unknown') as varchar(100))          as country,
        cast(coalesce(city, 'unknown') as varchar(200))             as city,

        -- Measures
        cast(coalesce(duration_ms, 0) as bigint)                    as duration_ms,
        cast(coalesce(revenue, 0.0) as numeric(18, 4))              as revenue,
        cast(coalesce(is_conversion, false) as boolean)             as is_conversion,

        -- Metadata
        cast(properties as text)                                    as properties_json,
        cast(ingested_at as timestamp)                              as ingested_at,
        cast(ingestion_date as date)                                as ingestion_date

    from deduplicated
    where _row_num = 1

)

select * from renamed
