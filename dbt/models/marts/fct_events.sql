{{
    config(
        materialized='table',
        unique_key='event_id',
        tags=['marts', 'fact', 'events'],
        partition_by={
            'field': 'event_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by=['event_type', 'user_id'],
        post_hook=[
            "create index if not exists idx_fct_events_user on {{ this }} (user_id)",
            "create index if not exists idx_fct_events_date on {{ this }} (event_date)",
            "create index if not exists idx_fct_events_session on {{ this }} (session_id)"
        ]
    )
}}

/*
    Fact Table: Events

    Grain: one row per event occurrence.

    Contains:
    - Foreign keys to dimension tables (user_sk, session_id)
    - Degenerate dimensions (event_type, device_type, browser, os)
    - Measures (duration_ms, revenue)
    - Computed metrics (is_conversion, event sequence in session)
    - Timestamps for partitioning
*/

with events as (

    select * from {{ ref('stg_events') }}

),

sessions as (

    select * from {{ ref('int_user_sessions') }}

),

dim_users as (

    select
        user_sk,
        user_id
    from {{ ref('dim_users') }}
    where is_current = true

),

-- Enrich events with session and dimension keys
enriched_events as (

    select
        -- Primary key
        e.event_id,

        -- Foreign keys
        coalesce(u.user_sk, {{ dbt_utils.generate_surrogate_key(["'unknown'"]) }}) as user_sk,
        s.session_id,

        -- Degenerate dimensions
        e.event_type,
        e.device_type,
        e.os,
        e.browser,
        e.country,
        e.city,
        e.page_url,
        e.referrer_url,

        -- Timestamps
        e.event_timestamp,
        e.event_date,
        e.event_hour,
        e.event_day_of_week,

        -- Measures
        e.duration_ms,
        e.revenue,
        e.is_conversion,

        -- Session context
        s.session_category,
        s.session_event_count,
        s.session_duration_seconds,

        -- Event sequence within session
        row_number() over (
            partition by s.session_id
            order by e.event_timestamp
        ) as event_sequence_in_session,

        -- Previous and next event type (for funnel analysis)
        lag(e.event_type) over (
            partition by e.user_id
            order by e.event_timestamp
        ) as prev_event_type,

        lead(e.event_type) over (
            partition by e.user_id
            order by e.event_timestamp
        ) as next_event_type,

        -- Time between events
        extract(epoch from (
            e.event_timestamp - lag(e.event_timestamp) over (
                partition by e.user_id
                order by e.event_timestamp
            )
        )) as seconds_since_prev_event,

        -- Metadata
        e.properties_json,
        e.ingested_at,
        current_timestamp as dbt_updated_at

    from events e

    left join sessions s
        on e.user_id = s.user_id
        and e.event_timestamp between s.session_start_at and s.session_end_at

    left join dim_users u
        on e.user_id = u.user_id

)

select * from enriched_events
