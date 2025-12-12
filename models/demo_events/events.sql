{{
    config(
        materialized='table'
    )
}}

with sample_data as (
    select 'user_1' as user_id, 'click' as event_type, timestamp_sub(current_timestamp(), interval 10 day) as event_timestamp, 'session_1' as session_id, 'mobile' as device, 'USA' as country
    union all
    select 'user_2', 'view', timestamp_sub(current_timestamp(), interval 5 day), 'session_2', 'desktop', 'Canada'
    union all
    select 'user_3', 'purchase', timestamp_sub(current_timestamp(), interval 95 day), 'session_3', 'tablet', null
    union all
    select 'user_4', 'click', timestamp_sub(current_timestamp(), interval 85 day), 'session_4', 'mobile', 'UK'
    union all
    select 'user_5', 'view', timestamp_sub(current_timestamp(), interval 100 day), 'session_5', 'desktop', 'Australia'
)

select * from sample_data