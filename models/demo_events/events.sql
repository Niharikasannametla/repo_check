-- dbt Model: insert_sample_events

{{
    config(
        materialized='table'
            )
}}

with sample_data as (
    select 'user_1' as user_id, 'click' as event_type, '2023-10-01 10:00:00' as event_timestamp, 'session_1' as session_id, 'mobile' as device, 'US' as country
    union all
    select 'user_2', 'view', '2023-10-01 11:00:00', 'session_2', 'desktop', 'CA'
    union all
    select 'user_3', 'purchase', '2023-10-01 12:00:00', 'session_3', 'tablet', 'UK'
    union all
    select 'user_4', 'click', '2023-10-01 13:00:00', 'session_4', 'mobile', null
    union all
    select 'user_5', 'view', '2023-10-01 14:00:00', 'session_5', 'desktop', 'AU'
)

select * from sample_data