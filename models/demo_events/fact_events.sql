with event_counts as (
    select
        user_id,
        session_id,
        date(event_timestamp) as event_date,
        count(*) as event_count
    from {{ ref('stg_events') }}
    group by 1, 2, 3
),

session_counts as (
    select
        user_id,
        event_date,
        count(distinct session_id) as sessions_per_user_per_day
    from event_counts
    group by 1, 2
),

average_events as (
    select
        session_id,
        avg(event_count) as avg_events_per_session
    from event_counts
    group by 1
)

select
    ec.user_id,
    ec.session_id,
    ec.event_date,
    ec.event_count,
    sc.sessions_per_user_per_day,
    ae.avg_events_per_session
from event_counts ec
inner join session_counts sc
    on ec.user_id = sc.user_id
    and ec.event_date = sc.event_date
inner join average_events ae
    on ec.session_id = ae.session_id