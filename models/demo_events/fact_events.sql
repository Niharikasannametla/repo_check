-- Fact Layer Model: fact_daily_sessions

WITH session_events AS (
    SELECT
        session_id,
        user_id,
        event_date,
        COUNT(*) AS event_count
    FROM
        {{ ref('stg_events') }}
    GROUP BY
        session_id,
        user_id,
        event_date
),

user_sessions AS (
    SELECT
        user_id,
        event_date,
        COUNT(DISTINCT session_id) AS session_count
    FROM
        session_events
    GROUP BY
        user_id,
        event_date
),

kpis AS (
    SELECT
        event_date,
        user_id,
        session_count,
        AVG(event_count) AS avg_events_per_session
    FROM
        session_events
    JOIN
        user_sessions USING (user_id, event_date)
    GROUP BY
        event_date,
        user_id,
        session_count
)

SELECT
    event_date,
    user_id,
    session_count AS sessions_per_user,
    avg_events_per_session
FROM
    kpis