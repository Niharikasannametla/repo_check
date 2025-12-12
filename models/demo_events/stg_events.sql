
WITH recent_events AS (
    SELECT
        user_id,
        event_type,
        event_timestamp,
        session_id,
        device,
        COALESCE(country, 'unknown') AS country
    FROM {{ source('raw', 'events') }}
    WHERE DATE(event_timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY)
)
SELECT * FROM recent_events
