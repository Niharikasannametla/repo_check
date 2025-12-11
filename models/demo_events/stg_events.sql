
WITH cleaned_events AS (
  SELECT
    user_id,
    event_type,
    SAFE_CAST(event_timestamp AS TIMESTAMP) AS event_ts,
    session_id,
    device,
    COALESCE(country, 'unknown') AS country,
    DATE(SAFE_CAST(event_timestamp AS TIMESTAMP)) AS event_date
  FROM {{ source('raw', 'events') }}
  WHERE SAFE_CAST(event_timestamp AS TIMESTAMP)
        >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 180 DAY)
)

SELECT
  user_id,
  event_type,
  event_ts AS event_timestamp,
  session_id,
  device,
  country,
  event_date
FROM cleaned_events
