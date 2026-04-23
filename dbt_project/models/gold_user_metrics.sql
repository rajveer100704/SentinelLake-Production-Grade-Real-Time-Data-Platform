{{ config(materialized='table') }}

WITH silver_data AS (
    SELECT * FROM read_parquet('/tmp/delta/silver/*.parquet')
)

SELECT
    user_id,
    COUNT(event_id) as total_events,
    COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) as total_purchases,
    MAX(event_time) as last_active
FROM silver_data
GROUP BY user_id
