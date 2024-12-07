upsert_data AS (
    -- Combine data from yesterday and today, ensuring arrays are correctly initialized
    SELECT
        DATE_TRUNC('month', '2023-01-01'::DATE)::DATE AS month,
        COALESCE(y.host, t.host) AS host,
        CASE
            WHEN y.host IS NOT NULL THEN
                array_replace(y.hit_array, y.hit_array[array_length(y.hit_array, 1)], t.daily_hits)
            ELSE
                ARRAY[]::INTEGER[]  ARRAY[t.daily_hits]
        END AS hit_array,
        CASE
            WHEN y.host IS NOT NULL THEN
                array_replace(y.unique_visitors, y.unique_visitors[array_length(y.unique_visitors, 1)], t.daily_unique_visitors)
            ELSE
                ARRAY[]::INTEGER[]  ARRAY[t.daily_unique_visitors]
        END AS unique_visitors
    FROM yesterday y
    FULL OUTER JOIN today t ON y.host = t.host
)

-- Insert or update records in the host_activity_reduced table
INSERT INTO host_activity_reduced (month, host, hit_array, unique_visitors)
SELECT
    u.month,
    u.host,
    u.hit_array,
    u.unique_visitors
FROM upsert_data u
ON CONFLICT (month, host) DO UPDATE
SET hit_array = EXCLUDED.hit_array,
    unique_visitors = EXCLUDED.unique_visitors;
