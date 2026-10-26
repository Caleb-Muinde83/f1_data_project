WITH raw_intervals AS (
    SELECT * FROM {{ source('openf1_raw', 'intervals') }}
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS INTEGER) AS session_key,
        CAST(meeting_key AS INTEGER) AS meeting_key,
        CAST(driver_number AS INTEGER) AS driver_number,
        CAST(date AS TIMESTAMP) AS interval_timestamp,

        -- Kept as text: the API mixes numbers with strings such as '+1 LAP' (see fct_race_intervals)
        CAST("interval" AS VARCHAR) AS interval_raw,
        CAST(gap_to_leader AS VARCHAR) AS gap_to_leader_raw
    FROM raw_intervals
    WHERE driver_number IS NOT NULL
)
SELECT * FROM renamed_and_casted
