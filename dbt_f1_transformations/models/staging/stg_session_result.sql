WITH raw_session_result AS (
    SELECT * FROM {{ source('openf1_raw', 'session_result') }}
),
typed AS (
    SELECT
        CAST(session_key AS INTEGER) AS session_key,
        CAST(meeting_key AS INTEGER) AS meeting_key,
        CAST(driver_number AS INTEGER) AS driver_number,
        TRY_CAST(position AS INTEGER) AS position,
        TRY_CAST(number_of_laps AS INTEGER) AS number_of_laps,
        COALESCE(CAST(dnf AS BOOLEAN), FALSE) AS dnf,
        COALESCE(CAST(dns AS BOOLEAN), FALSE) AS dns,
        COALESCE(CAST(dsq AS BOOLEAN), FALSE) AS dsq,
        -- Race: a number or text. Qualifying: a JSON array string like '[q1, q2, q3]'.
        CAST(duration AS VARCHAR) AS duration_raw,
        CAST(gap_to_leader AS VARCHAR) AS gap_to_leader_raw
    FROM raw_session_result
    WHERE driver_number IS NOT NULL
)
SELECT
    *,
    CASE WHEN duration_raw LIKE '[%' THEN NULL ELSE TRY_CAST(duration_raw AS DOUBLE) END AS race_time_seconds,
    TRY_CAST(json_extract_string(CASE WHEN duration_raw LIKE '[%' THEN duration_raw END, '$[0]') AS DOUBLE) AS q1_seconds,
    TRY_CAST(json_extract_string(CASE WHEN duration_raw LIKE '[%' THEN duration_raw END, '$[1]') AS DOUBLE) AS q2_seconds,
    TRY_CAST(json_extract_string(CASE WHEN duration_raw LIKE '[%' THEN duration_raw END, '$[2]') AS DOUBLE) AS q3_seconds,
    CASE WHEN gap_to_leader_raw LIKE '[%' THEN NULL ELSE TRY_CAST(gap_to_leader_raw AS DOUBLE) END AS gap_to_leader_seconds,
    CASE WHEN gap_to_leader_raw LIKE '+%LAP%' THEN gap_to_leader_raw END AS gap_to_leader_lapped_text
FROM typed
