WITH raw_session_result AS (
    SELECT * FROM {{ source('openf1_raw', 'session_result') }}
),
typed AS (
    SELECT
        CAST(session_key AS {{ f1_type('int') }}) AS session_key,
        CAST(meeting_key AS {{ f1_type('int') }}) AS meeting_key,
        CAST(driver_number AS {{ f1_type('int') }}) AS driver_number,
        {{ f1_try_cast_fn() }}(position AS {{ f1_type('int') }}) AS position,
        {{ f1_try_cast_fn() }}(number_of_laps AS {{ f1_type('int') }}) AS number_of_laps,
        COALESCE(CAST(dnf AS {{ f1_type('bool') }}), FALSE) AS dnf,
        COALESCE(CAST(dns AS {{ f1_type('bool') }}), FALSE) AS dns,
        COALESCE(CAST(dsq AS {{ f1_type('bool') }}), FALSE) AS dsq,
        -- Race: a number or text. Qualifying: a JSON array string like '[q1, q2, q3]'.
        CAST(duration AS {{ f1_type('string') }}) AS duration_raw,
        CAST(gap_to_leader AS {{ f1_type('string') }}) AS gap_to_leader_raw
    FROM raw_session_result
    WHERE driver_number IS NOT NULL
)
SELECT
    *,
    CASE WHEN duration_raw LIKE '[%' THEN NULL ELSE {{ f1_try_cast_fn() }}(duration_raw AS {{ f1_type('double') }}) END AS race_time_seconds,
    {{ f1_try_cast_fn() }}({{ f1_json_scalar("CASE WHEN duration_raw LIKE '[%' THEN duration_raw END", '$[0]') }} AS {{ f1_type('double') }}) AS q1_seconds,
    {{ f1_try_cast_fn() }}({{ f1_json_scalar("CASE WHEN duration_raw LIKE '[%' THEN duration_raw END", '$[1]') }} AS {{ f1_type('double') }}) AS q2_seconds,
    {{ f1_try_cast_fn() }}({{ f1_json_scalar("CASE WHEN duration_raw LIKE '[%' THEN duration_raw END", '$[2]') }} AS {{ f1_type('double') }}) AS q3_seconds,
    CASE WHEN gap_to_leader_raw LIKE '[%' THEN NULL ELSE {{ f1_try_cast_fn() }}(gap_to_leader_raw AS {{ f1_type('double') }}) END AS gap_to_leader_seconds,
    CASE WHEN gap_to_leader_raw LIKE '+%LAP%' THEN gap_to_leader_raw END AS gap_to_leader_lapped_text
FROM typed
