{# Time series of gaps. Lapped cars have text gaps ('+1 LAP') which become NULL numbers with the text kept in
   lapped_status. is_in_drs_range is a PROXY: real DRS eligibility is judged at detection points, which the API
   does not expose. sample_duration_seconds lets downstream models time-weight the samples. #}
WITH base AS (
    SELECT
        iv.session_key, iv.meeting_key, iv.driver_number, iv.interval_timestamp,
        {{ f1_try_cast_fn() }}(iv.gap_to_leader_raw AS {{ f1_type('double') }}) AS gap_to_leader_seconds,
        {{ f1_try_cast_fn() }}(iv.interval_raw AS {{ f1_type('double') }}) AS gap_to_car_ahead_seconds,
        CASE WHEN iv.gap_to_leader_raw LIKE '+%LAP%' THEN iv.gap_to_leader_raw END AS lapped_status,
        -- COALESCE: the last sample of a driver has no next sample. DuckDB's LEAST ignores NULLs (giving 60) but BigQuery's
        -- LEAST returns NULL if any argument is NULL, so the cap is made explicit.
        LEAST(60, COALESCE({{ f1_seconds_between('iv.interval_timestamp', 'LEAD(iv.interval_timestamp) OVER (PARTITION BY iv.session_key, iv.driver_number ORDER BY iv.interval_timestamp)') }}, 60))
            AS sample_duration_seconds
    FROM {{ ref('stg_intervals') }} iv
),
with_lap AS (
    {{ f1_asof_join(left='base', right=ref('stg_laps'), on=['session_key', 'driver_number'], left_ts='interval_timestamp', right_ts='date_start',
        left_cols=['session_key', 'meeting_key', 'driver_number', 'interval_timestamp', 'gap_to_leader_seconds',
                   'gap_to_car_ahead_seconds', 'lapped_status', 'sample_duration_seconds'],
        right_cols=['lap_number']) }}
)
SELECT
    b.*,
    COALESCE(b.gap_to_car_ahead_seconds <= {{ var('drs_range_seconds') }}, FALSE) AS is_in_drs_range,
    COALESCE(b.gap_to_car_ahead_seconds <= {{ var('dirty_air_seconds') }}, FALSE) AS is_in_dirty_air
FROM with_lap b
