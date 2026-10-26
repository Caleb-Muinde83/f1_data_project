{# Time series of gaps. Lapped cars have text gaps ('+1 LAP') which become NULL numbers with the text kept in
   lapped_status. is_in_drs_range is a PROXY: real DRS eligibility is judged at detection points, which the API
   does not expose. sample_duration_seconds lets downstream models time-weight the samples. #}
WITH base AS (
    SELECT
        iv.session_key, iv.meeting_key, iv.driver_number, iv.interval_timestamp,
        TRY_CAST(iv.gap_to_leader_raw AS DOUBLE) AS gap_to_leader_seconds,
        TRY_CAST(iv.interval_raw AS DOUBLE) AS gap_to_car_ahead_seconds,
        CASE WHEN iv.gap_to_leader_raw LIKE '+%LAP%' THEN iv.gap_to_leader_raw END AS lapped_status,
        LEAST(60, epoch(LEAD(iv.interval_timestamp) OVER (PARTITION BY iv.session_key, iv.driver_number ORDER BY iv.interval_timestamp) - iv.interval_timestamp))
            AS sample_duration_seconds
    FROM {{ ref('stg_intervals') }} iv
)
SELECT
    b.*,
    l.lap_number,
    COALESCE(b.gap_to_car_ahead_seconds <= {{ var('drs_range_seconds') }}, FALSE) AS is_in_drs_range,
    COALESCE(b.gap_to_car_ahead_seconds <= {{ var('dirty_air_seconds') }}, FALSE) AS is_in_dirty_air
FROM base b
ASOF LEFT JOIN {{ ref('stg_laps') }} l
    ON b.session_key = l.session_key AND b.driver_number = l.driver_number AND b.interval_timestamp >= l.date_start
