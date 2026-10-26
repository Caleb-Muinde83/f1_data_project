{# The classic "race trace": for every lap, each driver's gap to the leader computed from cumulative lap times
   (independent of the sampled intervals endpoint), plus position on cumulative time. #}
SELECT
    session_key, meeting_key, driver_number, lap_number,
    lap_time_seconds,
    cumulative_race_time_seconds,
    cumulative_race_time_seconds - MIN(cumulative_race_time_seconds) OVER (PARTITION BY session_key, lap_number)
        AS gap_to_leader_by_cumulative_time_seconds,
    RANK() OVER (PARTITION BY session_key, lap_number ORDER BY cumulative_race_time_seconds) AS position_by_cumulative_time,
    position_end AS reported_position_end,
    tyre_compound, track_status, is_in_lap
FROM {{ ref('int_lap_context') }}
WHERE session_type IN ('Race', 'Sprint') AND cumulative_race_time_seconds IS NOT NULL
