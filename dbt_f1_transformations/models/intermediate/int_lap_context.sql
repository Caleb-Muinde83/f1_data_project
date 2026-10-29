{#
  One row per driver per lap, enriched with everything a race engineer needs on one line:
  stint/tyre, pit flags, track status, weather at lap start, position and gaps at lap start/end,
  and pace-normalisation flags (representative lap, fuel-corrected time, deltas).
  Every time-based alignment uses an ASOF join (via the portable f1_asof_join macro): "the latest reading at or before this moment".
#}
WITH laps AS (
    SELECT
        l.*,
        COALESCE(
            LEAD(l.date_start) OVER (PARTITION BY l.session_key, l.driver_number ORDER BY l.lap_number),
            {{ f1_ts_add_seconds('l.date_start', 'l.lap_duration') }}
        ) AS lap_end_ts,
        MAX(l.lap_number) OVER (PARTITION BY l.session_key) AS session_lap_count
    FROM {{ ref('stg_laps') }} l
),

sessions AS (
    SELECT session_key, session_type, session_name, year FROM {{ ref('stg_sessions') }}
),

pit_laps AS (
    SELECT
        session_key, driver_number, lap_number,
        MIN(pit_timestamp) AS pit_timestamp,
        MAX(pit_duration) AS pit_lane_time_seconds
    FROM {{ ref('stg_pit') }}
    GROUP BY 1, 2, 3
),

with_stint_and_pit AS (
    SELECT
        l.*,
        s.session_type,
        s.session_name,
        s.year,
        st.stint_number,
        st.tyre_compound,
        st.tyre_age_at_start + (l.lap_number - st.lap_start) AS tyre_age_this_lap,
        (p.lap_number IS NOT NULL) AS is_in_lap,
        p.pit_lane_time_seconds,
        p.pit_timestamp
    FROM laps l
    LEFT JOIN sessions s ON l.session_key = s.session_key
    LEFT JOIN {{ ref('stg_stints') }} st
        ON l.session_key = st.session_key
        AND l.driver_number = st.driver_number
        AND l.lap_number BETWEEN st.lap_start AND st.lap_end
    LEFT JOIN pit_laps p
        ON l.session_key = p.session_key
        AND l.driver_number = p.driver_number
        AND l.lap_number = p.lap_number
),

with_out_lap AS (
    SELECT
        *,
        (is_pit_out_lap OR COALESCE(LAG(is_in_lap) OVER (PARTITION BY session_key, driver_number ORDER BY lap_number), FALSE)) AS is_out_lap
    FROM with_stint_and_pit
),

lap_track_status AS (
    SELECT
        l.session_key, l.driver_number, l.lap_number,
        MIN(p.severity_rank) AS worst_rank
    FROM with_out_lap l
    JOIN {{ ref('int_track_status_periods') }} p
        ON l.session_key = p.session_key
        AND p.period_start <= l.lap_end_ts
        AND p.period_end >= l.date_start
    GROUP BY 1, 2, 3
),

lap_yellow AS (
    SELECT DISTINCT l.session_key, l.driver_number, l.lap_number
    FROM with_out_lap l
    JOIN {{ ref('stg_race_control') }} rc
        ON l.session_key = rc.session_key
        AND rc.race_control_timestamp >= l.date_start
        AND rc.race_control_timestamp < l.lap_end_ts
    WHERE upper(rc.flag) IN ('YELLOW', 'DOUBLE YELLOW')
),

spine AS (
    SELECT session_key, driver_number, lap_number, date_start AS lap_start_ts, lap_end_ts FROM with_out_lap
),

weather_at_start AS (
    {{ f1_asof_join(left='spine', right=ref('stg_weather'), on=['session_key'], left_ts='lap_start_ts', right_ts='weather_timestamp',
        left_cols=['session_key', 'driver_number', 'lap_number', 'lap_start_ts'],
        right_cols=['weather_timestamp', 'air_temperature', 'track_temperature', 'humidity', 'wind_speed', 'rainfall_flag']) }}
),

position_at_start AS (
    {{ f1_asof_join(left='spine', right=ref('stg_position'), on=['session_key', 'driver_number'], left_ts='lap_start_ts', right_ts='position_timestamp',
        left_cols=['session_key', 'driver_number', 'lap_number', 'lap_start_ts'], right_cols=['position AS position_start']) }}
),

position_at_end AS (
    {{ f1_asof_join(left='spine', right=ref('stg_position'), on=['session_key', 'driver_number'], left_ts='lap_end_ts', right_ts='position_timestamp',
        left_cols=['session_key', 'driver_number', 'lap_number', 'lap_end_ts'], right_cols=['position AS position_end']) }}
),

gaps_at_end AS (
    {{ f1_asof_join(left='spine', right=ref('stg_intervals'), on=['session_key', 'driver_number'], left_ts='lap_end_ts', right_ts='interval_timestamp',
        left_cols=['session_key', 'driver_number', 'lap_number', 'lap_end_ts'], right_cols=['gap_to_leader_raw', 'interval_raw']) }}
),

enriched AS (
    SELECT
        l.*,
        CASE ts.worst_rank WHEN 1 THEN 'RED' WHEN 2 THEN 'SC' WHEN 3 THEN 'VSC' END AS neutralisation_status,
        (y.lap_number IS NOT NULL) AS has_yellow_flag,
        w.weather_timestamp,
        w.air_temperature,
        w.track_temperature,
        w.humidity,
        w.wind_speed,
        w.rainfall_flag,
        ps.position_start,
        pe.position_end,
        {{ f1_try_cast_fn() }}(g.gap_to_leader_raw AS {{ f1_type('double') }}) AS gap_to_leader_end_seconds,
        {{ f1_try_cast_fn() }}(g.interval_raw AS {{ f1_type('double') }}) AS gap_to_car_ahead_end_seconds,
        COALESCE(g.gap_to_leader_raw LIKE '+%LAP%', FALSE) AS is_lapped_at_lap_end
    FROM with_out_lap l
    LEFT JOIN lap_track_status ts
        ON l.session_key = ts.session_key AND l.driver_number = ts.driver_number AND l.lap_number = ts.lap_number
    LEFT JOIN lap_yellow y
        ON l.session_key = y.session_key AND l.driver_number = y.driver_number AND l.lap_number = y.lap_number
    LEFT JOIN weather_at_start w
        ON l.session_key = w.session_key AND l.driver_number = w.driver_number AND l.lap_number = w.lap_number
    LEFT JOIN position_at_start ps
        ON l.session_key = ps.session_key AND l.driver_number = ps.driver_number AND l.lap_number = ps.lap_number
    LEFT JOIN position_at_end pe
        ON l.session_key = pe.session_key AND l.driver_number = pe.driver_number AND l.lap_number = pe.lap_number
    LEFT JOIN gaps_at_end g
        ON l.session_key = g.session_key AND l.driver_number = g.driver_number AND l.lap_number = g.lap_number
),

flagged AS (
    SELECT
        *,
        COALESCE(neutralisation_status, CASE WHEN has_yellow_flag THEN 'YELLOW' END, 'GREEN') AS track_status,
        (lap_number = 1) AS is_first_lap,
        -- A "timed, comparable" lap: has a time, not first/in/out, no yellow/SC/VSC/red
        (lap_duration IS NOT NULL AND lap_duration > 0
            AND NOT is_in_lap AND NOT is_out_lap AND lap_number > 1
            AND neutralisation_status IS NULL AND NOT has_yellow_flag) AS is_clean_candidate
    FROM enriched
),

with_medians AS (
    SELECT
        *,
        {{ f1_median_over('CASE WHEN is_clean_candidate THEN lap_duration END', 'session_key, driver_number') }} AS driver_median_clean_lap_seconds
    FROM flagged
)

SELECT
    session_key, meeting_key, driver_number, lap_number,
    session_type, session_name, year,
    date_start AS lap_start_ts,
    lap_end_ts,
    lap_duration AS lap_time_seconds,
    duration_sector_1, duration_sector_2, duration_sector_3,
    speed_trap_i1, speed_trap_i2, speed_trap_finish,
    stint_number, tyre_compound, tyre_age_this_lap,
    is_first_lap, is_in_lap, is_out_lap, pit_lane_time_seconds,
    track_status, neutralisation_status, has_yellow_flag,
    (track_status = 'GREEN') AS is_green_flag_lap,
    weather_timestamp, air_temperature, track_temperature, humidity, wind_speed,
    (COALESCE(rainfall_flag, 0) > 0) AS is_raining_at_lap_start,
    position_start, position_end,
    gap_to_leader_end_seconds, gap_to_car_ahead_end_seconds, is_lapped_at_lap_end,
    driver_median_clean_lap_seconds,
    (is_clean_candidate AND lap_duration <= driver_median_clean_lap_seconds * {{ var('representative_lap_threshold') }}) AS is_representative_lap,
    CASE
        WHEN session_type IN ('Race', 'Sprint') AND lap_duration IS NOT NULL
        THEN lap_duration - {{ var('fuel_effect_seconds_per_lap') }} * (session_lap_count - lap_number)
    END AS fuel_corrected_lap_time_seconds,
    lap_duration - driver_median_clean_lap_seconds AS delta_to_driver_median_seconds,
    SUM(lap_duration) OVER (PARTITION BY session_key, driver_number ORDER BY lap_number) AS cumulative_race_time_seconds
FROM with_medians
