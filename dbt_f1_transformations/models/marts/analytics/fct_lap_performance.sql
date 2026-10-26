{# Lap-level pace table. Use is_representative_lap = TRUE for pace comparisons (excludes first/in/out laps,
   yellow/SC/VSC/red laps and slow outliers). fuel_corrected_* is an ASSUMPTION-driven normalisation
   (see var fuel_effect_seconds_per_lap). #}
SELECT
    session_key, meeting_key, driver_number, lap_number,
    session_type, lap_start_ts,
    lap_time_seconds,
    duration_sector_1, duration_sector_2, duration_sector_3,
    speed_trap_i1, speed_trap_i2, speed_trap_finish,
    stint_number, tyre_compound, tyre_age_this_lap,
    is_first_lap, is_in_lap, is_out_lap,
    track_status, is_green_flag_lap, is_representative_lap,
    fuel_corrected_lap_time_seconds,
    delta_to_driver_median_seconds,
    lap_time_seconds - MIN(lap_time_seconds) FILTER (WHERE is_representative_lap) OVER (PARTITION BY session_key)
        AS delta_to_session_best_representative_seconds,
    position_start, position_end,
    gap_to_leader_end_seconds, gap_to_car_ahead_end_seconds
FROM {{ ref('int_lap_context') }}
