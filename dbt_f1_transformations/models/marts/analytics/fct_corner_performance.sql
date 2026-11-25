{# Per driver per lap per DETECTED corner (see dim_session_corners for what "corner" means here).
   INCREMENTAL by session: a scoped run replaces those sessions, an unscoped run loads only sessions not yet here. #}
{{ config(
    materialized='incremental',
    incremental_strategy=var('bq_incremental_strategy', 'insert_overwrite') if target.type == 'bigquery' else 'delete+insert',
    unique_key=(['session_key', 'driver_number', 'lap_number', 'corner_number'] if var('bq_incremental_strategy', 'insert_overwrite') == 'merge' else none) if target.type == 'bigquery' else 'session_key',
    partition_by=({'field': 'session_key', 'data_type': 'int64',
                   'range': {'start': var('bq_session_key_start'), 'end': var('bq_session_key_end'), 'interval': 1}}
                  if target.type == 'bigquery' else none),
    pre_hook="{{ scoped_pre_delete() }}"
) }}

WITH j AS (
    SELECT
        t.session_key, t.driver_number, t.lap_number, c.corner_number,
        t.distance_bin, t.distance_m, t.avg_speed_kph, t.min_speed_kph, t.avg_throttle, t.max_brake, t.elapsed_seconds,
        t.is_complete_lap,
        c.entry_bin, c.apex_bin, c.exit_bin, c.apex_distance_m
    FROM {{ ref('fct_telemetry_distance_trace') }} t
    JOIN {{ ref('dim_session_corners') }} c
        ON t.session_key = c.session_key
        AND t.distance_bin BETWEEN c.entry_bin - {{ (var('corner_brake_lookback_m') / var('distance_bin_metres')) | int }} AND c.exit_bin + 2
    WHERE {{ telemetry_load_filter('t.session_key') }}
),
agg AS (
    SELECT
        session_key, driver_number, lap_number, corner_number,
        MIN(apex_distance_m) AS apex_distance_m,
        -- entry and exit use the bins within 2 of the entry/exit point: on a fast section an exact bin often has no sample (see dim_session_corners)
        AVG(CASE WHEN distance_bin BETWEEN entry_bin - 2 AND entry_bin + 2 THEN avg_speed_kph END) AS entry_speed_kph,
        MIN(CASE WHEN distance_bin BETWEEN apex_bin - 3 AND apex_bin + 3 THEN min_speed_kph END) AS apex_speed_kph,
        AVG(CASE WHEN distance_bin BETWEEN exit_bin - 2 AND exit_bin + 2 THEN avg_speed_kph END) AS exit_speed_kph,
        MIN(CASE WHEN max_brake > 0 AND distance_bin <= apex_bin THEN distance_m END) AS brake_start_distance_m,
        MIN(CASE WHEN avg_throttle >= 99 AND distance_bin > apex_bin THEN distance_m END) AS full_throttle_distance_m,
        MIN(CASE WHEN distance_bin BETWEEN exit_bin - 2 AND exit_bin + 2 THEN elapsed_seconds END)
            - MIN(CASE WHEN distance_bin BETWEEN entry_bin - 2 AND entry_bin + 2 THEN elapsed_seconds END) AS corner_time_seconds,
        MIN(is_complete_lap) AS is_complete_lap
    FROM j
    GROUP BY session_key, driver_number, lap_number, corner_number
)
SELECT
    a.*,
    a.apex_distance_m - a.brake_start_distance_m AS brake_to_apex_distance_m,
    a.full_throttle_distance_m - a.apex_distance_m AS apex_to_full_throttle_distance_m,
    a.apex_speed_kph - MAX(CASE WHEN a.is_complete_lap THEN a.apex_speed_kph END) OVER (PARTITION BY a.session_key, a.corner_number) AS apex_speed_delta_to_session_best_kph,
    l.is_representative_lap
FROM agg a
LEFT JOIN {{ ref('int_lap_context') }} l
    ON a.session_key = l.session_key AND a.driver_number = l.driver_number AND a.lap_number = l.lap_number
