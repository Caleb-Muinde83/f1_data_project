{#
  Telemetry resampled onto a DISTANCE axis (var distance_bin_metres) so laps can be compared point by point.
  Distance is INTEGRATED from speed (sum of speed x dt), which drifts a little every lap. Because the circuit length is
  constant, every COMPLETE lap (telemetry covers >= 98% of the lap time) is rescaled so that its total distance equals the
  session's typical lap length (the median of complete-lap totals). Incomplete laps (cut-off in/out laps, red-flagged
  laps) keep their raw distance and are marked is_complete_lap = FALSE; do not compare them point by point.
  Laps through the pit lane follow a different path but are still rescaled; filter on is_representative_lap upstream.
  Incremental by session, like fct_telemetry: a scoped run (var session_keys) replaces those sessions, an unscoped run loads only missing ones.

  Unavailable readings are masked, not dropped (the distance axis still needs every sample): a throttle above 100 means "no data" (the value 104
  appears in the raw feed), and a run of var stale_telemetry_min_samples or more consecutive samples with the same non-zero speed and gear is a
  frozen reading, not driving (on real data a qualifying lap started with 3 s of identical 168 km/h, gear 4, throttle 104). The channels of those
  samples are NULL in the output, so a chart shows a gap instead of a flat line and a minimum speed is not taken from them.
#}
{{ config(
    materialized='incremental',
    incremental_strategy=var('bq_incremental_strategy', 'insert_overwrite') if target.type == 'bigquery' else 'delete+insert',
    unique_key=(['session_key', 'driver_number', 'lap_number', 'distance_bin'] if var('bq_incremental_strategy', 'insert_overwrite') == 'merge' else none) if target.type == 'bigquery' else 'session_key',
    partition_by=({'field': 'session_key', 'data_type': 'int64',
                   'range': {'start': var('bq_session_key_start'), 'end': var('bq_session_key_end'), 'interval': 1}}
                  if target.type == 'bigquery' else none),
    pre_hook="{{ scoped_pre_delete() }}"
) }}

WITH base AS (
    SELECT t.*, l.lap_duration
    FROM {{ ref('fct_telemetry') }} t
    LEFT JOIN {{ ref('stg_laps') }} l
        ON t.session_key = l.session_key AND t.driver_number = l.driver_number AND t.lap_number = l.lap_number
    WHERE t.lap_number IS NOT NULL
      AND {{ telemetry_load_filter('t.session_key') }}
),
changes AS (
    SELECT
        *,
        CASE WHEN speed = LAG(speed) OVER w AND gear = LAG(gear) OVER w THEN 0 ELSE 1 END AS is_change
    FROM base
    WINDOW w AS (PARTITION BY session_key, driver_number, lap_number ORDER BY telemetry_timestamp)
),
runs AS (
    SELECT
        *,
        SUM(is_change) OVER (PARTITION BY session_key, driver_number, lap_number ORDER BY telemetry_timestamp ROWS UNBOUNDED PRECEDING) AS run_id
    FROM changes
),
flagged AS (
    SELECT *, (run_len >= {{ var('stale_telemetry_min_samples') }} AND speed > 0) AS is_stale
    FROM (SELECT *, COUNT(*) OVER (PARTITION BY session_key, driver_number, lap_number, run_id) AS run_len FROM runs) r
),
stepped AS (
    SELECT
        *,
        COALESCE({{ f1_seconds_between('LAG(telemetry_timestamp) OVER w', 'telemetry_timestamp') }}, 0) AS dt_seconds,
        -- window MIN/MAX run over integer microseconds, not TIMESTAMPs (some engines mishandle windowed MIN/MAX of TIMESTAMP)
        MIN({{ f1_epoch_us('telemetry_timestamp') }}) OVER (PARTITION BY session_key, driver_number, lap_number) AS lap_first_us,
        MAX({{ f1_epoch_us('telemetry_timestamp') }}) OVER (PARTITION BY session_key, driver_number, lap_number) AS lap_last_us
    FROM flagged
    WINDOW w AS (PARTITION BY session_key, driver_number, lap_number ORDER BY telemetry_timestamp)
),
integrated AS (
    SELECT
        *,
        SUM(speed / 3.6 * dt_seconds) OVER (
            PARTITION BY session_key, driver_number, lap_number ORDER BY telemetry_timestamp ROWS UNBOUNDED PRECEDING
        ) AS lap_distance_raw_m,
        ({{ f1_epoch_us('telemetry_timestamp') }} - lap_first_us) / 1000000.0 AS lap_elapsed_seconds
    FROM stepped
),
lap_totals AS (
    SELECT
        session_key, driver_number, lap_number,
        MAX(lap_distance_raw_m) AS lap_total_raw_m,
        MAX(lap_duration) AS lap_duration,
        (MAX(lap_last_us) - MIN(lap_first_us)) / 1000000.0 AS covered_seconds
    FROM integrated
    GROUP BY session_key, driver_number, lap_number
),
lap_flags AS (
    SELECT *, COALESCE(covered_seconds >= 0.98 * lap_duration, FALSE) AS is_complete_lap
    FROM lap_totals
),
session_length AS (
    SELECT DISTINCT
        session_key,
        {{ f1_median_over('CASE WHEN is_complete_lap THEN lap_total_raw_m END', 'session_key') }} AS track_length_m
    FROM lap_flags
),
normalised AS (
    SELECT
        i.*,
        f.is_complete_lap,
        s.track_length_m,
        ROUND(
            CASE WHEN f.is_complete_lap AND f.lap_total_raw_m > 0 AND s.track_length_m IS NOT NULL
                 THEN i.lap_distance_raw_m * s.track_length_m / f.lap_total_raw_m
                 ELSE i.lap_distance_raw_m END, 6) AS lap_distance_m
    FROM integrated i
    JOIN lap_flags f ON i.session_key = f.session_key AND i.driver_number = f.driver_number AND i.lap_number = f.lap_number
    JOIN session_length s ON i.session_key = s.session_key
),
binned AS (
    SELECT *, {{ f1_cast('FLOOR(lap_distance_m / ' ~ var('distance_bin_metres') ~ ')', 'int') }} AS distance_bin
    FROM normalised
)
SELECT
    session_key, meeting_key, driver_number, lap_number,
    distance_bin,
    distance_bin * {{ var('distance_bin_metres') }} AS distance_m,
    MIN(lap_elapsed_seconds) AS elapsed_seconds,
    AVG(CASE WHEN NOT is_stale THEN speed END) AS avg_speed_kph,
    MIN(CASE WHEN NOT is_stale THEN speed END) AS min_speed_kph,
    AVG(CASE WHEN NOT is_stale AND throttle <= 100 THEN throttle END) AS avg_throttle,
    MAX(CASE WHEN NOT is_stale THEN brake END) AS max_brake,
    {{ f1_cast('ROUND(AVG(CASE WHEN NOT is_stale THEN gear END))', 'int') }} AS gear,
    AVG(x) AS x, AVG(y) AS y,
    MIN(is_complete_lap) AS is_complete_lap,
    MIN(track_length_m) AS track_length_m
FROM binned
GROUP BY session_key, meeting_key, driver_number, lap_number, distance_bin
