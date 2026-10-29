{#
  DERIVED corner segmentation. OpenF1 publishes no corner data, so corners are DETECTED from telemetry:
    1. take each session's fastest lap that has telemetry (the reference lap) on the distance axis
    2. smooth speed, then call a bin an apex when it is the lowest speed within var corner_min_separation_bins
       AND at least var corner_min_speed_drop_kph below the highest speed within var corner_prominence_window_bins
    3. number the apexes in lap order; entry/exit points sit var corner_entry/exit_distance_m either side
  corner_number is therefore "the Nth detected braking/slowing zone", NOT the official turn number, and it can differ
  between sessions (a chicane may be one corner in one session and two in another). Apexes within
  var corner_min_separation_bins of the lap start/end are not detected (their detection windows would be cut off). turn_direction uses the raw x/y
  plane convention and may be mirrored relative to the real circuit. Speed class uses the reference apex speed.
#}
WITH ref AS (
    SELECT session_key, driver_number, lap_number
    FROM (
        SELECT t.session_key, t.driver_number, t.lap_number,
               ROW_NUMBER() OVER (PARTITION BY t.session_key ORDER BY f.fastest_lap_time_seconds) AS rn
        FROM (SELECT DISTINCT session_key, driver_number, lap_number FROM {{ ref('fct_telemetry_distance_trace') }}) t
        JOIN {{ ref('fct_fastest_laps') }} f
            ON t.session_key = f.session_key AND t.driver_number = f.driver_number AND t.lap_number = f.lap_number
    ) ranked
    WHERE rn = 1
),
trace AS (
    SELECT d.session_key, d.driver_number AS ref_driver_number, d.lap_number AS ref_lap_number,
           d.distance_bin, d.distance_m, d.avg_speed_kph, d.x, d.y
    FROM {{ ref('fct_telemetry_distance_trace') }} d
    JOIN ref r ON d.session_key = r.session_key AND d.driver_number = r.driver_number AND d.lap_number = r.lap_number
),
smoothed AS (
    SELECT *,
        AVG(avg_speed_kph) OVER (PARTITION BY session_key ORDER BY distance_bin
            ROWS BETWEEN {{ var('corner_smooth_bins') }} PRECEDING AND {{ var('corner_smooth_bins') }} FOLLOWING) AS speed_smooth
    FROM trace
),
windows AS (
    SELECT *,
        MIN(speed_smooth) OVER (PARTITION BY session_key ORDER BY distance_bin
            ROWS BETWEEN {{ var('corner_min_separation_bins') }} PRECEDING AND {{ var('corner_min_separation_bins') }} FOLLOWING) AS local_min,
        MAX(speed_smooth) OVER (PARTITION BY session_key ORDER BY distance_bin
            ROWS BETWEEN {{ var('corner_prominence_window_bins') }} PRECEDING AND {{ var('corner_prominence_window_bins') }} FOLLOWING) AS local_max
    FROM smoothed
),
candidates AS (
    SELECT *,
        (speed_smooth = local_min AND local_max - speed_smooth >= {{ var('corner_min_speed_drop_kph') }}
         -- windows are truncated at the lap edges, which makes the first/last bins look like minima: require a full window
         AND distance_bin >= MIN(distance_bin) OVER (PARTITION BY session_key) + {{ var('corner_min_separation_bins') }}
         AND distance_bin <= MAX(distance_bin) OVER (PARTITION BY session_key) - {{ var('corner_min_separation_bins') }}) AS is_candidate
    FROM windows
),
apexes AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY session_key ORDER BY distance_bin) AS corner_number
    FROM (
        SELECT *, COALESCE(LAG(is_candidate) OVER (PARTITION BY session_key ORDER BY distance_bin), FALSE) AS prev_candidate
        FROM candidates
    ) c
    WHERE is_candidate AND NOT prev_candidate     -- first bin of a flat minimum only
),
bounds AS (
    SELECT a.*,
        a.distance_bin - {{ (var('corner_entry_distance_m') / var('distance_bin_metres')) | int }} AS entry_bin,
        a.distance_bin + {{ (var('corner_exit_distance_m') / var('distance_bin_metres')) | int }} AS exit_bin
    FROM apexes a
),
geom AS (
    SELECT b.*, e.x AS entry_x, e.y AS entry_y, x_.x AS exit_x, x_.y AS exit_y
    FROM bounds b
    LEFT JOIN trace e ON b.session_key = e.session_key AND e.distance_bin = b.entry_bin
    LEFT JOIN trace x_ ON b.session_key = x_.session_key AND x_.distance_bin = b.exit_bin
),
turns AS (
    -- radians to degrees by hand: BigQuery has no DEGREES function
    SELECT *,
        ATAN2(y - entry_y, x - entry_x) * 180.0 / ACOS(-1) AS heading_in,
        ATAN2(exit_y - y, exit_x - x) * 180.0 / ACOS(-1) AS heading_out
    FROM geom
),
angled AS (
    SELECT *, heading_out - heading_in AS raw_turn
    FROM turns
)
SELECT
    session_key,
    corner_number,
    distance_bin AS apex_bin,
    distance_m AS apex_distance_m,
    entry_bin, exit_bin,
    avg_speed_kph AS reference_apex_speed_kph,
    CASE WHEN raw_turn > 180 THEN raw_turn - 360 WHEN raw_turn < -180 THEN raw_turn + 360 ELSE raw_turn END AS turn_angle_deg,
    CASE WHEN (CASE WHEN raw_turn > 180 THEN raw_turn - 360 WHEN raw_turn < -180 THEN raw_turn + 360 ELSE raw_turn END) > 0
         THEN 'LEFT' ELSE 'RIGHT' END AS turn_direction,
    CASE WHEN avg_speed_kph < {{ var('corner_slow_kph') }} THEN 'SLOW'
         WHEN avg_speed_kph >= {{ var('corner_fast_kph') }} THEN 'FAST' ELSE 'MEDIUM' END AS speed_class,
    x AS apex_x, y AS apex_y,
    ref_driver_number, ref_lap_number
FROM angled
