{# Each driver's best lap vs the session's fastest lap, point by point along the lap (the engineer's "delta trace").
   time_delta_seconds > 0 means the driver is behind the reference at that distance. Only drivers with extracted
   telemetry appear (telemetry is the heaviest endpoint and may be extracted for fewer drivers/sessions). #}
WITH best AS (
    SELECT session_key, driver_number, lap_number, is_overall_fastest_lap FROM {{ ref('fct_fastest_laps') }}
),
reference AS (
    SELECT session_key, driver_number AS ref_driver_number, lap_number AS ref_lap_number FROM best WHERE is_overall_fastest_lap
)
SELECT
    d.session_key, d.driver_number, d.lap_number, r.ref_driver_number,
    d.distance_bin, d.distance_m,
    d.elapsed_seconds - ref.elapsed_seconds AS time_delta_seconds,
    d.avg_speed_kph - ref.avg_speed_kph AS speed_delta_kph
FROM {{ ref('fct_telemetry_distance_trace') }} d
JOIN best b ON d.session_key = b.session_key AND d.driver_number = b.driver_number AND d.lap_number = b.lap_number
JOIN reference r ON d.session_key = r.session_key
JOIN {{ ref('fct_telemetry_distance_trace') }} ref
    ON ref.session_key = r.session_key AND ref.driver_number = r.ref_driver_number
    AND ref.lap_number = r.ref_lap_number AND ref.distance_bin = d.distance_bin
