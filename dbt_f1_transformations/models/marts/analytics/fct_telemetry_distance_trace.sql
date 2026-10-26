{#
  Telemetry resampled onto a DISTANCE axis (var distance_bin_metres) so laps can be compared point by point.
  Distance is integrated from speed (sum of speed x dt) per lap, so it drifts slightly vs the true track distance;
  it is good for driver-vs-driver comparison on the same circuit, not for survey-grade track length.
  Incremental by session, like fct_telemetry.
#}
{{ config(materialized='incremental', incremental_strategy='append') }}

WITH base AS (
    SELECT * FROM {{ ref('fct_telemetry') }}
    WHERE lap_number IS NOT NULL
    {% if is_incremental() %}
      AND session_key NOT IN (SELECT DISTINCT session_key FROM {{ this }})
    {% endif %}
),
stepped AS (
    SELECT
        *,
        COALESCE(epoch(telemetry_timestamp - LAG(telemetry_timestamp) OVER w), 0) AS dt_seconds,
        MIN(telemetry_timestamp) OVER (PARTITION BY session_key, driver_number, lap_number) AS lap_first_sample_ts
    FROM base
    WINDOW w AS (PARTITION BY session_key, driver_number, lap_number ORDER BY telemetry_timestamp)
),
dist AS (
    SELECT
        *,
        SUM(speed / 3.6 * dt_seconds) OVER (
            PARTITION BY session_key, driver_number, lap_number ORDER BY telemetry_timestamp ROWS UNBOUNDED PRECEDING
        ) AS lap_distance_raw_m,
        epoch(telemetry_timestamp - lap_first_sample_ts) AS lap_elapsed_seconds
    FROM stepped
),
rounded AS (
    -- rounding makes bin edges independent of floating-point summation order (full build == incremental refill)
    SELECT *, ROUND(lap_distance_raw_m, 6) AS lap_distance_m FROM dist
)
SELECT
    session_key, meeting_key, driver_number, lap_number,
    CAST(FLOOR(lap_distance_m / {{ var('distance_bin_metres') }}) AS INTEGER) AS distance_bin,
    CAST(FLOOR(lap_distance_m / {{ var('distance_bin_metres') }}) AS INTEGER) * {{ var('distance_bin_metres') }} AS distance_m,
    MIN(lap_elapsed_seconds) AS elapsed_seconds,
    AVG(speed) AS avg_speed_kph,
    MIN(speed) AS min_speed_kph,
    AVG(throttle) AS avg_throttle,
    MAX(brake) AS max_brake,
    CAST(ROUND(AVG(gear)) AS INTEGER) AS gear,
    AVG(x) AS x, AVG(y) AS y
FROM rounded
GROUP BY session_key, meeting_key, driver_number, lap_number, distance_bin
