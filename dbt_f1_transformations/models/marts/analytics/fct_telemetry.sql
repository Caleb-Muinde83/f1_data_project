{#
  High-frequency car telemetry joined to position and lap number.
  * ASOF joins: "latest location reading at or before this telemetry sample" (the two feeds are not
    timestamp-aligned, so an equality join silently drops almost every row).
  * location_age_ms exposes how stale the matched position is; positions older than 2 s are set to NULL.
  * INCREMENTAL by session: a session already present is skipped. To re-load a session whose raw files were
    re-extracted:  dbt run-operation delete_sessions_from_incrementals --args '{sessions: [9158]}'  then dbt run.
#}
{{ config(materialized='incremental', incremental_strategy='append') }}

WITH car_data AS (
    SELECT * FROM {{ ref('stg_car_data') }}
    {% if is_incremental() %}
    WHERE session_key NOT IN (SELECT DISTINCT session_key FROM {{ this }})
    {% endif %}
)

SELECT
    c.session_key,
    c.meeting_key,
    c.driver_number,
    c.telemetry_timestamp,
    l.lap_number,
    c.rpm, c.speed, c.gear, c.throttle, c.brake, c.drs,
    CASE WHEN epoch_ms(c.telemetry_timestamp) - epoch_ms(loc.location_timestamp) <= 2000 THEN loc.x_coordinate END AS x,
    CASE WHEN epoch_ms(c.telemetry_timestamp) - epoch_ms(loc.location_timestamp) <= 2000 THEN loc.y_coordinate END AS y,
    CASE WHEN epoch_ms(c.telemetry_timestamp) - epoch_ms(loc.location_timestamp) <= 2000 THEN loc.z_coordinate END AS z,
    epoch_ms(c.telemetry_timestamp) - epoch_ms(loc.location_timestamp) AS location_age_ms
FROM car_data c
ASOF LEFT JOIN {{ ref('stg_location') }} loc
    ON c.session_key = loc.session_key AND c.driver_number = loc.driver_number
    AND c.telemetry_timestamp >= loc.location_timestamp
ASOF LEFT JOIN {{ ref('stg_laps') }} l
    ON c.session_key = l.session_key AND c.driver_number = l.driver_number
    AND c.telemetry_timestamp >= l.date_start
