{#
  High-frequency car telemetry joined to position and lap number.
  * ASOF joins: "latest location reading at or before this telemetry sample" (the two feeds are not
    timestamp-aligned, so an equality join silently drops almost every row).
  * location_age_ms exposes how stale the matched position is; positions older than 2 s are set to NULL.
  * INCREMENTAL by session: a session already present is skipped. To re-load a session whose raw files were
    re-extracted:  dbt run-operation delete_sessions_from_incrementals --args '{sessions: [9158]}'  then dbt run.
#}
{{ config(
    materialized='incremental',
    incremental_strategy=var('bq_incremental_strategy', 'insert_overwrite') if target.type == 'bigquery' else 'append',
    unique_key=['session_key', 'driver_number', 'telemetry_timestamp'] if (target.type == 'bigquery' and var('bq_incremental_strategy', 'insert_overwrite') == 'merge') else none,
    partition_by=({'field': 'session_key', 'data_type': 'int64',
                   'range': {'start': var('bq_session_key_start'), 'end': var('bq_session_key_end'), 'interval': 1}}
                  if target.type == 'bigquery' else none)
) }}

WITH car_data AS (
    SELECT * FROM {{ ref('stg_car_data') }}
    {% if is_incremental() %}
    WHERE session_key NOT IN (SELECT DISTINCT session_key FROM {{ this }})
    {% endif %}
)
, with_location AS (
    {{ f1_asof_join(left='car_data', right=ref('stg_location'), on=['session_key', 'driver_number'], left_ts='telemetry_timestamp', right_ts='location_timestamp',
        left_cols=['session_key', 'meeting_key', 'driver_number', 'telemetry_timestamp', 'rpm', 'speed', 'gear', 'throttle', 'brake', 'drs'],
        right_cols=['location_timestamp', 'x_coordinate', 'y_coordinate', 'z_coordinate']) }}
),
with_lap AS (
    {{ f1_asof_join(left='with_location', right=ref('stg_laps'), on=['session_key', 'driver_number'], left_ts='telemetry_timestamp', right_ts='date_start',
        left_cols=['session_key', 'meeting_key', 'driver_number', 'telemetry_timestamp', 'rpm', 'speed', 'gear', 'throttle', 'brake', 'drs',
                   'location_timestamp', 'x_coordinate', 'y_coordinate', 'z_coordinate'],
        right_cols=['lap_number']) }}
)

SELECT
    session_key, meeting_key, driver_number, telemetry_timestamp, lap_number,
    rpm, speed, gear, throttle, brake, drs,
    CASE WHEN {{ f1_epoch_ms('telemetry_timestamp') }} - {{ f1_epoch_ms('location_timestamp') }} <= 2000 THEN x_coordinate END AS x,
    CASE WHEN {{ f1_epoch_ms('telemetry_timestamp') }} - {{ f1_epoch_ms('location_timestamp') }} <= 2000 THEN y_coordinate END AS y,
    CASE WHEN {{ f1_epoch_ms('telemetry_timestamp') }} - {{ f1_epoch_ms('location_timestamp') }} <= 2000 THEN z_coordinate END AS z,
    {{ f1_epoch_ms('telemetry_timestamp') }} - {{ f1_epoch_ms('location_timestamp') }} AS location_age_ms
FROM with_lap
