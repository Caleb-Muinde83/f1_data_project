{# Per driver per lap telemetry KPIs: top/min speed, throttle/brake application, gear usage, DRS and braking zones.
   INCREMENTAL by session (was a table that rescanned every telemetry row on each build): a scoped run replaces those sessions, an unscoped run
   loads only sessions not yet here. #}
{{ config(
    materialized='incremental',
    incremental_strategy=var('bq_incremental_strategy', 'insert_overwrite') if target.type == 'bigquery' else 'delete+insert',
    unique_key=(['session_key', 'driver_number', 'lap_number'] if var('bq_incremental_strategy', 'insert_overwrite') == 'merge' else none) if target.type == 'bigquery' else 'session_key',
    partition_by=({'field': 'session_key', 'data_type': 'int64',
                   'range': {'start': var('bq_session_key_start'), 'end': var('bq_session_key_end'), 'interval': 1}}
                  if target.type == 'bigquery' else none),
    pre_hook="{{ scoped_pre_delete() }}"
) }}

WITH samples AS (
    SELECT
        *,
        LAG(brake) OVER (PARTITION BY session_key, driver_number, lap_number ORDER BY telemetry_timestamp) AS prev_brake
    FROM {{ ref('fct_telemetry') }}
    WHERE lap_number IS NOT NULL
      AND {{ telemetry_load_filter('session_key') }}
)
SELECT
    session_key, meeting_key, driver_number, lap_number,
    COUNT(*) AS telemetry_samples,
    MAX(speed) AS top_speed_kph,
    MIN(speed) AS min_speed_kph,
    AVG(speed) AS avg_speed_kph,
    -- a throttle above 100 (the raw feed's 104) means 'no data': it must not count as full throttle
    AVG(CASE WHEN throttle <= 100 THEN {{ f1_cast('(throttle >= 99)', 'int') }} END) AS full_throttle_fraction,
    AVG({{ f1_cast('(brake > 0)', 'int') }}) AS braking_fraction,
    -- a braking zone starts when brake goes from released (0) to applied (>0)
    COUNT(CASE WHEN brake > 0 AND COALESCE(prev_brake, 0) = 0 THEN 1 END) AS braking_zone_count,
    AVG({{ f1_cast('(drs IN (10, 12, 14))', 'int') }}) AS drs_open_fraction,
    MAX(gear) AS max_gear,
    AVG({{ f1_cast('(gear <= 3)', 'int') }}) AS low_gear_fraction,
    AVG({{ f1_cast('(gear >= 7)', 'int') }}) AS high_gear_fraction
FROM samples
GROUP BY session_key, meeting_key, driver_number, lap_number
