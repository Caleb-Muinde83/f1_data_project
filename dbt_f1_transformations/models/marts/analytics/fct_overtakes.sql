{# Overtake events with context. is_probable_pit_cycle_pass = either driver pitted within 90 s of the pass,
   i.e. the position change is likely strategy, not an on-track pass. gap/tyre context is "latest known before the
   pass" (interval samples can be up to ~30 s old). Each ASOF lookup runs on a narrow spine keyed by ot_id and is
   joined back, which keeps the logic identical (and cheap) on DuckDB and BigQuery. #}
WITH o AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY session_key, overtake_timestamp, overtaking_driver_number, overtaken_driver_number) AS ot_id,
        session_key, meeting_key, overtake_timestamp, overtaking_driver_number, overtaken_driver_number, position
    FROM {{ ref('stg_overtakes') }}
),
overtaker AS (
    SELECT ot_id, session_key, overtaking_driver_number AS driver_number, overtake_timestamp FROM o
),
overtaken AS (
    SELECT ot_id, session_key, overtaken_driver_number AS driver_number, overtake_timestamp FROM o
),
overtaker_ctx AS (
    {{ f1_asof_join(left='overtaker', right=ref('int_lap_context'), on=['session_key', 'driver_number'], left_ts='overtake_timestamp', right_ts='lap_start_ts',
        left_cols=['ot_id', 'session_key', 'driver_number', 'overtake_timestamp'],
        right_cols=['lap_number', 'track_status', 'tyre_compound', 'tyre_age_this_lap']) }}
),
overtaken_ctx AS (
    {{ f1_asof_join(left='overtaken', right=ref('int_lap_context'), on=['session_key', 'driver_number'], left_ts='overtake_timestamp', right_ts='lap_start_ts',
        left_cols=['ot_id', 'session_key', 'driver_number', 'overtake_timestamp'],
        right_cols=['tyre_compound', 'tyre_age_this_lap']) }}
),
gap_ctx AS (
    {{ f1_asof_join(left='overtaker', right=ref('stg_intervals'), on=['session_key', 'driver_number'], left_ts='overtake_timestamp', right_ts='interval_timestamp',
        left_cols=['ot_id', 'session_key', 'driver_number', 'overtake_timestamp'], right_cols=['interval_raw']) }}
)
SELECT
    o.session_key, o.meeting_key, o.overtake_timestamp,
    o.overtaking_driver_number, o.overtaken_driver_number,
    o.position AS post_overtake_position,
    a.lap_number,
    a.track_status,
    a.tyre_compound AS overtaker_tyre_compound,
    a.tyre_age_this_lap AS overtaker_tyre_age,
    d.tyre_compound AS overtaken_tyre_compound,
    d.tyre_age_this_lap AS overtaken_tyre_age,
    a.tyre_age_this_lap - d.tyre_age_this_lap AS tyre_age_delta,
    {{ f1_try_cast_fn() }}(g.interval_raw AS {{ f1_type('double') }}) AS overtaker_gap_ahead_before_pass_seconds,
    COALESCE({{ f1_try_cast_fn() }}(g.interval_raw AS {{ f1_type('double') }}) <= {{ var('drs_range_seconds') }}, FALSE) AS was_in_drs_range_before_pass,
    EXISTS (
        SELECT 1 FROM {{ ref('stg_pit') }} p
        WHERE p.session_key = o.session_key
          AND p.driver_number IN (o.overtaking_driver_number, o.overtaken_driver_number)
          AND ABS({{ f1_seconds_between('o.overtake_timestamp', 'p.pit_timestamp') }}) <= 90
    ) AS is_probable_pit_cycle_pass
FROM o
LEFT JOIN overtaker_ctx a ON o.ot_id = a.ot_id
LEFT JOIN overtaken_ctx d ON o.ot_id = d.ot_id
LEFT JOIN gap_ctx g ON o.ot_id = g.ot_id
