{# Overtake events with context. is_probable_pit_cycle_pass = either driver pitted within var window of the pass,
   i.e. the position change is likely strategy, not an on-track pass. gap/tyre context is "latest known before the
   pass" (interval samples can be up to ~30 s old). #}
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
    TRY_CAST(iv.interval_raw AS DOUBLE) AS overtaker_gap_ahead_before_pass_seconds,
    COALESCE(TRY_CAST(iv.interval_raw AS DOUBLE) <= {{ var('drs_range_seconds') }}, FALSE) AS was_in_drs_range_before_pass,
    EXISTS (
        SELECT 1 FROM {{ ref('stg_pit') }} p
        WHERE p.session_key = o.session_key
          AND p.driver_number IN (o.overtaking_driver_number, o.overtaken_driver_number)
          AND abs(epoch(p.pit_timestamp - o.overtake_timestamp)) <= 90
    ) AS is_probable_pit_cycle_pass
FROM {{ ref('stg_overtakes') }} o
ASOF LEFT JOIN {{ ref('int_lap_context') }} a
    ON o.session_key = a.session_key AND o.overtaking_driver_number = a.driver_number AND o.overtake_timestamp >= a.lap_start_ts
ASOF LEFT JOIN {{ ref('int_lap_context') }} d
    ON o.session_key = d.session_key AND o.overtaken_driver_number = d.driver_number AND o.overtake_timestamp >= d.lap_start_ts
ASOF LEFT JOIN {{ ref('stg_intervals') }} iv
    ON o.session_key = iv.session_key AND o.overtaking_driver_number = iv.driver_number AND o.overtake_timestamp >= iv.interval_timestamp
