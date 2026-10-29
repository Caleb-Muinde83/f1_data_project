{# One row per pit stop. pit_lane_time_seconds is what OpenF1 reports (pit entry to exit), NOT stationary time.
   time_lost_seconds = (in-lap + out-lap) - 2 x the driver's median clean lap: it is only meaningful under green
   flags, and is_under_neutralisation flags stops made under SC/VSC/red where the real cost is much lower.
   position change across the stop includes any on-track passes made on the out-lap. #}
WITH pits AS (
    SELECT * FROM {{ ref('stg_pit') }} WHERE pit_duration IS NOT NULL
),
ctx AS (
    SELECT * FROM {{ ref('int_lap_context') }}
),
stops AS (
    SELECT
        p.session_key, p.meeting_key, p.driver_number, p.lap_number, p.pit_timestamp,
        p.pit_duration AS pit_lane_time_seconds,
        ROW_NUMBER() OVER (PARTITION BY p.session_key, p.driver_number ORDER BY p.lap_number, p.pit_timestamp) AS stop_number,
        i.tyre_compound AS compound_before, i.tyre_age_this_lap AS tyre_age_before,
        o.tyre_compound AS compound_after,
        i.position_start AS position_before_stop,
        o.position_end AS position_after_stop,
        i.track_status AS in_lap_track_status,
        i.lap_time_seconds AS in_lap_time_seconds,
        o.lap_time_seconds AS out_lap_time_seconds,
        i.driver_median_clean_lap_seconds AS reference_lap_seconds
    FROM pits p
    LEFT JOIN ctx i ON p.session_key = i.session_key AND p.driver_number = i.driver_number AND p.lap_number = i.lap_number
    LEFT JOIN ctx o ON p.session_key = o.session_key AND p.driver_number = o.driver_number AND p.lap_number + 1 = o.lap_number
)
SELECT
    *,
    in_lap_time_seconds + out_lap_time_seconds - 2 * reference_lap_seconds AS time_lost_seconds,
    position_before_stop - position_after_stop AS net_positions_gained,
    COALESCE(in_lap_track_status IN ('SC', 'VSC', 'RED'), FALSE) AS is_under_neutralisation,
    {{ f1_median_over('pit_lane_time_seconds', 'session_key') }} AS session_median_pit_lane_time_seconds,
    pit_lane_time_seconds > {{ f1_median_over('pit_lane_time_seconds', 'session_key') }} + {{ var('slow_stop_margin_seconds') }} AS is_slow_stop,
    RANK() OVER (PARTITION BY session_key ORDER BY pit_lane_time_seconds) AS pit_lane_rank
FROM stops
