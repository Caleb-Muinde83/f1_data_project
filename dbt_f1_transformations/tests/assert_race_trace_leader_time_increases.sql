{# Strict in CI (fixtures reproduce the retiring-driver bug); WARN against real data until its 27 hits there are explained: a new heuristic test must not
   block downstream models such as fct_undercut_overcut. #}
{{ config(severity = 'error' if target.name == 'ci' else 'warn') }}
-- The leader's cumulative time must grow by at least one plausible lap between consecutive laps. It stalls when a retiring driver's last,
-- duration-less lap is summed as if it were a full lap: his frozen clock becomes the minimum and every other car looks minutes behind
-- (real data: 140 s spikes in the race trace at the lap a driver retired).
WITH leader AS (
    SELECT session_key, lap_number, MIN(cumulative_race_time_seconds) AS leader_time
    FROM {{ ref('fct_race_trace') }}
    GROUP BY session_key, lap_number
)
SELECT a.session_key, a.lap_number, a.leader_time, b.leader_time AS previous_leader_time
FROM leader a
JOIN leader b ON a.session_key = b.session_key AND a.lap_number = b.lap_number + 1
WHERE a.leader_time - b.leader_time < {{ var('min_plausible_lap_seconds', 30) }}
