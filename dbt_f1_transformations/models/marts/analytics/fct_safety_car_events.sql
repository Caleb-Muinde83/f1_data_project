{# Every SC / VSC / red period in a Race, with the race lap it started and ended on (lap = the highest lap any car had
   started at that moment, i.e. effectively the leader's lap). #}
WITH periods AS (
    SELECT p.session_key, s.circuit_key, s.year, p.track_status, p.period_start, p.period_end
    FROM {{ ref('int_track_status_periods') }} p
    JOIN {{ ref('dim_sessions') }} s ON p.session_key = s.session_key
    WHERE s.session_type = 'Race'
),
laps AS (
    SELECT session_key, lap_number, lap_start_ts FROM {{ ref('int_lap_context') }}
),
race_length AS (
    SELECT session_key, MAX(lap_number) AS race_laps FROM laps GROUP BY session_key
)
SELECT
    p.session_key, p.circuit_key, p.year, p.track_status, p.period_start, p.period_end,
    MAX(CASE WHEN l.lap_start_ts <= p.period_start THEN l.lap_number END) AS start_lap,
    MAX(l.lap_number) AS end_lap,
    MAX(l.lap_number) - MAX(CASE WHEN l.lap_start_ts <= p.period_start THEN l.lap_number END) + 1 AS duration_laps,
    {{ f1_seconds_between('p.period_start', 'p.period_end') }} AS duration_seconds,
    MAX(r.race_laps) AS race_laps
FROM periods p
JOIN laps l ON p.session_key = l.session_key AND l.lap_start_ts <= p.period_end
JOIN race_length r ON p.session_key = r.session_key
GROUP BY p.session_key, p.circuit_key, p.year, p.track_status, p.period_start, p.period_end
