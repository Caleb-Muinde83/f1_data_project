{#
  Probability of at least one SC (or VSC) starting in the next N laps, per circuit.
  Model: constant per-lap start hazard h, so P(at least one in N laps) = 1 - (1 - h)^N.
  h is shrunk toward the all-circuit rate:  h_shrunk = (starts + k * h_global) / (laps + k)  with k = var sc_prior_laps.
  HONEST LIMITS: OpenF1 history is short (a handful of races per circuit), so per-circuit estimates are weak, which is
  what the shrinkage and the low_sample flag are for; the constant-hazard assumption ignores lap-1 incidents,
  weather and late-race effects; red flags are not modelled.
#}
WITH races AS (
    SELECT s.circuit_key, s.session_key, MAX(l.lap_number) AS race_laps
    FROM {{ ref('int_lap_context') }} l
    JOIN {{ ref('dim_sessions') }} s ON l.session_key = s.session_key
    WHERE s.session_type = 'Race'
    GROUP BY s.circuit_key, s.session_key
),
exposure AS (
    SELECT circuit_key, COUNT(*) AS races, SUM(race_laps) AS laps FROM races GROUP BY circuit_key
),
statuses AS (
    SELECT 'SC' AS track_status UNION ALL SELECT 'VSC'
),
events AS (
    SELECT circuit_key, track_status, COUNT(*) AS starts, AVG(duration_laps) AS avg_duration_laps,
           COUNT(DISTINCT session_key) AS races_affected
    FROM {{ ref('fct_safety_car_events') }}
    WHERE track_status IN ('SC', 'VSC')
    GROUP BY circuit_key, track_status
),
circuit_status AS (
    SELECT e.circuit_key, e.races, e.laps, s.track_status,
           COALESCE(v.starts, 0) AS starts, v.avg_duration_laps, COALESCE(v.races_affected, 0) AS races_affected
    FROM exposure e
    CROSS JOIN statuses s
    LEFT JOIN events v ON e.circuit_key = v.circuit_key AND s.track_status = v.track_status
),
global_rate AS (
    SELECT track_status, SUM(starts) * 1.0 / NULLIF(SUM(laps), 0) AS global_hazard_per_lap
    FROM circuit_status GROUP BY track_status
),
horizons AS (
    SELECT 5 AS laps_remaining UNION ALL SELECT 10 UNION ALL SELECT 20 UNION ALL SELECT 30
    UNION ALL SELECT 40 UNION ALL SELECT 50 UNION ALL SELECT 60
),
rates AS (
    SELECT
        c.*, g.global_hazard_per_lap,
        c.starts * 1.0 / NULLIF(c.laps, 0) AS raw_hazard_per_lap,
        (c.starts + {{ var('sc_prior_laps') }} * g.global_hazard_per_lap) / (c.laps + {{ var('sc_prior_laps') }}) AS shrunk_hazard_per_lap
    FROM circuit_status c
    JOIN global_rate g ON c.track_status = g.track_status
)
SELECT
    r.circuit_key, r.track_status, h.laps_remaining,
    r.races AS races_observed, r.laps AS race_laps_observed, r.starts AS starts_observed,
    r.races_affected, r.avg_duration_laps,
    r.raw_hazard_per_lap, r.global_hazard_per_lap, r.shrunk_hazard_per_lap,
    1 - POWER(1 - r.shrunk_hazard_per_lap, h.laps_remaining) AS p_at_least_one_start,
    (r.races < 3) AS low_sample
FROM rates r
CROSS JOIN horizons h
