{{ config(severity='warn') }}
-- Reconciliation: a Race/Sprint with classified results should also have lap data (and vice versa).
WITH results AS (SELECT DISTINCT session_key FROM {{ ref('fct_race_results') }} WHERE session_type IN ('Race', 'Sprint')),
laps AS (SELECT DISTINCT session_key FROM {{ ref('stg_laps') }})
SELECT r.session_key, 'results without laps' AS issue FROM results r LEFT JOIN laps l USING (session_key) WHERE l.session_key IS NULL
UNION ALL
SELECT s.session_key, 'laps without results' FROM laps s
JOIN {{ ref('dim_sessions') }} d ON s.session_key = d.session_key
LEFT JOIN results r ON s.session_key = r.session_key
WHERE d.session_type IN ('Race', 'Sprint') AND r.session_key IS NULL
