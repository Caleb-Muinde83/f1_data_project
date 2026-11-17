{# Strict in CI; WARN on real data (a driver can legitimately receive two penalties of the same size in one race). #}
{{ config(severity = 'error' if target.name == 'ci' else 'warn') }}
-- 'FIA STEWARDS: PENALTY SERVED - 10 SECOND TIME PENALTY FOR CAR 31 (OCO) - ...' confirms a penalty that was already announced. Counted as a new penalty it
-- doubled OCO's penalty in real data (laps 37 and 51).
SELECT session_key, driver_number, penalty_seconds, penalty_type, COUNT(*) AS rows_for_one_penalty
FROM {{ ref('fct_penalties') }}
GROUP BY session_key, driver_number, penalty_seconds, penalty_type
HAVING COUNT(*) > 1
