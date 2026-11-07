-- Garage visits in practice/qualifying are not strategy stops and must never reach fct_pit_stops.
SELECT p.session_key, s.session_type
FROM {{ ref('fct_pit_stops') }} p
JOIN {{ ref('dim_sessions') }} s ON p.session_key = s.session_key
WHERE s.session_type NOT IN ('Race', 'Sprint')
