-- Since 2022 a Sprint winner scores 8 (2021: 3). Anything above that means the Race scale was applied to a Sprint.
SELECT r.session_key, r.driver_number, r.total_points
FROM {{ ref('fct_race_results') }} r
JOIN {{ ref('dim_sessions') }} s ON r.session_key = s.session_key
WHERE s.session_name = 'Sprint' AND r.total_points > CASE WHEN s.year = 2021 THEN 3 ELSE 8 END
