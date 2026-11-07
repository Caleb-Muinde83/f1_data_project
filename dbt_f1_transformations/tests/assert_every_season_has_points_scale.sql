-- A scored session (Race/Sprint) whose season has no rows in f1_points_system would silently score 0 points.
SELECT DISTINCT s.year, s.session_type
FROM {{ ref('stg_sessions') }} s
WHERE s.session_type IN ('Race', 'Sprint')
  AND NOT EXISTS (
      SELECT 1 FROM {{ ref('f1_points_system') }} p
      WHERE p.season = s.year AND p.session_type = s.session_type
  )
