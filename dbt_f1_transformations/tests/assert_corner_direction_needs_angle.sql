-- A corner's direction can only be known if its turn angle was measured. Fails for any corner that has a direction but no angle
-- (the model used to report 'RIGHT' for every corner whose angle was NULL).
SELECT session_key, corner_number, turn_angle_deg, turn_direction
FROM {{ ref('dim_session_corners') }}
WHERE turn_direction IS NOT NULL AND turn_angle_deg IS NULL
  AND {{ in_scope('session_key') }}
