-- OpenF1 reports a Sprint with session_type 'Race'. A session named 'Sprint' that is not typed 'Sprint' would be scored on the Race points scale and
-- could never find its grid (the Race branch looks for a Qualifying session that, on sprint weekends, runs AFTER the sprint).
SELECT session_key, session_name, session_type
FROM {{ ref('stg_sessions') }}
WHERE session_name = 'Sprint' AND session_type <> 'Sprint'
