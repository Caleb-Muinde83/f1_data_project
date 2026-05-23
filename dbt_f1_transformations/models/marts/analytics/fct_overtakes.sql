WITH overtakes AS (
    SELECT * FROM {{ ref('stg_overtakes') }}
)

SELECT
    session_key,
    meeting_key,
    overtaking_driver_number,
    overtaken_driver_number,
    position AS post_overtake_position
FROM overtakes