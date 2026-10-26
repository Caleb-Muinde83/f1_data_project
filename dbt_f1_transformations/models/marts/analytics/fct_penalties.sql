{# Penalties parsed from race control text. Parsing depends on the FIA message wording, so unparsed fields stay NULL
   instead of guessing. #}
WITH msgs AS (
    SELECT * FROM {{ ref('stg_race_control') }} WHERE upper(message) LIKE '%PENALTY%'
)
SELECT
    session_key, meeting_key,
    COALESCE(driver_number, TRY_CAST(regexp_extract(message, 'CAR (\d+)', 1) AS INTEGER)) AS driver_number,
    race_control_timestamp AS penalty_timestamp,
    lap_number,
    TRY_CAST(NULLIF(regexp_extract(message, '(\d+) SECOND', 1), '') AS INTEGER) AS penalty_seconds,
    CASE
        WHEN upper(message) LIKE '%DRIVE THROUGH%' THEN 'DRIVE THROUGH'
        WHEN upper(message) LIKE '%STOP/GO%' OR upper(message) LIKE '%STOP AND GO%' THEN 'STOP/GO'
        WHEN upper(message) LIKE '%SECOND%' THEN 'TIME PENALTY'
        ELSE 'OTHER'
    END AS penalty_type,
    NULLIF(regexp_extract(message, ' - (.*)$', 1), '') AS reason,
    message
FROM msgs
