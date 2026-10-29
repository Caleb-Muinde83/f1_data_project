{# Penalties parsed from race control text. Parsing depends on the FIA message wording, so unparsed fields stay NULL
   instead of guessing. #}
WITH msgs AS (
    SELECT * FROM {{ ref('stg_race_control') }} WHERE upper(message) LIKE '%PENALTY%'
)
SELECT
    session_key, meeting_key,
    COALESCE(driver_number, {{ f1_try_cast_fn() }}({{ f1_regexp_extract('message', 'CAR (\d+)') }} AS {{ f1_type('int') }})) AS driver_number,
    race_control_timestamp AS penalty_timestamp,
    lap_number,
    {{ f1_try_cast_fn() }}({{ f1_regexp_extract('message', '(\d+) SECOND') }} AS {{ f1_type('int') }}) AS penalty_seconds,
    CASE
        WHEN upper(message) LIKE '%DRIVE THROUGH%' THEN 'DRIVE THROUGH'
        WHEN upper(message) LIKE '%STOP/GO%' OR upper(message) LIKE '%STOP AND GO%' THEN 'STOP/GO'
        WHEN upper(message) LIKE '%SECOND%' THEN 'TIME PENALTY'
        ELSE 'OTHER'
    END AS penalty_type,
    {{ f1_regexp_extract('message', ' - (.*)$') }} AS reason,
    message
FROM msgs
