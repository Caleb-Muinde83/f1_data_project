{# Penalties parsed from race control text. Parsing depends on the FIA message wording, so unparsed fields stay NULL
   instead of guessing.
   The stewards also announce when a penalty has been SERVED ('FIA STEWARDS: PENALTY SERVED - 10 SECOND TIME PENALTY FOR CAR 31 (OCO) - ...'). That is the
   same penalty, not a second one: real data counted it twice (laps 37 and 51). Served notices are removed from the penalty rows and their lap is attached
   to the matching penalty (same session, driver and seconds) as served_lap. #}
WITH msgs AS (
    SELECT * FROM {{ ref('stg_race_control') }} WHERE upper(message) LIKE '%PENALTY%'
),

parsed AS (
    SELECT
        session_key, meeting_key,
        COALESCE(driver_number, {{ f1_try_cast_fn() }}({{ f1_regexp_extract('message', 'CAR (\\d+)') }} AS {{ f1_type('int') }})) AS driver_number,
        race_control_timestamp AS penalty_timestamp,
        lap_number,
        {{ f1_try_cast_fn() }}({{ f1_regexp_extract('message', '(\\d+) SECOND') }} AS {{ f1_type('int') }}) AS penalty_seconds,
        CASE
            WHEN upper(message) LIKE '%DRIVE THROUGH%' THEN 'DRIVE THROUGH'
            WHEN upper(message) LIKE '%STOP/GO%' OR upper(message) LIKE '%STOP AND GO%' THEN 'STOP/GO'
            WHEN upper(message) LIKE '%SECOND%' THEN 'TIME PENALTY'
            ELSE 'OTHER'
        END AS penalty_type,
        {{ f1_regexp_extract('message', ' - (.*)$') }} AS reason,
        message,
        upper(message) LIKE '%SERVED%' AS is_served_notice
    FROM msgs
),

served AS (
    SELECT session_key, driver_number, penalty_seconds, MIN(lap_number) AS served_lap
    FROM parsed WHERE is_served_notice
    GROUP BY session_key, driver_number, penalty_seconds
)

SELECT
    p.session_key, p.meeting_key, p.driver_number, p.penalty_timestamp, p.lap_number, p.penalty_seconds, p.penalty_type, p.reason, p.message,
    s.served_lap
FROM parsed p
LEFT JOIN served s
    ON p.session_key = s.session_key AND p.driver_number = s.driver_number AND p.penalty_seconds = s.penalty_seconds
WHERE NOT p.is_served_notice
