WITH raw_session_result AS (
    SELECT * FROM {{ source('openf1_raw', 'session_result') }}
),

renamed_and_casted AS (
    SELECT
        CAST(meeting_key AS INTEGER) AS meeting_key,
        CAST(session_key AS INTEGER) AS session_key,
        CAST(driver_number AS INTEGER) AS driver_number,
        CAST(position AS INTEGER) AS final_position,
        CAST(dnf AS BOOLEAN) AS is_dnf,
        CAST(dns AS BOOLEAN) AS is_dns
    FROM raw_session_result
    WHERE session_key IS NOT NULL
)

SELECT * FROM renamed_and_casted