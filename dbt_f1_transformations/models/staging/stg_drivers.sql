WITH raw_drivers AS (
    SELECT * FROM {{ source('openf1_raw', 'drivers') }}
),

renamed_and_casted AS (
    SELECT
        CAST(driver_number AS INTEGER) AS driver_number,
        CAST(session_key AS INTEGER) AS session_key,
        CAST(meeting_key AS INTEGER) AS meeting_key,
        
        full_name,
        name_acronym AS driver_acronym,
        team_name,
        team_colour AS team_color_hex,
        country_code,
        
        headshot_url

    FROM raw_drivers
    WHERE driver_number IS NOT NULL
)

SELECT * FROM renamed_and_casted