WITH staging_drivers AS (
    SELECT * FROM {{ ref('stg_drivers') }}
),

deduplicated_drivers AS (
    SELECT
        driver_number,
        full_name,
        driver_acronym,
        team_name,
        team_color_hex,
        country_code,
        headshot_url,
        -- This ranks a driver's rows, putting their most recent session as #1
        ROW_NUMBER() OVER (PARTITION BY driver_number ORDER BY session_key DESC) as row_num
    FROM staging_drivers
)

SELECT
    driver_number,
    full_name,
    driver_acronym,
    team_name,
    team_color_hex,
    country_code,
    headshot_url
FROM deduplicated_drivers
WHERE row_num = 1