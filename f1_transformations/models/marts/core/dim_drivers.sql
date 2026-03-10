WITH drivers AS (
    SELECT * FROM {{ ref('stg_drivers') }}
),

latest_driver_info AS (
    SELECT
        driver_number,
        full_name,
        team_name,
        team_color_hex,  -- Renamed here
        headshot_url,    -- Passed here
        ROW_NUMBER() OVER (PARTITION BY driver_number ORDER BY session_key DESC) as row_num
    FROM drivers
    WHERE full_name IS NOT NULL
)

SELECT
    driver_number,
    full_name,
    team_name,
    team_color_hex, -- MAKE SURE THIS IS HERE
    headshot_url    -- AND THIS
FROM latest_driver_info
WHERE row_num = 1