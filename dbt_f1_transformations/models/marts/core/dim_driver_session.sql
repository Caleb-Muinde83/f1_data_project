{# Driver-to-team assignment PER SESSION. dim_drivers only keeps the latest team, which is wrong for historical
   joins after a mid-season move; join facts to this table on (session_key, driver_number) instead. #}
WITH ranked AS (
    SELECT
        session_key, meeting_key, driver_number, full_name, driver_acronym,
        team_name, team_color_hex, country_code,
        ROW_NUMBER() OVER (PARTITION BY session_key, driver_number ORDER BY team_name) AS row_num
    FROM {{ ref('stg_drivers') }}
)
SELECT session_key, meeting_key, driver_number, full_name, driver_acronym, team_name, team_color_hex, country_code
FROM ranked
WHERE row_num = 1
