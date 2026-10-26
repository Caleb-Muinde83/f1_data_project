WITH raw_laps AS (
    SELECT * FROM {{ source('openf1_raw', 'laps') }}
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS INTEGER) AS session_key,
        CAST(meeting_key AS INTEGER) AS meeting_key,
        CAST(driver_number AS INTEGER) AS driver_number,

        CAST(lap_number AS INTEGER) AS lap_number,
        -- Lap start time: the key that lets weather, position, gaps and telemetry be aligned to a lap
        CAST(date_start AS TIMESTAMP) AS date_start,
        TRY_CAST(lap_duration AS DOUBLE) AS lap_duration,

        TRY_CAST(duration_sector_1 AS DOUBLE) AS duration_sector_1,
        TRY_CAST(duration_sector_2 AS DOUBLE) AS duration_sector_2,
        TRY_CAST(duration_sector_3 AS DOUBLE) AS duration_sector_3,

        -- Speed trap readings (km/h): intermediate 1, intermediate 2, and the finish-line speed trap
        TRY_CAST(i1_speed AS INTEGER) AS speed_trap_i1,
        TRY_CAST(i2_speed AS INTEGER) AS speed_trap_i2,
        TRY_CAST(st_speed AS INTEGER) AS speed_trap_finish,

        segments_sector_1,
        segments_sector_2,
        segments_sector_3,

        COALESCE(is_pit_out_lap, FALSE) AS is_pit_out_lap
    FROM raw_laps
    WHERE driver_number IS NOT NULL
)
SELECT * FROM renamed_and_casted
