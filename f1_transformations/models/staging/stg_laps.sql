WITH raw_laps AS (
    SELECT * FROM {{ source('openf1_raw', 'laps') }}
),

renamed_and_casted AS (
    SELECT
        -- Keys & Identifiers
        CAST(meeting_key AS INTEGER) AS meeting_key,
        CAST(session_key AS INTEGER) AS session_key,
        CAST(driver_number AS INTEGER) AS driver_number,
        CAST(lap_number AS INTEGER) AS lap_number,

        -- Timestamps
        CAST(date_start AS TIMESTAMP) AS date_start,

        -- Lap & Sector Durations (in seconds)
        CAST(lap_duration AS DOUBLE) AS lap_duration,
        CAST(duration_sector_1 AS DOUBLE) AS duration_sector_1,
        CAST(duration_sector_2 AS DOUBLE) AS duration_sector_2,
        CAST(duration_sector_3 AS DOUBLE) AS duration_sector_3,

        -- Speed Traps (km/h)
        CAST(i1_speed AS INTEGER) AS i1_speed,
        CAST(i2_speed AS INTEGER) AS i2_speed,
        CAST(st_speed AS INTEGER) AS st_speed,

        -- Flags
        CAST(is_pit_out_lap AS BOOLEAN) AS is_pit_out_lap,

        -- Segment Arrays (Leaving as raw DuckDB arrays for now)
        segments_sector_1,
        segments_sector_2,
        segments_sector_3

    FROM raw_laps
    WHERE meeting_key IS NOT NULL 
      AND session_key IS NOT NULL
)

SELECT * FROM renamed_and_casted