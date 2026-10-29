WITH raw_laps AS (
    SELECT * FROM {{ source('openf1_raw', 'laps') }}
),
renamed_and_casted AS (
    SELECT
        CAST(session_key AS {{ f1_type('int') }}) AS session_key,
        CAST(meeting_key AS {{ f1_type('int') }}) AS meeting_key,
        CAST(driver_number AS {{ f1_type('int') }}) AS driver_number,

        CAST(lap_number AS {{ f1_type('int') }}) AS lap_number,
        -- Lap start time: the key that lets weather, position, gaps and telemetry be aligned to a lap
        CAST(date_start AS {{ f1_type('timestamp') }}) AS date_start,
        {{ f1_try_cast_fn() }}(lap_duration AS {{ f1_type('double') }}) AS lap_duration,

        {{ f1_try_cast_fn() }}(duration_sector_1 AS {{ f1_type('double') }}) AS duration_sector_1,
        {{ f1_try_cast_fn() }}(duration_sector_2 AS {{ f1_type('double') }}) AS duration_sector_2,
        {{ f1_try_cast_fn() }}(duration_sector_3 AS {{ f1_type('double') }}) AS duration_sector_3,

        -- Speed trap readings (km/h): intermediate 1, intermediate 2, and the finish-line speed trap
        {{ f1_try_cast_fn() }}(i1_speed AS {{ f1_type('int') }}) AS speed_trap_i1,
        {{ f1_try_cast_fn() }}(i2_speed AS {{ f1_type('int') }}) AS speed_trap_i2,
        {{ f1_try_cast_fn() }}(st_speed AS {{ f1_type('int') }}) AS speed_trap_finish,

        segments_sector_1,
        segments_sector_2,
        segments_sector_3,

        COALESCE(is_pit_out_lap, FALSE) AS is_pit_out_lap
    FROM raw_laps
    WHERE driver_number IS NOT NULL
)
SELECT * FROM renamed_and_casted
