{% macro export_fct_telemetry() %}

  -- 1. Free up memory by letting DuckDB discard original row-insertion tracking
  {% do run_query("SET preserve_insertion_order = false;") %}
  
  -- 2. Throttling: Automatically flush & close partition writers if more than 10 are open
  {% do run_query("SET partitioned_write_max_open_files = 10;") %}

  {% set export_query %}
    COPY (
        SELECT * FROM {{ ref('fct_telemetry') }}
    )
    TO 'F:/DaTech/f1_data_project/output/telemetry_data'
    (
        FORMAT PARQUET,
        PARTITION_BY (meeting_key, session_key, driver_number),
        OVERWRITE_OR_IGNORE TRUE
    );
  {% endset %}

  {% do log("Starting memory-optimized streaming Parquet export for fct_telemetry...", info=True) %}
  {% do run_query(export_query) %}
  {% do log("Successfully exported telemetry data to lake!", info=True) %}

{% endmacro %}