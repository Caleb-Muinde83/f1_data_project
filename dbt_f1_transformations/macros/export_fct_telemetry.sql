{% macro export_fct_telemetry() %}

  {% if execute %}
    
    {# 1. Guardrail: Only run if explicitly told to export #}
    {% set should_export = var('export_data', false) %}
    
    {% if should_export %}
      
      {# 2. Capture target parameters #}
      {% set target_session = var('target_session_key', none) %}
      {% set target_meeting = var('target_meeting_key', none) %}

      {% do log("=======================================================================", info=True) %}
      {% if target_session %}
        {% do log("ATTENTION: Starting INCREMENTAL Parquet export for Session: " ~ target_session, info=True) %}
      {% elif target_meeting %}
        {% do log("ATTENTION: Starting INCREMENTAL Parquet export for Meeting: " ~ target_meeting, info=True) %}
      {% else %}
        {% do log("ATTENTION: Starting FULL historical Parquet export for fct_telemetry.", info=True) %}
      {% endif %}
      {% do log("PLEASE WAIT... Streaming data to local storage.", info=True) %}
      {% do log("=======================================================================", info=True) %}

      {# 3. DuckDB Optimizations #}
      {% do run_query("SET preserve_insertion_order = false;") %}
      {% do run_query("SET partitioned_write_max_open_files = 10;") %}

      {# 4. Build Export Query #}
      {% set export_query %}
          COPY (
              SELECT * FROM {{ ref('fct_telemetry') }}
              WHERE 1=1
              {% if target_session %}
                  AND session_key = {{ target_session }}
              {% endif %}
              {% if target_meeting %}
                  AND meeting_key = {{ target_meeting }}
              {% endif %}
          )
          TO 'F:/DaTech/Production/f1_data_project/output/telemetry_data'
          (
              FORMAT PARQUET,
              PARTITION_BY (meeting_key, session_key, driver_number),
              OVERWRITE_OR_IGNORE TRUE
          );
      {% endset %}

      {% do run_query(export_query) %}
      {% do log("SUCCESS: Telemetry export completed successfully!", info=True) %}
      {% do log("=======================================================================", info=True) %}
      
    {% else %}
      {% do log("Skipping export — export_data flag not set to true.", info=True) %}
    {% endif %}

  {% else %}
    {% do log("Skipping export execution during dbt parsing phase.", info=False) %}
  {% endif %}

{% endmacro %}