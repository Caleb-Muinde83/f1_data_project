{#
  Streams fct_telemetry to a Hive-partitioned Parquet lake (meeting_key/session_key/driver_number).
  Opt-in and scoped, so a normal `dbt run` never triggers a long export:
    dbt run-operation export_fct_telemetry --vars '{export_data: true}'                       # everything
    dbt run-operation export_fct_telemetry --vars '{export_data: true, target_session_key: 9158}'
    dbt run-operation export_fct_telemetry --vars '{export_data: true, target_meeting_key: 1219}'
  Output folder: var telemetry_export_path (env F1_EXPORT_PATH). The partition folders are created by COPY, but
  the PARENT of that path must already exist (COPY will not create missing parents):  mkdir -p output
  Memory notes (see handbook Chapter 7): no global ORDER BY, preserve_insertion_order=false, capped open writers.
#}
{% macro export_fct_telemetry() %}
  {% if execute %}
    {% if not var('export_data', false) %}
      {% do log("export_fct_telemetry skipped: pass --vars '{export_data: true}' to run it.", info=True) %}
    {% else %}
      {% set path = var('telemetry_export_path') %}
      {% set filters = [] %}
      {% if var('target_session_key', none) is not none %}{% do filters.append("session_key = " ~ (var('target_session_key') | int)) %}{% endif %}
      {% if var('target_meeting_key', none) is not none %}{% do filters.append("meeting_key = " ~ (var('target_meeting_key') | int)) %}{% endif %}
      {% set where_clause = ("WHERE " ~ (filters | join(" AND "))) if filters else "" %}

      {% do run_query("SET preserve_insertion_order = false;") %}
      {% do run_query("SET partitioned_write_max_open_files = 10;") %}
      {% set export_query %}
        COPY (SELECT * FROM {{ ref('fct_telemetry') }} {{ where_clause }})
        TO '{{ path }}'
        (FORMAT PARQUET, PARTITION_BY (meeting_key, session_key, driver_number), OVERWRITE_OR_IGNORE TRUE);
      {% endset %}
      {% do log("Starting Parquet export to " ~ path ~ " " ~ where_clause, info=True) %}
      {% do run_query(export_query) %}
      {% do log("Export finished.", info=True) %}
    {% endif %}
  {% endif %}
{% endmacro %}
