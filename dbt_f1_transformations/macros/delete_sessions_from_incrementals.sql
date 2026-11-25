{# Remove sessions from EVERY incremental model in the telemetry group (fct_telemetry, fct_telemetry_distance_trace,
   fct_telemetry_lap_summary, dim_session_corners, fct_corner_performance, fct_lap_time_delta_trace), found through the dbt tag.
   For manual use: a scoped run (--vars '{session_keys: [...]}') already replaces sessions by itself.
     dbt run-operation delete_sessions_from_incrementals --args '{sessions: [9158, 9159]}'
   Optional: --args '{sessions: [9158], tag: some_other_tag}' to target another group of incrementals. #}
{% macro delete_sessions_from_incrementals(sessions, tag='telemetry_group') %}
  {% if execute %}
    {% set keys = validate_session_keys(sessions) %}
    {% if keys | length > 0 %}
      {% set ids = keys | join(', ') %}
      {% for node in graph.nodes.values() | selectattr('resource_type', 'equalto', 'model') %}
        {% if tag in node.tags and node.config.materialized == 'incremental' %}
          {% set rel = adapter.get_relation(database=node.database, schema=node.schema, identifier=node.alias) %}
          {% if rel is not none %}
            {% do run_query("DELETE FROM " ~ rel ~ " WHERE session_key IN (" ~ ids ~ ")") %}
            {% do log("Deleted sessions " ~ ids ~ " from " ~ node.name, info=True) %}
          {% endif %}
        {% endif %}
      {% endfor %}
      {# run-operation does not auto-commit DML: without this the DELETE is silently rolled back #}
      {% do adapter.commit() %}
    {% endif %}
  {% endif %}
{% endmacro %}
