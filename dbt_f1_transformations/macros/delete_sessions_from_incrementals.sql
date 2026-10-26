{# Remove sessions from the incremental telemetry tables so the next `dbt run` rebuilds them from fresh raw files.
   dbt run-operation delete_sessions_from_incrementals --args '{sessions: [9158, 9159]}' #}
{% macro delete_sessions_from_incrementals(sessions) %}
  {% if execute and sessions | length > 0 %}
    {% set ids = sessions | map('int') | join(', ') %}
    {% for model_name in ['fct_telemetry', 'fct_telemetry_distance_trace'] %}
      {% set rel = adapter.get_relation(database=target.database, schema=target.schema, identifier=model_name) %}
      {% if rel is not none %}
        {% do run_query("DELETE FROM " ~ rel ~ " WHERE session_key IN (" ~ ids ~ ")") %}
        {% do log("Deleted sessions " ~ ids ~ " from " ~ model_name, info=True) %}
      {% endif %}
    {% endfor %}
    {# run-operation does not auto-commit DML: without this the DELETE is silently rolled back #}
    {% do adapter.commit() %}
  {% endif %}
{% endmacro %}
