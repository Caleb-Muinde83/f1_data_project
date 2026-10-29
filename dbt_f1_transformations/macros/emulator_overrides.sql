{#
  TEST-ONLY. The BigQuery emulator answers "CREATE SCHEMA IF NOT EXISTS" for an existing dataset with an internal error that the
  Google client then retries for minutes. When the target is the emulator, the test harness creates the dataset up front and
  dbt skips this step. Every other target (including the real `bigquery` one) uses dbt-bigquery's normal behaviour.
#}
{% macro bigquery__create_schema(relation) -%}
  {%- if target.name == 'bigquery_emulator' -%}
    {# dataset pre-created by ci/ harness #}
  {%- else -%}
    {{ adapter.create_schema(relation) }}
  {%- endif -%}
{%- endmacro %}
