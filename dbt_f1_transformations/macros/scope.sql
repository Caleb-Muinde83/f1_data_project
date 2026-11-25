{#
  SESSION SCOPE. Airflow tells dbt which sessions changed; dbt never guesses.

    dbt build --vars '{session_keys: [9158, 9159]}'

  * var `session_keys` unset or empty  -> no scope: everything behaves as before (manual runs, CI, dbt_scope=full). The
                                          telemetry tables then load only the sessions they do not have yet.
  * var `session_keys` set             -> the scoped staging views (stg_car_data, stg_location) expose only those sessions,
                                          and every model in the telemetry group REPLACES those sessions (never appends).
  * `telemetry_only_sessions` is the old name of the var and still works as an alias.

  RULE: only stg_car_data and stg_location are scoped views. A `table` model must never read them, because a table rebuilt from a
  scoped view silently loses every other session. ci/check_scoped_views.py enforces this on the manifest.
#}

{# Validated, sorted, de-duplicated list of integer session keys. Anything that is not a positive integer is an error:
   the keys are pasted into SQL, so they are never coerced. `raw` none -> []. #}
{% macro validate_session_keys(raw) %}
  {%- set keys = [] -%}
  {%- if raw is not none -%}
    {%- if raw is string or raw is mapping or raw is not iterable -%}
      {{ exceptions.raise_compiler_error("Session keys must be a list of integers, e.g. [9158, 9159]. Got: " ~ raw) }}
    {%- endif -%}
    {%- for k in raw -%}
      {%- if not modules.re.fullmatch('[0-9]+', k | string) or (k | int) <= 0 -%}
        {{ exceptions.raise_compiler_error("Invalid session key: " ~ k ~ ". Session keys are positive integers.") }}
      {%- endif -%}
      {%- do keys.append(k | int) -%}
    {%- endfor -%}
  {%- endif -%}
  {{ return(keys | unique | sort | list) }}
{% endmacro %}

{# The scope of this run: var session_keys, else the legacy alias telemetry_only_sessions, else [] (no scope). #}
{% macro scope_session_keys() %}
  {%- set raw = var('session_keys', none) -%}
  {%- if raw is none or (raw is not string and raw is not mapping and raw is iterable and raw | length == 0) -%}
    {%- set raw = var('telemetry_only_sessions', none) -%}
  {%- endif -%}
  {{ return(validate_session_keys(raw)) }}
{% endmacro %}

{% macro is_scoped() %}{{ return(scope_session_keys() | length > 0) }}{% endmacro %}

{# `col IN (...)` when a scope is set, otherwise the always-true `1 = 1`. Safe to AND into any WHERE. #}
{% macro in_scope(col) -%}
  {%- set keys = scope_session_keys() -%}
  {%- if keys | length > 0 -%}{{ col }} IN ({{ keys | join(', ') }}){%- else -%}1 = 1{%- endif -%}
{%- endmacro %}

{#
  The row filter of every telemetry-group model that reads PERSISTED or scoped telemetry:
    scope set                     -> only the scoped sessions (the model REPLACES them: see scoped_pre_delete)
    no scope, incremental run     -> only sessions this table does not hold yet
    no scope, first build/refresh -> everything
  --full-refresh together with a scope would rebuild the table from the scoped sessions ONLY and drop all the others: refused.
#}
{% macro telemetry_load_filter(col='session_key') -%}
  {%- if execute and flags.FULL_REFRESH and is_scoped() -%}
    {{ exceptions.raise_compiler_error("--full-refresh together with session_keys would rebuild the telemetry tables from the scoped sessions only and silently drop every other session. Unset session_keys for a full refresh, or drop --full-refresh for a scoped run.") }}
  {%- endif -%}
  {%- if is_scoped() -%}{{ in_scope(col) }}
  {%- elif is_incremental() -%}{{ col }} NOT IN (SELECT DISTINCT session_key FROM {{ this }})
  {%- else -%}1 = 1{%- endif -%}
{%- endmacro %}

{# pre_hook for the incremental telemetry-group models. delete+insert already replaces sessions present in the new rows; this also
   clears a scoped session whose upstream rows have vanished, which delete+insert alone would leave behind as stale data.
   Empty (no statement) unless the table exists and a scope is set. #}
{% macro scoped_pre_delete() -%}
  {%- if is_incremental() and is_scoped() -%}DELETE FROM {{ this }} WHERE {{ in_scope('session_key') }}{%- endif -%}
{%- endmacro %}

{# For generic tests on heavy tables: the relation to test, restricted to the scoped sessions when a scope is set. #}
{% macro scoped_test_relation(model, scope_column) -%}
  {%- if scope_column and is_scoped() -%}(SELECT * FROM {{ model }} WHERE {{ in_scope(scope_column) }}) f1_scoped
  {%- else -%}{{ model }}{%- endif -%}
{%- endmacro %}
