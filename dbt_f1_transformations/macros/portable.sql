{#
  Cross-database SQL helpers. DuckDB is the default dialect (`default__`); BigQuery overrides are `bigquery__`.
  Models call these instead of writing DuckDB-only syntax directly (TRY_CAST, epoch(), REGR_SLOPE, MEDIAN, ASOF JOIN,
  FILTER (WHERE ...), LIST(), json_extract_string, regexp_extract, BOOL_OR, ARG_MAX).
  Dialect is `target.type`, overridable with --vars '{f1_dialect: bigquery}' for compile-only checks.
#}
{% macro f1_dialect() %}{{ return(var('f1_dialect', target.type)) }}{% endmacro %}
{% macro f1_is_bq() %}{{ return(f1_dialect() == 'bigquery') }}{% endmacro %}

{# ---- types & casts ---- #}
{% macro f1_type(dtype) %}
  {%- set bq = {'int': 'INT64', 'double': 'FLOAT64', 'string': 'STRING', 'timestamp': 'TIMESTAMP', 'bool': 'BOOL'} -%}
  {%- set dd = {'int': 'INTEGER', 'double': 'DOUBLE', 'string': 'VARCHAR', 'timestamp': 'TIMESTAMP', 'bool': 'BOOLEAN'} -%}
  {{- bq[dtype] if f1_is_bq() else dd[dtype] -}}
{% endmacro %}
{% macro f1_try_cast_fn() %}{{ 'SAFE_CAST' if f1_is_bq() else 'TRY_CAST' }}{% endmacro %}
{% macro f1_cast(expr, dtype) %}CAST({{ expr }} AS {{ f1_type(dtype) }}){% endmacro %}
{% macro f1_try_cast(expr, dtype) %}{{ 'SAFE_CAST' if f1_is_bq() else 'TRY_CAST' }}({{ expr }} AS {{ f1_type(dtype) }}){% endmacro %}

{# ---- time ---- #}
{% macro f1_seconds_between(start_ts, end_ts) %}
  {%- if f1_is_bq() -%}TIMESTAMP_DIFF({{ end_ts }}, {{ start_ts }}, MICROSECOND) / 1000000.0
  {%- else -%}epoch({{ end_ts }} - {{ start_ts }}){%- endif -%}
{% endmacro %}
{% macro f1_ts_add_seconds(ts, secs) %}
  {%- if f1_is_bq() -%}TIMESTAMP_ADD({{ ts }}, INTERVAL CAST(ROUND(({{ secs }}) * 1000000) AS INT64) MICROSECOND)
  {%- else -%}({{ ts }} + ({{ secs }}) * INTERVAL 1 SECOND){%- endif -%}
{% endmacro %}
{% macro f1_epoch_us(ts) %}{{ 'UNIX_MICROS' if f1_is_bq() else 'epoch_us' }}({{ ts }}){% endmacro %}
{% macro f1_epoch_ms(ts) %}{{ 'UNIX_MILLIS' if f1_is_bq() else 'epoch_ms' }}({{ ts }}){% endmacro %}

{# ---- aggregates / windows ---- #}
{% macro f1_median_over(expr, partition_by) %}
  {%- if f1_is_bq() -%}PERCENTILE_CONT({{ expr }}, 0.5) OVER (PARTITION BY {{ partition_by }})
  {%- else -%}MEDIAN({{ expr }}) OVER (PARTITION BY {{ partition_by }}){%- endif -%}
{% endmacro %}
{# y, x must already be NULL-masked identically by the caller (CASE WHEN keep THEN value END for both). #}
{% macro f1_regr_slope(y, x) %}
  {%- if f1_is_bq() -%}SAFE_DIVIDE(COVAR_POP({{ y }}, {{ x }}), VAR_POP({{ x }})){%- else -%}REGR_SLOPE({{ y }}, {{ x }}){%- endif -%}
{% endmacro %}
{% macro f1_regr_intercept(y, x) %}
  {%- if f1_is_bq() -%}(AVG({{ y }}) - SAFE_DIVIDE(COVAR_POP({{ y }}, {{ x }}), VAR_POP({{ x }})) * AVG({{ x }}))
  {%- else -%}REGR_INTERCEPT({{ y }}, {{ x }}){%- endif -%}
{% endmacro %}
{% macro f1_r2(y, x) %}POWER(CORR({{ y }}, {{ x }}), 2){% endmacro %}
{% macro f1_list_agg(expr, order_by) %}{{ 'ARRAY_AGG' if f1_is_bq() else 'LIST' }}({{ expr }} ORDER BY {{ order_by }}){% endmacro %}
{% macro f1_bool_or(expr) %}{{ 'LOGICAL_OR' if f1_is_bq() else 'BOOL_OR' }}({{ expr }}){% endmacro %}
{% macro f1_arg_max(value, by) %}
  {%- if f1_is_bq() -%}ARRAY_AGG({{ value }} ORDER BY {{ by }} DESC LIMIT 1)[OFFSET(0)]
  {%- else -%}ARG_MAX({{ value }}, {{ by }}){%- endif -%}
{% endmacro %}
{% macro f1_json_scalar(expr, path) %}
  {%- if f1_is_bq() -%}JSON_EXTRACT_SCALAR({{ expr }}, '{{ path }}'){%- else -%}json_extract_string({{ expr }}, '{{ path }}'){%- endif -%}
{% endmacro %}
{# pattern is a plain regex with ONE capture group; returns that group (or NULL). #}
{% macro f1_regexp_extract(expr, pattern) %}
  {%- if f1_is_bq() -%}REGEXP_EXTRACT({{ expr }}, r'{{ pattern }}')
  {%- else -%}NULLIF(regexp_extract({{ expr }}, '{{ pattern }}', 1), ''){%- endif -%}
{% endmacro %}

{#
  ASOF LEFT JOIN: attach to every `left` row the latest `right` row with the same keys and right_ts <= left_ts.
    left / right : relation or CTE names
    on           : equality key column names (must exist in BOTH, and be in left_cols)
    left_cols    : left columns to carry through (include the keys)
    right_cols   : right columns to attach, either 'col' or 'col AS alias'
  DuckDB uses its native ASOF JOIN. BigQuery has none, so the same result is built in LINEAR time by merging both
  sides onto one timeline, carrying the latest right-side timestamp forward with a running MAX over integer microseconds,
  then joining back to the right table on (keys, that timestamp). This keeps legitimately-NULL right values intact and works
  on engines without LAST_VALUE ... IGNORE NULLS. Right rows win ties (right_ts = left_ts). Right rows are de-duplicated
  per (keys, right_ts) first, so duplicate readings cannot multiply left rows.
#}
{% macro f1_asof_join(left, right, on, left_ts, right_ts, left_cols, right_cols) %}
  {%- set rc = [] -%}
  {%- for c in right_cols -%}
    {%- set parts = c.split(' AS ') if ' AS ' in c else [c, c] -%}
    {%- do rc.append({'src': parts[0] | trim, 'alias': parts[1] | trim}) -%}
  {%- endfor -%}
  {%- if f1_is_bq() -%}
    SELECT
      {% for c in left_cols %}m.{{ c }},{% endfor %}
      {% for r in rc %}r.{{ r.src }} AS {{ r.alias }}{{ ',' if not loop.last }}{% endfor %}
    FROM (
      SELECT
        {% for c in left_cols %}{{ c }},{% endfor %} _side,
        MAX(CASE WHEN _side = 1 THEN _ts END) OVER (
          PARTITION BY {{ on | join(', ') }} ORDER BY _ts, _side DESC
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS _last_ts
      FROM (
        SELECT 0 AS _side, {% for c in left_cols %}{{ c }},{% endfor %} UNIX_MICROS({{ left_ts }}) AS _ts
        FROM {{ left }}
        UNION ALL
        SELECT 1 AS _side, {% for c in left_cols %}{{ c if c in on else 'NULL' }} AS {{ c }},{% endfor %} UNIX_MICROS({{ right_ts }}) AS _ts
        FROM {{ right }}
        WHERE {{ right_ts }} IS NOT NULL
      )
    ) m
    LEFT JOIN (
      SELECT * EXCEPT (_rn) FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY {{ on | join(', ') }}, {{ right_ts }} ORDER BY {{ right_ts }}) AS _rn
        FROM {{ right }}
      ) WHERE _rn = 1
    ) r
      ON {% for k in on %}r.{{ k }} = m.{{ k }} AND {% endfor %}UNIX_MICROS(r.{{ right_ts }}) = m._last_ts
    WHERE m._side = 0
  {%- else -%}
    SELECT
      {% for c in left_cols %}l.{{ c }},{% endfor %}
      {% for r in rc %}r.{{ r.src }} AS {{ r.alias }}{{ ',' if not loop.last }}{% endfor %}
    FROM {{ left }} l
    ASOF LEFT JOIN {{ right }} r
      ON {% for k in on %}l.{{ k }} = r.{{ k }} AND {% endfor %}l.{{ left_ts }} >= r.{{ right_ts }}
  {%- endif -%}
{% endmacro %}
