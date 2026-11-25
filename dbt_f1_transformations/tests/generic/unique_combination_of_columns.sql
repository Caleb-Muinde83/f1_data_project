{# Grain test: fails for every combination of the given columns that appears more than once.
   Dependency-free replacement for dbt_utils.unique_combination_of_columns.
   scope_column (optional): for heavy tables, name the session column. When var session_keys is set, only those sessions are checked
   (a scoped run can only change those sessions); with no scope the whole table is checked as before. #}
{% test unique_combination_of_columns(model, combination_of_columns, scope_column=none) %}
SELECT {{ combination_of_columns | join(', ') }}, COUNT(*) AS n_rows
FROM {{ scoped_test_relation(model, scope_column) }}
GROUP BY {{ combination_of_columns | join(', ') }}
HAVING COUNT(*) > 1
{% endtest %}
