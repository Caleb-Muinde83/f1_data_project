{# Grain test: fails for every combination of the given columns that appears more than once.
   Dependency-free replacement for dbt_utils.unique_combination_of_columns. #}
{% test unique_combination_of_columns(model, combination_of_columns) %}
SELECT {{ combination_of_columns | join(', ') }}, COUNT(*) AS n_rows
FROM {{ model }}
GROUP BY {{ combination_of_columns | join(', ') }}
HAVING COUNT(*) > 1
{% endtest %}
