{# not_null that honours the session scope: with var session_keys set only those sessions are scanned (fct_telemetry has 200M+ rows),
   with no scope it checks the whole table, exactly like the built-in not_null. #}
{% test not_null_in_scope(model, column_name, scope_column='session_key') %}
SELECT {{ column_name }}
FROM {{ scoped_test_relation(model, scope_column) }}
WHERE {{ column_name }} IS NULL
{% endtest %}
