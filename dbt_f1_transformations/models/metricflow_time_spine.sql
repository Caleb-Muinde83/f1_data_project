{{ config(materialized='table') }}
{# Daily calendar required by MetricFlow for time-based metrics. #}
SELECT CAST(date_day AS DATE) AS date_day
FROM ({{ dbt.date_spine('day', "CAST('2023-01-01' AS DATE)", "CAST('2032-01-01' AS DATE)") }}) AS spine
