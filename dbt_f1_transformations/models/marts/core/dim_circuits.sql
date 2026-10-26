{# One row per circuit. NOTE: the OpenF1 API exposes no corner / sector geometry, so corner-level analysis
   needs an external reference table (see README, "Known limitations"). #}
SELECT
    circuit_key,
    MAX(circuit_short_name) AS circuit_short_name,
    MAX(location) AS location,
    MAX(country_code) AS country_code,
    MIN(year) AS first_year,
    MAX(year) AS last_year,
    COUNT(*) AS sessions_held,
    COUNT(*) FILTER (WHERE session_type = 'Race') AS races_held
FROM {{ ref('dim_sessions') }}
WHERE circuit_key IS NOT NULL
GROUP BY circuit_key
