{# Typical cost of a stop at each circuit, split into green-flag and neutralised stops.
   neutralisation_discount_ratio = how much cheaper a stop is under SC/VSC/red (NULL until both kinds are observed). #}
WITH stops AS (
    SELECT p.*, s.circuit_key, s.year
    FROM {{ ref('fct_pit_stops') }} p
    JOIN {{ ref('dim_sessions') }} s ON p.session_key = s.session_key
),
medians AS (
    -- exact medians via window functions (BigQuery has no exact grouped median; APPROX_QUANTILES would only be approximate)
    SELECT
        *,
        {{ f1_median_over('CASE WHEN NOT is_under_neutralisation THEN pit_lane_time_seconds END', 'circuit_key') }} AS med_green_lane,
        {{ f1_median_over('CASE WHEN NOT is_under_neutralisation THEN time_lost_seconds END', 'circuit_key') }} AS med_green_lost,
        {{ f1_median_over('CASE WHEN is_under_neutralisation THEN time_lost_seconds END', 'circuit_key') }} AS med_neutralised_lost
    FROM stops
)
SELECT
    circuit_key,
    COUNT(*) AS stops_observed,
    COUNT(CASE WHEN NOT is_under_neutralisation THEN 1 END) AS green_stops,
    COUNT(CASE WHEN is_under_neutralisation THEN 1 END) AS neutralised_stops,
    MAX(med_green_lane) AS median_green_pit_lane_time_seconds,
    MAX(med_green_lost) AS median_green_time_lost_seconds,
    MAX(med_neutralised_lost) AS median_neutralised_time_lost_seconds,
    1 - MAX(med_neutralised_lost) / NULLIF(MAX(med_green_lost), 0) AS neutralisation_discount_ratio
FROM medians
GROUP BY circuit_key
