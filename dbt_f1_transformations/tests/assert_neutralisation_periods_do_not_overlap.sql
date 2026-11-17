{# Strict in CI; WARN on real data until proven there (an SC that follows a VSC, or a restart sequence, could in principle overlap). #}
{{ config(severity = 'error' if target.name == 'ci' else 'warn') }}
-- A VSC that is upgraded to a Safety Car (or a red flag) must END when the more severe period starts. Real data (2024 Shanghai) has no
-- 'VIRTUAL SAFETY CAR ENDING' for such a VSC, so without superseding it ran on beside the Safety Car.
SELECT a.session_key, a.track_status AS first_status, a.period_start AS first_start, a.period_end AS first_end,
       b.track_status AS second_status, b.period_start AS second_start
FROM {{ ref('int_track_status_periods') }} a
JOIN {{ ref('int_track_status_periods') }} b
  ON a.session_key = b.session_key AND b.period_start > a.period_start AND b.period_start < a.period_end
JOIN {{ ref('stg_sessions') }} s ON a.session_key = s.session_key
WHERE s.session_type IN ('Race', 'Sprint')   -- the FIA tests SC/VSC deployments in practice sessions: overlaps there are irrelevant
