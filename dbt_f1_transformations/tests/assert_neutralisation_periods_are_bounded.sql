-- SC/VSC periods without a closing message are capped; a VSC lasting longer than the cap means the cap is not applied.
SELECT session_key, track_status, period_start, period_end
FROM {{ ref('int_track_status_periods') }}
WHERE (track_status = 'VSC' AND {{ f1_epoch_ms('period_end') }} - {{ f1_epoch_ms('period_start') }} > 1000 * ({{ var('max_unclosed_vsc_seconds', 600) }} + 1)
                              AND is_unclosed)
   OR (track_status = 'SC'  AND {{ f1_epoch_ms('period_end') }} - {{ f1_epoch_ms('period_start') }} > 1000 * ({{ var('max_unclosed_sc_seconds', 1200) }} + 1)
                              AND is_unclosed)
