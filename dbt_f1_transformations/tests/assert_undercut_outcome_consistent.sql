-- The recorded outcome must agree with the signed gap: attacker_ahead_after is TRUE exactly when gap_after_seconds < 0.
SELECT session_key, attacker_driver_number, defender_driver_number, gap_after_seconds, attacker_ahead_after
FROM {{ ref('fct_undercut_overcut') }}
WHERE gap_after_seconds IS NOT NULL
  AND attacker_ahead_after <> (gap_after_seconds < 0)
