{#
  Transcribed team radio with race context (lap, tyre, track status, position) and KEYWORD tags.
  The tags are plain substring heuristics on the transcript, NOT a language model: they will miss paraphrases and can
  fire on unrelated words. Treat them as a way to find messages to read, not as ground truth. Transcript text itself
  comes from speech-to-text on noisy radio audio and can be wrong. Empty unless var enable_radio_transcripts is true.
#}
WITH msgs AS (
    SELECT
        session_key, meeting_key, driver_number, radio_timestamp, recording_url,
        transcript, language, audio_duration_seconds, transcription_error,
        LOWER(COALESCE(transcript, '')) AS t
    FROM {{ ref('stg_radio_transcripts') }}
),
with_context AS (
    {{ f1_asof_join(
        left='msgs', right=ref('int_lap_context'),
        on=['session_key', 'driver_number'], left_ts='radio_timestamp', right_ts='lap_start_ts',
        left_cols=['session_key', 'meeting_key', 'driver_number', 'radio_timestamp', 'recording_url', 'transcript',
                   'language', 'audio_duration_seconds', 'transcription_error', 't'],
        right_cols=['lap_number', 'tyre_compound', 'tyre_age_this_lap', 'track_status', 'position_start']
    ) }}
)
SELECT
    c.session_key, c.meeting_key, c.driver_number,
    d.full_name AS driver_name, d.team_name,
    c.radio_timestamp, c.recording_url, c.transcript, c.language, c.audio_duration_seconds, c.transcription_error,
    c.lap_number, c.tyre_compound, c.tyre_age_this_lap, c.track_status, c.position_start,
    (c.t LIKE '%box%' OR c.t LIKE '%pit this lap%' OR c.t LIKE '%pit now%' OR c.t LIKE '%come in%') AS mentions_pit_call,
    (c.t LIKE '%tyre%' OR c.t LIKE '%tire%' OR c.t LIKE '%grain%' OR c.t LIKE '%degrad%' OR c.t LIKE '%blister%') AS mentions_tyres,
    (c.t LIKE '%safety car%' OR c.t LIKE '%virtual%' OR c.t LIKE '%vsc%' OR c.t LIKE '%red flag%' OR c.t LIKE '%yellow%') AS mentions_neutralisation,
    (c.t LIKE '%drs%') AS mentions_drs,
    (c.t LIKE '%penalt%' OR c.t LIKE '%investigat%' OR c.t LIKE '%stewards%') AS mentions_penalty,
    (c.t LIKE '%fuel%' OR c.t LIKE '%lift and coast%' OR c.t LIKE '%engine%' OR c.t LIKE '%battery%' OR c.t LIKE '%deploy%') AS mentions_power_unit
FROM with_context c
LEFT JOIN {{ ref('dim_driver_session') }} d
    ON c.session_key = d.session_key AND c.driver_number = d.driver_number
