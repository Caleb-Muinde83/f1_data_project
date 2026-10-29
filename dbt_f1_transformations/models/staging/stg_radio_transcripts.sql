{# OPTIONAL source: when var enable_radio_transcripts is false (default) this is an empty, correctly typed table, so the
   project builds before any transcript file exists (a source glob with no files is a hard error in DuckDB). #}
{% if var('enable_radio_transcripts', false) %}
SELECT
    {{ f1_cast('session_key', 'int') }} AS session_key,
    {{ f1_cast('meeting_key', 'int') }} AS meeting_key,
    {{ f1_cast('driver_number', 'int') }} AS driver_number,
    {{ f1_cast('date', 'timestamp') }} AS radio_timestamp,
    recording_url,
    {{ f1_cast('transcript', 'string') }} AS transcript,
    {{ f1_cast('language', 'string') }} AS language,
    {{ f1_try_cast('audio_duration_seconds', 'double') }} AS audio_duration_seconds,
    {{ f1_cast('error', 'string') }} AS transcription_error
FROM {{ source('openf1_raw', 'radio_transcripts') }}
WHERE driver_number IS NOT NULL
{% else %}
SELECT
    {{ f1_cast('NULL', 'int') }} AS session_key,
    {{ f1_cast('NULL', 'int') }} AS meeting_key,
    {{ f1_cast('NULL', 'int') }} AS driver_number,
    {{ f1_cast('NULL', 'timestamp') }} AS radio_timestamp,
    {{ f1_cast('NULL', 'string') }} AS recording_url,
    {{ f1_cast('NULL', 'string') }} AS transcript,
    {{ f1_cast('NULL', 'string') }} AS language,
    {{ f1_cast('NULL', 'double') }} AS audio_duration_seconds,
    {{ f1_cast('NULL', 'string') }} AS transcription_error
FROM (SELECT 1) AS one_row
WHERE FALSE
{% endif %}
