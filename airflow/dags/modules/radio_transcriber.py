"""
Speech-to-text for team radio. OPT-IN and self-hosted (faster-whisper on CPU), in keeping with the rest of the project.

  raw_dir/team_radio_<session>_extracted.parquet        <- written by the extractor (metadata + recording_url)
  raw_dir/radio_transcripts_<session>_extracted.parquet <- written here

The transcripts file deliberately does NOT start with "team_radio_": that prefix is the glob of the team_radio source,
and a transcript file matching it would be mixed into the radio metadata table.

Needs `faster-whisper` (add it to AIRFLOW_EXTRA_PIP) and outbound HTTPS to the recording host. Clips already transcribed
are skipped, so re-runs only process new radio. A clip that fails to download or decode is recorded with its error
instead of failing the session. Transcription quality on noisy radio audio is imperfect and is NOT validated here.
"""
import logging
import os
import tempfile
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq

COLUMNS = ["session_key", "meeting_key", "driver_number", "date", "recording_url", "transcript", "language",
           "audio_duration_seconds", "model", "error", "_transcribed_at_utc"]


def default_download(url: str, dest: str) -> str:
    import requests
    with requests.get(url, timeout=60, stream=True) as r:
        r.raise_for_status()
        with open(dest, "wb") as fh:
            for chunk in r.iter_content(1 << 16):
                fh.write(chunk)
    return dest


def load_model(model_name: str = "small.en"):
    try:
        from faster_whisper import WhisperModel
    except ImportError as exc:
        raise RuntimeError("faster-whisper is not installed; add it to AIRFLOW_EXTRA_PIP to enable radio transcription") from exc
    return WhisperModel(model_name, device="cpu", compute_type="int8")


def _read(path):
    return pq.read_table(path).to_pylist() if os.path.exists(path) else []


def transcribe_session(session_key, raw_dir: str, model=None, download=None, model_name: str = "small.en", limit: int = None):
    """Transcribe all not-yet-transcribed radio clips of one session. Returns the transcripts file path, or None when
    the session has no radio metadata."""
    src = os.path.join(raw_dir, f"team_radio_{session_key}_extracted.parquet")
    out = os.path.join(raw_dir, f"radio_transcripts_{session_key}_extracted.parquet")
    clips = _read(src)
    if not clips:
        logging.info(f"No team radio metadata for session {session_key}; nothing to transcribe.")
        return None

    done_rows = _read(out)
    done = {r["recording_url"] for r in done_rows if r.get("recording_url")}
    todo = [c for c in clips if c.get("recording_url") and c["recording_url"] not in done]
    if limit:
        todo = todo[:limit]
    if not todo:
        logging.info(f"Session {session_key}: all {len(clips)} clips already transcribed.")
        return out

    model = model or load_model(model_name)
    download = download or default_download
    new_rows = []
    for clip in todo:
        row = {"session_key": clip.get("session_key"), "meeting_key": clip.get("meeting_key"),
               "driver_number": clip.get("driver_number"), "date": clip.get("date"), "recording_url": clip["recording_url"],
               "transcript": None, "language": None, "audio_duration_seconds": None, "model": model_name, "error": None,
               "_transcribed_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds")}
        tmp = None
        try:
            fd, tmp = tempfile.mkstemp(suffix=".mp3"); os.close(fd)
            download(clip["recording_url"], tmp)
            segments, info = model.transcribe(tmp, language="en", vad_filter=True)
            row["transcript"] = " ".join(s.text.strip() for s in segments).strip()
            row["language"] = getattr(info, "language", None)
            row["audio_duration_seconds"] = getattr(info, "duration", None)
        except Exception as exc:  # one bad clip must not lose the rest of the session
            logging.warning(f"Transcription failed for {clip['recording_url']}: {exc}")
            row["error"] = str(exc)[:300]
        finally:
            if tmp and os.path.exists(tmp):
                os.remove(tmp)
        new_rows.append(row)

    all_rows = [{k: r.get(k) for k in COLUMNS} for r in done_rows + new_rows]
    tmp_out = out + ".tmp"
    pq.write_table(pa.Table.from_pylist(all_rows), tmp_out, compression="snappy")
    os.replace(tmp_out, out)
    logging.info(f"Session {session_key}: transcribed {len(new_rows)} new clip(s), {sum(1 for r in new_rows if r['error'])} failed.")
    return out
