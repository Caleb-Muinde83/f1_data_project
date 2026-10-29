import os
from types import SimpleNamespace
import pyarrow as pa, pyarrow.parquet as pq


class FakeModel:
    calls = []
    def transcribe(self, path, **kw):
        FakeModel.calls.append(path)
        text = open(path, "rb").read().decode()
        return [SimpleNamespace(text=f" {text} "), SimpleNamespace(text="over")], SimpleNamespace(language="en", duration=4.2)


def fake_download(url, dest):
    if "bad" in url:
        raise IOError("404 not found")
    open(dest, "wb").write(url.rsplit("/", 1)[-1].encode())
    return dest


def _write_radio(tmp_path, session, urls):
    rows = [{"session_key": session, "meeting_key": 1, "driver_number": 44, "date": "2024-03-02T15:10:00+00:00", "recording_url": u} for u in urls]
    pq.write_table(pa.Table.from_pylist(rows), tmp_path / f"team_radio_{session}_extracted.parquet")


def test_transcribes_new_clips_records_failures_and_is_incremental(tmp_path):
    from modules.radio_transcriber import transcribe_session
    _write_radio(tmp_path, 9158, ["http://x/box-box", "http://x/bad-clip"])
    FakeModel.calls = []
    out = transcribe_session(9158, str(tmp_path), model=FakeModel(), download=fake_download)
    assert os.path.basename(out) == "radio_transcripts_9158_extracted.parquet"      # not matched by the team_radio_* glob
    rows = {r["recording_url"]: r for r in pq.read_table(out).to_pylist()}
    assert rows["http://x/box-box"]["transcript"] == "box-box over" and rows["http://x/box-box"]["language"] == "en"
    assert rows["http://x/bad-clip"]["transcript"] is None and "404" in rows["http://x/bad-clip"]["error"]
    n_before = len(FakeModel.calls)
    transcribe_session(9158, str(tmp_path), model=FakeModel(), download=fake_download)   # nothing new
    assert len(FakeModel.calls) == n_before
    _write_radio(tmp_path, 9158, ["http://x/box-box", "http://x/bad-clip", "http://x/new-one"])
    transcribe_session(9158, str(tmp_path), model=FakeModel(), download=fake_download)
    assert len(FakeModel.calls) == n_before + 1 and pq.read_table(out).num_rows == 3


def test_no_radio_metadata_returns_none(tmp_path):
    from modules.radio_transcriber import transcribe_session
    assert transcribe_session(1, str(tmp_path), model=FakeModel(), download=fake_download) is None


def test_missing_whisper_gives_actionable_error(monkeypatch):
    import builtins, pytest
    real = builtins.__import__
    monkeypatch.setattr(builtins, "__import__", lambda n, *a, **k: (_ for _ in ()).throw(ImportError()) if n == "faster_whisper" else real(n, *a, **k))
    from modules.radio_transcriber import load_model
    with pytest.raises(RuntimeError, match="AIRFLOW_EXTRA_PIP"):
        load_model()
