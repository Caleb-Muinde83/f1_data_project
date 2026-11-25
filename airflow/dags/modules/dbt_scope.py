"""Airflow-free helpers: turn extraction results into a dbt *scope*, and a scope into dbt commands.

Design rules (see docs/SESSION_SCOPED_BUILDS.md):
  1. The run tells dbt what changed. dbt never guesses: the scope is passed as the `session_keys` var.
  2. An unset scope behaves as before (manual runs, CI and dbt_scope=full).
  3. A scoped session is replaced, never appended (enforced by the dbt models, not here).
  4. No `table` model may read scoped data (enforced by dbt/ci/check_scoped_views.py).

Nothing here imports Airflow or touches dbt, so it is unit tested like the other helpers.
"""
import glob
import json
import os
import re

TELEMETRY_TAG = "telemetry_group"      # dbt tag on stg_car_data, stg_location and every model downstream of them
DEFAULT_CHUNK_SIZE = 10
SCOPE_MODES = ("none", "scoped", "full", "full_refresh")
DBT_SCOPE_CHOICES = ("auto", "full")

_DIGITS = re.compile(r"[0-9]+")


class ScopeError(ValueError):
    """The requested scope is invalid or unsafe. The DAG turns this into a failed task."""


# ---------------------------------------------------------------------------------------------
# validation
# ---------------------------------------------------------------------------------------------
def validate_session_keys(keys) -> list:
    """Return sorted, de-duplicated positive ints. Anything else is rejected, never coerced:
    the keys end up inside SQL (`session_key IN (...)`), so '9158; DROP' or 9158.5 must not get through."""
    if keys is None or isinstance(keys, (str, bytes, dict)):
        raise ScopeError(f"session keys must be a list of integers, got {type(keys).__name__}")
    out = set()
    for k in keys:
        if isinstance(k, bool):
            raise ScopeError(f"invalid session key {k!r}")
        if isinstance(k, int):
            value = k
        elif isinstance(k, str) and _DIGITS.fullmatch(k.strip()):
            value = int(k.strip())
        else:
            raise ScopeError(f"invalid session key {k!r}: session keys are integers")
        if value <= 0:
            raise ScopeError(f"invalid session key {k!r}: must be positive")
        out.add(value)
    return sorted(out)


def validate_chunk_size(size) -> int:
    if isinstance(size, bool) or not isinstance(size, int) or size < 1:
        raise ScopeError(f"dbt_chunk_size must be an integer >= 1, got {size!r}")
    return size


def chunk_keys(keys, size) -> list:
    keys = list(keys)
    size = validate_chunk_size(size)
    return [keys[i:i + size] for i in range(0, len(keys), size)]


def flatten_results(*groups) -> list:
    """Flatten Airflow's mapped-task outputs into one list of extractor result dicts.

    The DAG passes lists of mapped-task outputs; Airflow resolves each mapped output to a LAZY SEQUENCE, which is not a list or
    tuple, so this recurses into anything iterable (except dicts and strings). None entries (skipped mapped instances) are dropped.
    Any other leaf is an error, never silently ignored: dropping results hides exactly the data the dbt scope is built from.
    """
    flat = []

    def walk(item):
        if item is None:
            return
        if isinstance(item, dict):
            flat.append(item)
        elif isinstance(item, (str, bytes)) or not hasattr(item, "__iter__"):
            raise TypeError(f"unexpected extractor result of type {type(item).__name__}: {item!r}; expected a result dict")
        else:
            for sub in item:
                walk(sub)

    for group in groups:
        walk(group)
    return flat


# ---------------------------------------------------------------------------------------------
# scope builder
# ---------------------------------------------------------------------------------------------
def build_dbt_scope(results, sessions=None, *, dbt_scope="auto", dbt_full_refresh=False,
                    chunk_size=DEFAULT_CHUNK_SIZE, allow_large_backfill=False, non_telemetry_changed=False) -> dict:
    """Decide what dbt should build.

    results   : list of extractor results {endpoint, session_key, path, status} (None / non-dicts are ignored)
    sessions  : session keys this run selected (optional). When given, only these can end up in the scope.
    returns   : {mode, keys, endpoints, chunks, reason}
        mode 'none'         nothing was written: skip dbt
        mode 'scoped'       `keys` = sessions with at least one file actually written; `chunks` = those keys in chunks
        mode 'full'         dbt_scope=full: whole graph, scope unset (telemetry loads only the sessions it lacks)
        mode 'full_refresh' deliberate rebuild of everything; refused unless allow_large_backfill is set
    non_telemetry_changed: something outside the extractor results changed (e.g. radio transcripts were produced). When
    nothing was written but this is true, the non-telemetry models still rebuild (scoped mode with no session keys).
    """
    if dbt_scope not in DBT_SCOPE_CHOICES:
        raise ScopeError(f"dbt_scope must be one of {DBT_SCOPE_CHOICES}, got {dbt_scope!r}")
    chunk_size = validate_chunk_size(chunk_size)

    written = [r for r in (results or []) if isinstance(r, dict) and r.get("status") == "written"]
    endpoints = sorted({r["endpoint"] for r in written if r.get("endpoint")})

    if dbt_full_refresh:
        if not allow_large_backfill:
            raise ScopeError("dbt_full_refresh rebuilds every model, including the 200M+ row telemetry tables. "
                             "Tick allow_large_backfill to run it deliberately.")
        return {"mode": "full_refresh", "keys": [], "endpoints": endpoints, "chunks": [],
                "reason": "dbt_full_refresh=true"}

    if dbt_scope == "full":
        return {"mode": "full", "keys": [], "endpoints": endpoints, "chunks": [],
                "reason": "dbt_scope=full: whole graph, telemetry loads only missing sessions"}

    if not written and non_telemetry_changed:
        return {"mode": "scoped", "keys": [], "endpoints": [], "chunks": [],
                "reason": "no extractor file was written, but other inputs changed (radio transcripts)"}

    if not written:
        return {"mode": "none", "keys": [], "endpoints": [], "chunks": [],
                "reason": "no file was written in this run, so there is nothing to transform"}

    wanted = None if sessions is None else set(validate_session_keys(sessions))
    keys = set()
    for r in written:
        sk = r.get("session_key")
        if sk == "global" or sk is None:
            continue                      # global files (meetings, drivers, sessions) are not session scoped
        sk = validate_session_keys([sk])[0]
        if wanted is None or sk in wanted:
            keys.add(sk)
    keys = sorted(keys)
    return {"mode": "scoped", "keys": keys, "endpoints": endpoints, "chunks": chunk_keys(keys, chunk_size),
            "reason": f"{len(keys)} session(s) with written files"
                      if keys else "only global files were written; no session telemetry to load"}


# ---------------------------------------------------------------------------------------------
# dbt command planning
# ---------------------------------------------------------------------------------------------
def radio_transcripts_exist(raw_dir: str) -> bool:
    """enable_radio_transcripts must be true only when the files exist: a missing glob is a hard error in DuckDB."""
    return bool(glob.glob(os.path.join(raw_dir, "radio_transcripts_*_extracted.parquet")))


def _vars_arg(variables: dict) -> list:
    # JSON is valid YAML, which is what dbt parses --vars as. Compact separators: no shell, so no quoting needed.
    return ["--vars", json.dumps(variables, separators=(",", ":"), sort_keys=True)] if variables else []


def plan_dbt_commands(scope: dict, *, target: str, profiles_dir: str, radio_transcripts: bool = False,
                      chunk_size: int = DEFAULT_CHUNK_SIZE, dbt_executable: str = "dbt") -> list:
    """Return the ordered dbt invocations as [{label, argv, session_keys}]. argv is a list: it is never joined into a shell string.

    scoped, one chunk of sessions : ONE whole-graph build with session_keys set. Telemetry group replaces those
                                    sessions; every other model does its normal cheap full rebuild.
    scoped, many chunks           : first the non-telemetry models once (the telemetry group reads fct_fastest_laps,
                                    int_lap_context and stg_laps, so they must be fresh first), then the telemetry
                                    group once per chunk of sessions.
    scoped, no session keys       : non-telemetry models only (only global files changed).
    """
    mode = scope.get("mode")
    if mode not in SCOPE_MODES:
        raise ScopeError(f"unknown scope mode {mode!r}")
    if mode == "none":
        return []

    base = [dbt_executable, "build", "--target", target, "--profiles-dir", profiles_dir]
    common = {"enable_radio_transcripts": True} if radio_transcripts else {}
    tag = f"tag:{TELEMETRY_TAG}"

    def step(label, extra, variables=None, keys=None):
        return {"label": label, "argv": base + extra + _vars_arg({**common, **(variables or {})}),
                "session_keys": keys or []}

    if mode == "full":
        return [step("whole graph (scope unset)", [])]
    if mode == "full_refresh":
        return [step("FULL REFRESH of the whole graph", ["--full-refresh"])]

    keys = validate_session_keys(scope.get("keys", []))
    if not keys:
        return [step("non-telemetry models (no session scope)", ["--exclude", tag])]
    chunks = chunk_keys(keys, validate_chunk_size(chunk_size))
    if len(chunks) == 1:
        return [step(f"whole graph, scoped to {len(keys)} session(s)", [], {"session_keys": keys}, keys)]
    steps = [step("non-telemetry models", ["--exclude", tag])]
    for i, chunk in enumerate(chunks, 1):
        steps.append(step(f"telemetry group, chunk {i}/{len(chunks)} ({len(chunk)} sessions)",
                          ["--select", tag], {"session_keys": chunk}, chunk))
    return steps