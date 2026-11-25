"""Runs the dbt commands planned by modules.dbt_scope. Airflow-free, so it can be tested with a fake `runner`.

Safety properties:
  * every command is an argument list handed to subprocess.run, never a shell string (shell=False)
  * the exact command and scope are logged before each step runs
  * a failing step stops the run and raises DbtRunError with the tail of dbt's output
"""
import logging
import os
import shlex
import subprocess
import time


class DbtRunError(RuntimeError):
    """A dbt invocation exited non-zero. The DAG turns this into a failed task."""


def run_dbt_plan(steps, *, cwd: str, profiles_dir: str, scope: dict = None, runner=subprocess.run, log=logging) -> dict:
    """Run each planned step in order; return a summary dict (also safe to return from an Airflow task)."""
    env = {**os.environ, "DBT_PROFILES_DIR": profiles_dir}
    scope = scope or {}
    log.info("dbt scope: mode=%s keys=%s endpoints=%s (%s)", scope.get("mode"), scope.get("keys"),
             scope.get("endpoints"), scope.get("reason"))
    summary = {"mode": scope.get("mode"), "session_keys": scope.get("keys", []), "steps": []}
    for i, step in enumerate(steps, 1):
        argv = step["argv"]
        log.info("dbt step %d/%d [%s]: %s (cwd=%s)", i, len(steps), step["label"], shlex.join(argv), cwd)
        started = time.monotonic()
        result = runner(argv, capture_output=True, text=True, cwd=cwd, env=env, shell=False)
        seconds = round(time.monotonic() - started, 1)
        log.info(result.stdout or "")      # the whole dbt output: per-model timings are needed to see what is slow
        summary["steps"].append({"label": step["label"], "command": shlex.join(argv),
                                 "returncode": result.returncode, "seconds": seconds})
        if result.returncode != 0:
            log.error((result.stderr or "")[-3000:])
            raise DbtRunError(f"dbt step {i}/{len(steps)} [{step['label']}] failed with exit code {result.returncode}. "
                              f"Command: {shlex.join(argv)}. See the task log above.")
    return summary