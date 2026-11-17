import logging
import os
from pathlib import Path
import requests
from airflow.configuration import conf

log = logging.getLogger(__name__)

MAX_LOG_CHARS = 2400  # Keeps Slack section blocks safely under Slack's 3,000-char limit


def _read_task_log_tail(ti) -> str:
    base_log_folder = conf.get("logging", "base_log_folder", fallback="/opt/airflow/logs")
    parts = [
        base_log_folder,
        f"dag_id={ti.dag_id}",
        f"run_id={ti.run_id}",
        f"task_id={ti.task_id}",
    ]
    map_index = getattr(ti, "map_index", -1)
    if map_index is not None and map_index >= 0:
        parts.append(f"map_index={map_index}")

    log_dir = Path(*parts)
    candidate = log_dir / f"attempt={ti.try_number}.log"

    if not candidate.exists() and log_dir.exists():
        attempts = sorted(log_dir.glob("attempt=*.log"))
        if attempts:
            candidate = attempts[-1]

    if not candidate.exists():
        return f"Log file not found at {candidate} (task may have been killed before writing logs)."

    text = candidate.read_text(encoding="utf-8", errors="replace")
    if len(text) > MAX_LOG_CHARS:
        return "... [truncated] ...\n" + text[-MAX_LOG_CHARS:]
    return text or "(empty log file)"


def send_slack_failure_alert(context: dict) -> None:
    ti = context["task_instance"]
    if ti.try_number <= ti.max_tries:
        return

    webhook_url = os.getenv("ALERT_WEBHOOK_URL", "").strip()
    if not webhook_url:
        log.warning("ALERT_WEBHOOK_URL is not set; skipping Slack failure alert.")
        return

    exception = context.get("exception")
    map_index = getattr(ti, "map_index", -1)
    task_label = f"{ti.task_id}[{map_index}]" if map_index is not None and map_index >= 0 else ti.task_id
    log_tail = _read_task_log_tail(ti)

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f":x: Airflow Task Failed: {ti.dag_id}.{task_label}",
            },
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*DAG:*\n`{ti.dag_id}`"},
                {"type": "mrkdwn", "text": f"*Task:*\n`{task_label}`"},
                {"type": "mrkdwn", "text": f"*Run ID:*\n`{ti.run_id}`"},
                {"type": "mrkdwn", "text": f"*Attempt:*\n`{ti.try_number}`"},
            ],
        },
    ]

    if exception:
        exc_text = str(exception)
        if len(exc_text) > 500:
            exc_text = exc_text[:500] + "..."
        blocks.append(
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Exception:*\n`{exc_text}`"},
            }
        )

    blocks.extend(
        [
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Log Tail:*\n```{log_tail}```"},
            },
            {
                "type": "context",
                "elements": [
                    {"type": "mrkdwn", "text": f"<{ti.log_url}|Open Task Log in Airflow UI>"},
                ],
            },
        ]
    )

    try:
        resp = requests.post(webhook_url, json={"blocks": blocks}, timeout=10)
        resp.raise_for_status()
    except Exception as err:
        log.error("Failed to send Slack alert: %s", err)