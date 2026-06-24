"""
slack_alert.py — Airflow on_failure_callback로 쓰는 재사용 Slack 실패 경보.

DAG default_args에 on_failure_callback=slack_failure_callback로 걸면, 태스크가 (재시도까지
소진하고) 실패할 때 SLACK_WEBHOOK_URL 채널로 경보를 보낸다. 운영자는 평소 무알림, 문제 시 핑.

(DAG 객체가 없으므로 스케줄러가 잡으로 등록하지 않는다. 같은 dags 폴더라 다른 DAG가 import만 한다.)
"""

import json
import os
import socket
import urllib.request
from datetime import timedelta, timezone

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")
APP_ENV = os.environ.get("APP_ENV", "dev")
KST = timezone(timedelta(hours=9))


def _post(payload: dict) -> None:
    """SLACK_WEBHOOK_URL이 있으면 POST. 미설정 시 skip, 실패해도 잡을 죽이지 않는다."""
    if not SLACK_WEBHOOK_URL:
        return
    try:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            SLACK_WEBHOOK_URL, data=data, headers={"Content-Type": "application/json"}
        )
        urllib.request.urlopen(req, timeout=10)
        print("[INFO] Slack 실패 경보 전송 완료")
    except Exception as e:  # 경보 실패가 콜백/잡을 죽이면 안 됨
        print(f"[WARN] Slack 경보 전송 실패: {e}")


def slack_failure_callback(context: dict) -> None:
    """Airflow 태스크 실패 시 호출. context에서 dag/task/시각/로그URL/예외를 뽑아 경보 전송."""
    ti = context.get("task_instance")
    dag = context.get("dag")
    dag_id = getattr(ti, "dag_id", None) or getattr(dag, "dag_id", "?")
    task_id = getattr(ti, "task_id", "?")
    log_url = getattr(ti, "log_url", "")
    run_id = context.get("run_id", "")
    when = context.get("logical_date") or context.get("execution_date")
    ts = when.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S KST") if when else "?"
    exc_lines = str(context.get("exception") or "").strip().splitlines()
    err = exc_lines[0] if exc_lines else "(예외 메시지 없음)"
    if len(err) > 300:
        err = err[:300] + " …"

    blocks = [
        {"type": "header",
         "text": {"type": "plain_text", "text": "🚨 Airflow 태스크 실패 (CRITICAL)", "emoji": True}},
        {"type": "section", "fields": [
            {"type": "mrkdwn", "text": f"*DAG*\n`{dag_id}`"},
            {"type": "mrkdwn", "text": f"*Task*\n`{task_id}`"},
            {"type": "mrkdwn", "text": f"*환경*\n`{APP_ENV}`"},
            {"type": "mrkdwn", "text": f"*호스트*\n`{socket.gethostname()}`"},
            {"type": "mrkdwn", "text": f"*실행시각*\n{ts}"},
            {"type": "mrkdwn", "text": f"*Run*\n`{run_id}`"},
        ]},
        {"type": "section", "text": {"type": "mrkdwn", "text": f"*마지막 오류*\n```{err}```"}},
    ]
    if log_url:
        blocks.append({"type": "section", "text": {"type": "mrkdwn",
            "text": f"*원인/실패 체크 확인* → <{log_url}|Airflow 로그 열기>"}})
    blocks.append({"type": "context", "elements": [
        {"type": "mrkdwn", "text": "ad-event-lakehouse · 재시도 소진 후 알림 · WARN은 알림 없음(FAIL/크래시만)"}]})

    _post({
        # <!here> = @here 태깅(현재 활성 멤버 호출). 긴급 경보라 사람을 부른다.
        "text": f"<!here> 🚨 [CRITICAL] Airflow 실패 — {dag_id}.{task_id} ({APP_ENV}) {ts}",
        "attachments": [{"color": "#D7263D", "blocks": blocks}],
    })
