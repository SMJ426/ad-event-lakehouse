"""
daily_report.py — 데일리 KPI 리포트 (Gold 집계 → WoW/MoM 동요일 비교 + 추세 차트 → Slack 봇)

매일 정상 운영 KPI 요약을 Slack 채널에 능동 푸시한다(실패 경보와 별개의 '정상 리포트').
광고 도메인 특성상 어제 대비보다 전주/전월 동요일 대비가 의미있어 그 비교를 중심에 둔다.

  - 기준일 D = gold.campaign_daily_stats의 최신 event_date(또는 --date).
  - WoW = D vs D-7(전주 동요일), MoM = D vs D-28(전월 동요일, 4주=같은 요일).
  - 최근 30일 추세 PNG(matplotlib) 첨부.
  - 전송: Slack 봇 토큰(chat_postMessage + files_upload_v2). 토큰 없으면 콘솔 출력만.

실행:
  spark-submit daily_report.py
  spark-submit daily_report.py --date 2026-06-24 --lookback-days 35
  (env: S3_BUCKET, AWS_REGION, SLACK_BOT_TOKEN, SLACK_REPORT_CHANNEL)
"""

import argparse
import os
from datetime import date, datetime, timedelta

import matplotlib
matplotlib.use("Agg")                      # 헤드리스(컨테이너)에서 PNG 렌더
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm

from spark_common import CATALOG, build_spark

# 한글 폰트 등록 (없으면 차트 글자가 □□로 깨짐). airflow 이미지에 fonts-nanum 설치.
_KFONT = "/usr/share/fonts/truetype/nanum/NanumGothic.ttf"
if os.path.exists(_KFONT):
    fm.fontManager.addfont(_KFONT)
    plt.rcParams["font.family"] = "NanumGothic"
plt.rcParams["axes.unicode_minus"] = False

SOURCE = f"{CATALOG}.gold.campaign_daily_stats"
SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN", "")
SLACK_REPORT_CHANNEL = os.environ.get("SLACK_REPORT_CHANNEL", "")
REVENUE_PER_CONVERSION = 10.0              # ROAS 가정 단가 (gold_aggregations와 동일)
CHART_PATH = "/tmp/daily_report_trend.png"


# ── 집계 조회 ────────────────────────────────────────────────────────────────

def load_daily(spark, lookback_days: int) -> dict:
    """campaign_daily_stats를 event_date별로 SUM 집계해 {date: {지표}} 반환."""
    rows = spark.sql(
        f"SELECT event_date, sum(requests) requests, sum(impressions) impressions, "
        f"sum(clicks) clicks, sum(conversions) conversions, sum(cost) cost "
        f"FROM {SOURCE} WHERE event_date >= current_date() - INTERVAL {int(lookback_days)} DAYS "
        f"GROUP BY event_date"
    ).collect()
    out = {}
    for r in rows:
        out[r["event_date"]] = {
            "requests": r["requests"] or 0, "impressions": r["impressions"] or 0,
            "clicks": r["clicks"] or 0, "conversions": r["conversions"] or 0,
            "cost": float(r["cost"] or 0.0),
        }
    return out


def kpis(rec: dict) -> dict:
    """원시 합계에서 파생 KPI(CTR/CVR/ROAS) 계산."""
    impr, clk, conv, cost = rec["impressions"], rec["clicks"], rec["conversions"], rec["cost"]
    return {
        "impressions": impr, "clicks": clk, "conversions": conv, "cost": cost,
        "ctr": clk / impr if impr else 0.0,
        "cvr": conv / clk if clk else 0.0,
        "roas": conv * REVENUE_PER_CONVERSION / cost if cost else 0.0,
    }


# ── 포맷 헬퍼 ────────────────────────────────────────────────────────────────

def _h(n: float) -> str:
    n = float(n)
    if abs(n) >= 1e6:
        return f"{n/1e6:.2f}M"
    if abs(n) >= 1e3:
        return f"{n/1e3:.1f}k"
    return f"{n:.0f}"


def _money(n: float) -> str:
    return f"${n:,.0f}"


def _pct(cur, prev):
    if not prev:
        return None
    return (cur - prev) / prev * 100


def _delta(p) -> str:
    if p is None:
        return "비교없음"
    return f"{'▲' if p >= 0 else '▼'}{p:+.1f}%"


def _dw(s: str) -> int:
    """표시 폭(한글 음절은 2칸). monospace 표 정렬용."""
    return sum(2 if 0xAC00 <= ord(c) <= 0xD7A3 else 1 for c in s)


def _padr(s: str, width: int) -> str:
    return s + " " * max(0, width - _dw(s))


def _padl(s: str, width: int) -> str:
    return " " * max(0, width - _dw(s)) + s


# ── 메시지/차트 ──────────────────────────────────────────────────────────────

def build_blocks(daily: dict, d: date) -> tuple:
    """Block Kit 메시지(blocks) + fallback 텍스트 생성."""
    cur = kpis(daily[d])
    wow = kpis(daily[d - timedelta(days=7)]) if (d - timedelta(days=7)) in daily else None
    mom = kpis(daily[d - timedelta(days=28)]) if (d - timedelta(days=28)) in daily else None

    # ── 핵심 KPI 표 (오늘 값 + 전주/전월 동요일 대비) — monospace 정렬 ──
    metrics = [
        ("노출", "impressions", _h(cur["impressions"])),
        ("클릭", "clicks", _h(cur["clicks"])),
        ("전환", "conversions", _h(cur["conversions"])),
        ("광고비", "cost", _money(cur["cost"])),
        ("CTR", "ctr", f"{cur['ctr']*100:.2f}%"),
        ("CVR", "cvr", f"{cur['cvr']*100:.2f}%"),
        ("ROAS", "roas", f"{cur['roas']:.2f}x"),
    ]
    kpi = [_padr("지표", 8) + _padl("오늘", 10) + _padl("전주대비", 12) + _padl("전월대비", 12),
           "─" * 42]
    for label, key, valstr in metrics:
        w = _delta(_pct(cur[key], wow[key]) if wow else None)
        m = _delta(_pct(cur[key], mom[key]) if mom else None)
        kpi.append(_padr(label, 8) + _padl(valstr, 10) + _padl(w, 12) + _padl(m, 12))
    kpi_md = "```\n" + "\n".join(kpi) + "\n```"

    # 최근 7일 표 (monospace)
    recent = sorted([x for x in daily if x <= d], reverse=True)[:7]
    tbl = ["날짜        impr     clk    conv      cost",
           "─" * 44]
    for dd in recent:
        k = kpis(daily[dd])
        tbl.append(f"{dd.strftime('%m-%d')}   {_h(k['impressions']):>7} {_h(k['clicks']):>7} "
                   f"{_h(k['conversions']):>7}  {_money(k['cost']):>10}")
    table_md = "```\n" + "\n".join(tbl) + "\n```"

    blocks = [
        {"type": "header", "text": {"type": "plain_text", "text": f"📊 데일리 KPI 리포트 — {d}", "emoji": True}},
        {"type": "section", "text": {"type": "mrkdwn", "text": f"*핵심 지표 (전주·전월 동요일 대비)*\n{kpi_md}"}},
        {"type": "section", "text": {"type": "mrkdwn", "text": f"*최근 7일*\n{table_md}"}},
        {"type": "context", "elements": [{"type": "mrkdwn",
            "text": "전주대비 = 지난주 같은 요일 대비 · 전월대비 = 4주 전 같은 요일 대비 · 첨부 = 최근 30일 추세"}]},
    ]
    fallback = (f"📊 데일리 KPI 리포트 {d} | 노출 {_h(cur['impressions'])} · 클릭 {_h(cur['clicks'])} · "
                f"광고비 {_money(cur['cost'])} · ROAS {cur['roas']:.2f}x")
    return blocks, fallback


def make_chart(daily: dict, d: date, days: int = 30) -> str:
    """최근 days일 핵심 지표(광고비·노출·클릭·전환) 추세를 2×2 대시보드 PNG로 생성."""
    xs = sorted([x for x in daily if x <= d])[-days:]
    panels = [
        ("광고비 (cost, $)", "cost", "#D7263D"),
        ("노출 (impressions)", "impressions", "#1f77b4"),
        ("클릭 (clicks)", "clicks", "#2ca02c"),
        ("전환 (conversions)", "conversions", "#ff7f0e"),
    ]
    fig, axes = plt.subplots(2, 2, figsize=(11, 6))
    for ax, (title, key, color) in zip(axes.flat, panels):
        ax.plot(xs, [daily[x][key] for x in xs], marker="o", markersize=2.5, linewidth=1.4, color=color)
        ax.set_title(title, fontsize=10)
        ax.grid(True, alpha=0.3)
        ax.tick_params(axis="x", labelsize=7, rotation=30)
        ax.tick_params(axis="y", labelsize=7)
    fig.suptitle(f"최근 {len(xs)}일 핵심 지표 추세 (기준일 {d})", fontsize=13)
    fig.tight_layout(rect=(0, 0, 1, 0.96))
    fig.savefig(CHART_PATH, dpi=120)
    plt.close(fig)
    return CHART_PATH


def send_slack(blocks, fallback, chart_path) -> None:
    """봇 토큰으로 텍스트 + 차트 전송. 토큰 미설정이면 콘솔 출력만."""
    if not (SLACK_BOT_TOKEN and SLACK_REPORT_CHANNEL):
        print("[INFO] SLACK_BOT_TOKEN/CHANNEL 미설정 — 전송 skip. 미리보기:")
        print("  " + fallback)
        print(f"  (차트 저장됨: {chart_path})")
        return
    from slack_sdk import WebClient
    client = WebClient(token=SLACK_BOT_TOKEN)
    client.chat_postMessage(channel=SLACK_REPORT_CHANNEL, text=fallback, blocks=blocks)
    client.files_upload_v2(channel=SLACK_REPORT_CHANNEL, file=chart_path,
                           title="최근 30일 핵심 지표 추세", initial_comment="📈 핵심 지표 추세 (광고비·노출·클릭·전환)")
    print("[INFO] Slack 데일리 리포트 전송 완료")


def run(args) -> None:
    spark = build_spark("daily-report")
    spark.sparkContext.setLogLevel("WARN")

    daily = load_daily(spark, args.lookback_days)
    if not daily:
        print("[WARN] 집계 데이터 없음 — 리포트 생략.")
        return
    # 데일리 배치 정석: 어제(완료된 날) 집계. 오늘(진행 중)은 불완전하므로 기본 D = 어제(D-1).
    d = datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else (date.today() - timedelta(days=1))
    if d not in daily:
        print(f"[WARN] 기준일 {d}에 데이터 없음 — 가용한 최신일로 대체.")
        d = max(daily)

    blocks, fallback = build_blocks(daily, d)
    chart = make_chart(daily, d)
    print(f"[INFO] 리포트 기준일={d} | 집계 {len(daily)}일")
    send_slack(blocks, fallback, chart)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--date", default=None, help="리포트 기준일(YYYY-MM-DD). 기본=최신 데이터일.")
    p.add_argument("--lookback-days", type=int, default=35, help="집계/추세 조회 일수. 기본 35.")
    run(p.parse_args())
