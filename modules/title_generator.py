"""
title_generator.py — Generates 5 style-typed YouTube titles using the Anthropic API.
Title selection is logged to titles_log.csv for ongoing CTR performance tracking.
Titles are AI-generated assist content (CLAUDE.md rule #4 — stored separately from story).

CTR NOTE: YouTube Analytics reports CTR with a 24-48h delay. Titles are logged at
upload time with a blank CTR column. Call update_title_ctr(post_id, ctr) from a
separate analytics fetch pass once data is available.
"""

import csv
import json
import logging
import os
import re
import threading
from datetime import date

import anthropic

import config

log = logging.getLogger(__name__)

TITLES_LOG_CSV = "titles_log.csv"
TITLES_LOG_MIN_VIDEOS = 20   # minimum entries before CTR-based analysis fires
TITLES_SUMMARY_EVERY = 5     # print style frequency summary every N videos

_PROMPT = """\
You are a YouTube growth expert.

Generate 5 titles for this video.

RULES:
- curiosity-driven
- slightly provocative or emotional
- clear but not revealing everything
- max 60 characters
- avoid generic phrasing

Script: {script}"""

_EXPECTED_STYLES = ["curiosity", "emotional", "provocative", "dramatic", "confessional"]


# ─── Generation ───────────────────────────────────────────────────────────────

def generate_titles(script: str) -> dict:
    """
    Calls the Anthropic API and returns:
      {
        "titles": [{"text": str, "style": str}, ...],  # 5 items
        "selected": 0
      }
    Falls back to placeholder titles if the API call fails.
    """
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        log.warning("ANTHROPIC_API_KEY not set — using placeholder titles.")
        return _fallback_titles(script)

    try:
        client = anthropic.Anthropic(api_key=api_key)
        user_content = (
            _PROMPT.format(script=script[:2000])
            + '\n\nReturn JSON only:\n'
            '{\n'
            '  "titles": [\n'
            '    {"text": "...", "style": "curiosity"},\n'
            '    {"text": "...", "style": "emotional"},\n'
            '    {"text": "...", "style": "provocative"},\n'
            '    {"text": "...", "style": "dramatic"},\n'
            '    {"text": "...", "style": "confessional"}\n'
            '  ],\n'
            '  "selected": 0\n'
            '}'
        )

        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=600,
            messages=[{"role": "user", "content": user_content}],
        )

        raw = response.content[0].text.strip()
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
        result = json.loads(raw)

        if "titles" not in result or len(result["titles"]) < 5:
            raise ValueError("Expected 5 titles in response")
        # Enforce 60-char hard cap (model may slip)
        for t in result["titles"]:
            t["text"] = t["text"][:60].strip()
        result.setdefault("selected", 0)
        return result

    except Exception as e:
        log.warning(f"Title generation failed: {e} — using fallback titles.")
        return _fallback_titles(script)


def _fallback_titles(script: str) -> dict:
    first = " ".join(script.split()[:6]).rstrip(".,!?")
    return {
        "titles": [
            {"text": f"Nobody Expected This To Happen...",          "style": "curiosity"},
            {"text": f"This Broke Me. I Had To Share It.",          "style": "emotional"},
            {"text": f"The Part They Don't Want You To Know",       "style": "provocative"},
            {"text": f"Everything Changed After This One Moment",   "style": "dramatic"},
            {"text": f"I Can't Believe I'm Saying This Out Loud",   "style": "confessional"},
        ],
        "selected": 0,
    }


# ─── Interactive Selection ─────────────────────────────────────────────────────

def prompt_title_selection(titles_result: dict) -> int:
    """
    Prints all 5 titles and runs a 10-second countdown.
    Returns the 0-based index of the selected title.
    User presses Enter for title 1, or types 2-5 to pick another.
    """
    titles = titles_result["titles"]

    print("\n[TITLES] Generated titles:")
    for i, t in enumerate(titles, 1):
        char_count = len(t["text"])
        print(f"  {i}. [{t['style']:12s}] {t['text']}  ({char_count}ch)")

    print("\nPress Enter to use title 1 or type 2-5 to select different (10s)...")

    selected_idx = [0]
    input_received = threading.Event()

    def _read_input():
        try:
            val = input().strip()
            if val in ("2", "3", "4", "5"):
                selected_idx[0] = int(val) - 1
        except Exception:
            pass
        input_received.set()

    t = threading.Thread(target=_read_input, daemon=True)
    t.start()

    for remaining in range(10, 0, -1):
        if input_received.wait(timeout=1):
            break
        print(f"\r  Auto-selecting title 1 in {remaining - 1}s...  ", end="", flush=True)

    print()

    chosen = selected_idx[0]
    chosen_title = titles[chosen]
    print(f"[TITLES] Selected title {chosen + 1}: [{chosen_title['style']}] {chosen_title['text']}")
    return chosen


# ─── Storage ──────────────────────────────────────────────────────────────────

def store_titles(post_id: str, titles_result: dict) -> None:
    """
    Saves all 5 titles + selected index to output/{post_id}_titles.json.
    This also serves as the 'videos table' final_title record until a DB is active.
    """
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    path = os.path.join(config.OUTPUT_DIR, f"{post_id}_titles.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(titles_result, f, indent=2, ensure_ascii=False)
    log.info(f"Titles stored: {path}")


# ─── CSV Logging ──────────────────────────────────────────────────────────────

def log_title(post_id: str, title: dict) -> None:
    """
    Appends one row to titles_log.csv with a blank CTR column:
      post_id, title_text, style, date, ctr
    CTR is populated later via update_title_ctr() once YouTube Analytics data arrives.
    """
    file_exists = os.path.exists(TITLES_LOG_CSV)
    with open(TITLES_LOG_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["post_id", "title_text", "style", "date", "ctr"])
        writer.writerow([post_id, title["text"], title["style"], date.today().isoformat(), ""])


def update_title_ctr(post_id: str, ctr: float) -> bool:
    """
    Finds the row for post_id in titles_log.csv and writes the CTR value.
    Call this from an analytics fetch pass (YouTube Analytics has a 24-48h delay).
    Returns True if the row was found and updated, False otherwise.
    """
    if not os.path.exists(TITLES_LOG_CSV):
        log.warning(f"update_title_ctr: {TITLES_LOG_CSV} not found.")
        return False

    rows = []
    updated = False
    with open(TITLES_LOG_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for row in reader:
            if row["post_id"] == post_id and row["ctr"] == "":
                row["ctr"] = str(round(ctr, 4))
                updated = True
            rows.append(row)

    if updated:
        with open(TITLES_LOG_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        log.info(f"CTR updated for {post_id}: {ctr}")

    return updated


# ─── Performance Analysis ──────────────────────────────────────────────────────

def query_title_performance() -> None:
    """
    Reads titles_log.csv and prints performance summaries:
    - Style frequency tally every TITLES_SUMMARY_EVERY videos (always)
    - CTR-by-style leaderboard once TITLES_LOG_MIN_VIDEOS entries exist with CTR data
    """
    if not os.path.exists(TITLES_LOG_CSV):
        return

    rows = []
    with open(TITLES_LOG_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)

    total = len(rows)
    if total == 0:
        return

    # ── Every N videos: style frequency summary ───────────────────────────────
    if total % TITLES_SUMMARY_EVERY == 0:
        style_counts: dict[str, int] = {}
        for row in rows:
            s = row.get("style", "unknown")
            style_counts[s] = style_counts.get(s, 0) + 1

        print(f"\n{'=' * 60}")
        print(f"TITLE STYLE FREQUENCY ({total} videos)")
        print(f"{'=' * 60}")
        for style, count in sorted(style_counts.items(), key=lambda x: -x[1]):
            bar = "#" * count
            pct = count / total * 100
            print(f"  {style:14s} {bar:20s} {count:3d} ({pct:.0f}%)")
        most_used = max(style_counts, key=style_counts.__getitem__)
        print(f"  Most used style: {most_used.upper()}")
        print(f"{'=' * 60}\n")

    # ── After MIN_VIDEOS: CTR-by-style leaderboard ────────────────────────────
    if total < TITLES_LOG_MIN_VIDEOS:
        return

    ctr_by_style: dict[str, list[float]] = {}
    for row in rows:
        ctr_raw = row.get("ctr", "").strip()
        if not ctr_raw:
            continue
        try:
            ctr_val = float(ctr_raw)
        except ValueError:
            continue
        style = row.get("style", "unknown")
        ctr_by_style.setdefault(style, []).append(ctr_val)

    if not ctr_by_style:
        print(f"  [TITLES] {total} videos logged — CTR data not yet available (check back in 24-48h).")
        return

    print(f"\n{'=' * 60}")
    print(f"TITLE CTR PERFORMANCE ({total} videos, {sum(len(v) for v in ctr_by_style.values())} with CTR data)")
    print(f"{'=' * 60}")
    avg_by_style = {s: sum(v) / len(v) for s, v in ctr_by_style.items()}
    for style, avg in sorted(avg_by_style.items(), key=lambda x: -x[1]):
        sample = len(ctr_by_style[style])
        print(f"  {style:14s} avg CTR: {avg:.2%}  (n={sample})")
    best = max(avg_by_style, key=avg_by_style.__getitem__)
    print(f"\n  Best performing title style: {best.upper()} ({avg_by_style[best]:.2%} avg CTR)")
    print(f"{'=' * 60}\n")
