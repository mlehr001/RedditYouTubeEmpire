"""
beat_mapper.py — Breaks a script into visual beats using the Anthropic API.
Each beat carries emotion, visual direction, keywords, and duration for the editor.
Beat data is AI-generated assist content (CLAUDE.md rule #4 — stored separately).

RETENTION NOTE: Emotion-to-retention correlation requires YouTube Analytics data.
beats_log.csv tracks emotion distribution now; correlation analysis will fire
automatically once retention figures are back-filled via an analytics fetch pass.
"""

import csv
import json
import logging
import os
import re
from datetime import date

import anthropic

import config

log = logging.getLogger(__name__)

BEATS_LOG_CSV = "beats_log.csv"
BEATS_LOG_MIN_VIDEOS = 20

_PROMPT = """\
You are a video editor.

Break this script into visual beats.

For each beat provide:
1. Beat Name
2. Emotion (tension, awkward, suspense, shock, relief, curiosity, etc.)
3. Visual Direction (what should be shown)
4. Keywords for stock footage (3-5 single words, no phrases)
5. Duration (3-5 seconds typical)

RULES:
- visuals must match emotion, not just words
- vary shot types (close-up, wide, reaction)
- no repetitive visuals
- keep it dynamic and engaging

Script: {script}"""


# ─── Generation ───────────────────────────────────────────────────────────────

def generate_beats(script: str) -> dict:
    """
    Calls the Anthropic API and returns:
      {
        "beats": [
          {
            "name": str,
            "emotion": str,
            "visual_direction": str,
            "keywords": [str, ...],   # 3-5 items
            "duration": int           # seconds
          },
          ...
        ],
        "total_duration": int
      }
    Falls back to keyword-derived beats if the API call fails.
    """
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        log.warning("ANTHROPIC_API_KEY not set — using fallback beats.")
        return _fallback_beats(script)

    try:
        client = anthropic.Anthropic(api_key=api_key)
        user_content = (
            _PROMPT.format(script=script[:3000])
            + '\n\nReturn JSON only:\n'
            '{\n'
            '  "beats": [\n'
            '    {\n'
            '      "name": "hook",\n'
            '      "emotion": "shock",\n'
            '      "visual_direction": "close-up of phone screen at night",\n'
            '      "keywords": ["phone", "night", "dark", "close-up"],\n'
            '      "duration": 4\n'
            '    }\n'
            '  ],\n'
            '  "total_duration": 0\n'
            '}'
        )

        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=2000,
            messages=[{"role": "user", "content": user_content}],
        )

        raw = response.content[0].text.strip()
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
        result = json.loads(raw)

        if "beats" not in result or not result["beats"]:
            raise ValueError("No beats in response")

        # Clamp durations to sane range and recompute total
        for beat in result["beats"]:
            beat["duration"] = max(2, min(8, int(beat.get("duration", 4))))
            # Ensure keywords is a list of strings
            kws = beat.get("keywords", [])
            beat["keywords"] = [str(k).strip() for k in kws if k][:5] or ["video"]

        result["total_duration"] = sum(b["duration"] for b in result["beats"])
        return result

    except Exception as e:
        log.warning(f"Beat generation failed: {e} — using fallback beats.")
        return _fallback_beats(script)


def _fallback_beats(script: str) -> dict:
    """
    Derives basic beats from word count, using generic visual keywords.
    Produces one beat per ~40 words so the video has reasonable coverage.
    """
    words = script.split()
    chunk_size = 40
    chunks = [words[i:i + chunk_size] for i in range(0, len(words), chunk_size)]

    _beat_templates = [
        ("hook",       "shock",     "close-up reaction",      ["close-up", "face", "shock", "night"]),
        ("setup",      "curiosity", "establishing wide shot",  ["city", "street", "daylight", "wide"]),
        ("tension",    "tension",   "slow zoom on subject",    ["indoor", "window", "shadows", "zoom"]),
        ("escalation", "suspense",  "rapid cut montage",       ["hands", "phone", "car", "movement"]),
        ("resolution", "relief",    "calm wide outdoor shot",  ["nature", "sky", "outdoor", "calm"]),
    ]

    beats = []
    for i, chunk in enumerate(chunks):
        template = _beat_templates[i % len(_beat_templates)]
        beats.append({
            "name":             template[0],
            "emotion":          template[1],
            "visual_direction": template[2],
            "keywords":         template[3],
            "duration":         4,
        })

    return {
        "beats": beats,
        "total_duration": len(beats) * 4,
    }


# ─── Storage ──────────────────────────────────────────────────────────────────

def store_beats(post_id: str, beats_result: dict) -> None:
    """Saves the full beat manifest to output/{post_id}_beats.json."""
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    path = os.path.join(config.OUTPUT_DIR, f"{post_id}_beats.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(beats_result, f, indent=2, ensure_ascii=False)
    log.info(f"Beats stored: {path}")


# ─── CSV Logging ──────────────────────────────────────────────────────────────

def log_beats(post_id: str, beats: list) -> None:
    """
    Appends one row per beat to beats_log.csv:
      post_id, beat_name, emotion, keywords, duration, retention (blank)
    retention is populated later via an analytics fetch pass.
    """
    file_exists = os.path.exists(BEATS_LOG_CSV)
    with open(BEATS_LOG_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["post_id", "beat_name", "emotion", "keywords", "duration", "retention"])
        today = date.today().isoformat()
        for beat in beats:
            keywords_str = "|".join(beat.get("keywords", []))
            writer.writerow([
                post_id,
                beat.get("name", ""),
                beat.get("emotion", ""),
                keywords_str,
                beat.get("duration", 4),
                "",  # retention blank until Analytics data arrives
            ])


# ─── Performance Analysis ──────────────────────────────────────────────────────

def query_beat_performance() -> None:
    """
    Reads beats_log.csv and prints:
    - Emotion frequency distribution (always, after first run)
    - Emotion-to-retention correlation (after BEATS_LOG_MIN_VIDEOS unique videos
      have retention data back-filled via update_beat_retention())

    Retention correlation requires YouTube Analytics (24-48h delay after upload).
    """
    if not os.path.exists(BEATS_LOG_CSV):
        return

    rows = []
    with open(BEATS_LOG_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)

    if not rows:
        return

    # Count unique videos logged
    unique_posts = len({r["post_id"] for r in rows})

    # ── Emotion frequency ─────────────────────────────────────────────────────
    emotion_counts: dict[str, int] = {}
    for row in rows:
        e = row.get("emotion", "unknown")
        emotion_counts[e] = emotion_counts.get(e, 0) + 1

    total_beats = len(rows)
    print(f"\n{'=' * 60}")
    print(f"BEAT EMOTION DISTRIBUTION ({total_beats} beats, {unique_posts} videos)")
    print(f"{'=' * 60}")
    for emotion, count in sorted(emotion_counts.items(), key=lambda x: -x[1]):
        bar = "#" * min(count, 30)
        pct = count / total_beats * 100
        print(f"  {emotion:14s} {bar:30s} {count:3d} ({pct:.0f}%)")
    print(f"{'=' * 60}\n")

    # ── Retention correlation (requires Analytics data) ───────────────────────
    if unique_posts < BEATS_LOG_MIN_VIDEOS:
        remaining = BEATS_LOG_MIN_VIDEOS - unique_posts
        print(f"  [BEATS] Retention analysis unlocks after {remaining} more video(s).")
        return

    retention_by_emotion: dict[str, list[float]] = {}
    for row in rows:
        ret_raw = row.get("retention", "").strip()
        if not ret_raw:
            continue
        try:
            ret_val = float(ret_raw)
        except ValueError:
            continue
        emotion = row.get("emotion", "unknown")
        retention_by_emotion.setdefault(emotion, []).append(ret_val)

    if not retention_by_emotion:
        print(f"  [BEATS] {unique_posts} videos logged — retention data not yet available.")
        print(f"          Back-fill via update_beat_retention() after Analytics data arrives.")
        return

    print(f"\n{'=' * 60}")
    print(f"BEAT RETENTION CORRELATION ({unique_posts} videos with data)")
    print(f"{'=' * 60}")
    avg_by_emotion = {e: sum(v) / len(v) for e, v in retention_by_emotion.items()}
    for emotion, avg in sorted(avg_by_emotion.items(), key=lambda x: -x[1]):
        n = len(retention_by_emotion[emotion])
        print(f"  {emotion:14s} avg retention: {avg:.1f}%  (n={n})")
    best = max(avg_by_emotion, key=avg_by_emotion.__getitem__)
    print(f"\n  Best retention emotion: {best.upper()} ({avg_by_emotion[best]:.1f}% avg)")
    print(f"{'=' * 60}\n")


def update_beat_retention(post_id: str, retention: float) -> bool:
    """
    Fills in the retention column for all beats belonging to post_id.
    Call this from an analytics fetch pass once YouTube data is available.
    Returns True if any rows were updated.
    """
    if not os.path.exists(BEATS_LOG_CSV):
        return False

    rows = []
    updated = False
    with open(BEATS_LOG_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for row in reader:
            if row["post_id"] == post_id and row.get("retention", "") == "":
                row["retention"] = str(round(retention, 2))
                updated = True
            rows.append(row)

    if updated:
        with open(BEATS_LOG_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        log.info(f"Retention updated for {post_id}: {retention}%")

    return updated
