"""
beat_mapper.py — Breaks a script into visual beats using the Anthropic API.
Each beat carries emotion, visual direction, keywords, duration, script placement,
excerpt, speaker pace, and hold duration for the editor.
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

# Valid script positions in order
SCRIPT_POSITIONS = [
    "cold_open", "intro",
    "entry_5", "entry_4", "entry_3", "entry_2", "entry_1",
    "outro",
]

# Hold duration by speaker pace (seconds)
_PACE_HOLD = {
    "slow":   7.0,
    "medium": 4.5,
    "fast":   2.5,
}
_SHOCK_HOLD = 1.5
_SHOCK_EMOTIONS = {"shock", "reveal", "twist"}

_PROMPT = """\
You are a video editor.

Break this script into visual beats.

For each beat provide:
1. Beat Name
2. Emotion (tension, awkward, suspense, shock, reveal, relief, curiosity, dread, mystery, etc.)
3. Visual Direction (what should be shown)
4. Keywords for stock footage (3-5 single words, no phrases)
5. Duration (3-5 seconds typical)
6. Script Position — exactly where in the script this beat occurs.
   Valid values: cold_open | intro | entry_5 | entry_4 | entry_3 | entry_2 | entry_1 | outro
7. Script Excerpt — the first 40 words of the script section this beat covers (verbatim from the script).
   This is shown to the human reviewer so they can verify the visual matches what is literally being said.
8. Speaker Pace — based on sentence length in that section:
   slow  → long sentences, dramatic pauses
   medium → normal narration flow
   fast  → punchy, short sentences, rapid delivery

SPECIAL BEAT TYPES — use these names when the script references real evidence:
- "real_photo" — script references a named historical photo, case file, evidence photo,
  or missing person photo. Keywords should describe the photo subject.
- "real_video" — script references real footage: surveillance camera, news archive,
  declassified government footage, documentary clip. Keywords = ["footage", subject].
- "real_audio" — script references an audio recording: 911 call, interview recording,
  intercepted transmission, voicemail. Keywords = ["audio", source_name].

RULES:
- For each beat, identify exactly where in the script this moment occurs
- Reference the actual words being spoken when choosing visuals
- Match the visual to what is LITERALLY being described, not just the general emotion
- Use real_photo / real_video / real_audio beat names ONLY when the script explicitly
  references real evidence — do NOT use them for general descriptions
- Vary shot types (close-up, wide, reaction)
- No repetitive visuals
- Keep it dynamic and engaging

Script: {script}"""


# ─── Helpers ───────────────────────────────────────────────────────────────────

def _hold_duration(beat: dict) -> float:
    """Derives hold_duration from speaker_pace, with shock/reveal override."""
    emotion = beat.get("emotion", "").lower().strip()
    if emotion in _SHOCK_EMOTIONS:
        return _SHOCK_HOLD
    pace = beat.get("speaker_pace", "medium").lower().strip()
    return _PACE_HOLD.get(pace, _PACE_HOLD["medium"])


def _beat_media_defaults(beat: dict) -> dict:
    """
    Set extended media-pipeline fields on every beat.
    Derives visual_source, narration_active, music_active, music_volume
    from the beat name — do not rely on the AI to set these.
    """
    name = beat.get("name", "").lower()

    if name == "real_photo":
        beat.setdefault("narration_active", True)
        beat.setdefault("music_active",     True)
        beat.setdefault("music_volume",     0.10)
        beat.setdefault("visual_source",    "real_media")
        beat.setdefault("caption_text",     "")

    elif name == "real_video":
        beat.setdefault("narration_active", False)
        beat.setdefault("music_active",     True)
        beat.setdefault("music_volume",     0.18)
        beat.setdefault("visual_source",    "real_media")
        beat.setdefault("caption_text",     "")

    elif name == "real_audio":
        beat.setdefault("narration_active", False)
        beat.setdefault("music_active",     False)
        beat.setdefault("music_volume",     0.0)
        beat.setdefault("visual_source",    "real_media")
        # caption_text populated later in main.py when media_item is attached

    else:
        beat.setdefault("narration_active", True)
        beat.setdefault("music_active",     True)
        beat.setdefault("music_volume",     0.10)
        beat.setdefault("visual_source",    "broll")
        beat.setdefault("caption_text",     "")

    beat.setdefault("media_item", None)
    return beat


def _insert_followup_beats(beats: list) -> list:
    """
    After every real_video beat: insert a 'discussing' broll beat.
    After every real_audio beat: insert a 'summary' broll beat.
    Both inherit the parent beat's script_position.
    """
    result = []
    for beat in beats:
        result.append(beat)
        name = beat.get("name", "").lower()

        if name == "real_video":
            followup = {
                "name":             "discussing",
                "emotion":          "curiosity",
                "visual_direction": "atmospheric b-roll while narrator discusses footage",
                "keywords":         beat.get("keywords", ["mystery", "dark"])[:3],
                "duration":         5,
                "script_position":  beat.get("script_position", "intro"),
                "script_excerpt":   "",
                "speaker_pace":     "medium",
                "hold_duration":    _PACE_HOLD["medium"],
                "narration_active": True,
                "music_active":     True,
                "music_volume":     0.10,
                "visual_source":    "broll",
                "caption_text":     "",
                "media_item":       None,
            }
            result.append(followup)

        elif name == "real_audio":
            followup = {
                "name":             "summary",
                "emotion":          "mystery",
                "visual_direction": "atmospheric b-roll while narrator summarises recording",
                "keywords":         beat.get("keywords", ["mystery", "dark"])[:3],
                "duration":         5,
                "script_position":  beat.get("script_position", "intro"),
                "script_excerpt":   "",
                "speaker_pace":     "medium",
                "hold_duration":    _PACE_HOLD["medium"],
                "narration_active": True,
                "music_active":     True,
                "music_volume":     0.10,
                "visual_source":    "broll",
                "caption_text":     "",
                "media_item":       None,
            }
            result.append(followup)

    return result


_POSITION_LABELS = {
    "cold_open": "Cold Open",
    "intro":     "Intro",
    "entry_5":   "Entry #5",
    "entry_4":   "Entry #4",
    "entry_3":   "Entry #3",
    "entry_2":   "Entry #2",
    "entry_1":   "Entry #1 (climax)",
    "outro":     "Outro",
}


def _normalize_position(pos: str) -> str:
    """Returns pos if valid, else 'intro' as default."""
    return pos if pos in SCRIPT_POSITIONS else "intro"


def _wrap_text(text: str, width: int = 60, indent: str = "           ") -> str:
    """Wraps text at word boundaries, indenting continuation lines."""
    words = text.split()
    lines = []
    current = ""
    for word in words:
        if current and len(current) + 1 + len(word) > width:
            lines.append(current)
            current = word
        else:
            current = (current + " " + word).strip()
    if current:
        lines.append(current)
    return ("\n" + indent).join(lines)


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
            "keywords": [str, ...],       # 3-5 items
            "duration": int,              # seconds
            "script_position": str,       # cold_open / intro / entry_N / outro
            "script_excerpt": str,        # first 40 words of that section
            "speaker_pace": str,          # slow / medium / fast
            "hold_duration": float        # derived from speaker_pace + emotion
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
            '      "emotion": "dread",\n'
            '      "visual_direction": "close-up of empty road at night",\n'
            '      "keywords": ["road", "night", "fog", "dark"],\n'
            '      "duration": 4,\n'
            '      "script_position": "cold_open",\n'
            '      "script_excerpt": "It was a Tuesday night when everything changed in the small town. No one expected what would happen next. The streets were empty and the air was cold.",\n'
            '      "speaker_pace": "slow"\n'
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

        for beat in result["beats"]:
            # Clamp duration
            beat["duration"] = max(2, min(8, int(beat.get("duration", 4))))
            # Ensure keywords list
            kws = beat.get("keywords", [])
            beat["keywords"] = [str(k).strip() for k in kws if k][:5] or ["video"]
            # Normalize position / excerpt / pace
            beat["script_position"] = _normalize_position(
                beat.get("script_position", "intro")
            )
            beat["script_excerpt"] = str(beat.get("script_excerpt", "")).strip()
            beat["speaker_pace"]   = beat.get("speaker_pace", "medium").lower().strip()
            if beat["speaker_pace"] not in _PACE_HOLD:
                beat["speaker_pace"] = "medium"
            # Derive hold_duration
            beat["hold_duration"] = _hold_duration(beat)
            # Extended media-pipeline fields
            _beat_media_defaults(beat)

        # Auto-insert discussing/summary follow-up beats
        result["beats"] = _insert_followup_beats(result["beats"])
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
        ("hook",       "shock",     "close-up reaction",      ["close-up", "face", "shock", "night"],   "cold_open", "slow"),
        ("setup",      "curiosity", "establishing wide shot",  ["city", "street", "daylight", "wide"],   "intro",     "medium"),
        ("tension",    "tension",   "slow zoom on subject",    ["indoor", "window", "shadows", "zoom"],  "entry_3",   "slow"),
        ("escalation", "suspense",  "rapid cut montage",       ["hands", "phone", "car", "movement"],    "entry_2",   "fast"),
        ("resolution", "relief",    "calm wide outdoor shot",  ["nature", "sky", "outdoor", "calm"],     "outro",     "medium"),
    ]

    beats = []
    for i, chunk in enumerate(chunks):
        t = _beat_templates[i % len(_beat_templates)]
        excerpt = " ".join(chunk[:40])
        pace = t[5]
        beat = {
            "name":             t[0],
            "emotion":          t[1],
            "visual_direction": t[2],
            "keywords":         t[3],
            "duration":         4,
            "script_position":  t[4],
            "script_excerpt":   excerpt,
            "speaker_pace":     pace,
        }
        beat["hold_duration"] = _hold_duration(beat)
        _beat_media_defaults(beat)
        beats.append(beat)

    return {
        "beats": beats,
        "total_duration": len(beats) * 4,
    }


# ─── Review & Approval ────────────────────────────────────────────────────────

def review_and_approve_beats(post_id: str, beats_result: dict) -> dict:
    """
    Prints a numbered review table to terminal, prompts the user to approve or
    edit individual beats, then saves the approved beat map to
    output/{post_id}_beats_approved.json.

    Returns the (possibly edited) beats_result dict.
    Only call get_clips_for_beats() with the result of this function.
    """
    beats = beats_result["beats"]

    _print_review_table(beats)

    print("\nReview beats above. Enter beat numbers to edit (comma separated) "
          "or press Enter to approve all:")
    raw_input = input("> ").strip()

    if raw_input:
        to_edit = []
        for token in raw_input.split(","):
            token = token.strip()
            if token.isdigit():
                idx = int(token) - 1  # convert to 0-based
                if 0 <= idx < len(beats):
                    to_edit.append(idx)
                else:
                    print(f"  [WARN] Beat {token} out of range — skipped.")

        for idx in to_edit:
            beat = beats[idx]
            beat_num = idx + 1
            total = len(beats)
            kw_str = " ".join(beat.get("keywords", []))
            pos_raw = beat.get("script_position", "intro")
            pos_label = _POSITION_LABELS.get(pos_raw, pos_raw)
            excerpt = beat.get("script_excerpt", "")
            emotion = beat.get("emotion", "")
            hold = beat.get("hold_duration", 0)

            divider = "-" * 60
            wrapped_excerpt = _wrap_text(f'"{excerpt}"', width=60, indent="           ")
            print(f"\n{divider}")
            print(f"Beat {beat_num} of {total}")
            print(f"Position : {pos_label}")
            print(f"Excerpt  : {wrapped_excerpt}")
            print(f"Emotion  : {emotion}")
            print(f"Keywords : {kw_str}")
            print(f"Hold     : {hold}s")
            print(divider)

            print("Edit keywords? (space separated, Enter to keep):")
            new_kw = input("> ").strip()
            if new_kw:
                beat["keywords"] = [k.strip() for k in new_kw.split() if k.strip()][:5]

            print("Edit emotion? (Enter to keep):")
            new_emotion = input("> ").strip()
            if new_emotion:
                beat["emotion"] = new_emotion.lower().strip()
                beat["hold_duration"] = _hold_duration(beat)

            final_kw = " ".join(beat.get("keywords", []))
            final_emotion = beat.get("emotion", "")
            final_hold = beat.get("hold_duration", 0)
            print(f"[OK] Beat {beat_num} updated: keywords={final_kw}  emotion={final_emotion}  hold={final_hold}s")

    # Re-print table after edits so user sees final state
    if raw_input:
        print("\nFinal approved beats:")
        _print_review_table(beats)

    beats_result["beats"] = beats
    _store_approved_beats(post_id, beats_result)
    print(f"\n[BEATS] Approved beat map saved → output/{post_id}_beats_approved.json")
    return beats_result


def _print_review_table(beats: list) -> None:
    """Prints the beat review table to stdout."""
    col_beat  = 5
    col_pos   = 13
    col_emo   = 11
    col_kw    = 20
    col_hold  = 6
    col_narr  = 5   # narration_active (T/F)
    col_src   = 10  # visual_source

    header = (
        f"{'Beat':<{col_beat}} | "
        f"{'Position':<{col_pos}} | "
        f"{'Emotion':<{col_emo}} | "
        f"{'Keywords':<{col_kw}} | "
        f"{'Hold':<{col_hold}} | "
        f"{'Narr':<{col_narr}} | "
        f"{'Source':<{col_src}}"
    )
    sep = "-" * len(header)

    print(f"\n{sep}")
    print(header)
    print(sep)

    for i, beat in enumerate(beats, 1):
        kw_str   = " ".join(beat.get("keywords", []))[:col_kw]
        position = beat.get("script_position", "")[:col_pos]
        emotion  = beat.get("emotion", "")[:col_emo]
        hold     = beat.get("hold_duration", 0)
        hold_str = f"{hold}s"
        narr_str = "Y" if beat.get("narration_active", True) else "N"
        src_str  = beat.get("visual_source", "broll")[:col_src]

        print(
            f"{i:<{col_beat}} | "
            f"{position:<{col_pos}} | "
            f"{emotion:<{col_emo}} | "
            f"{kw_str:<{col_kw}} | "
            f"{hold_str:<{col_hold}} | "
            f"{narr_str:<{col_narr}} | "
            f"{src_str:<{col_src}}"
        )

    print(sep)


# ─── Storage ──────────────────────────────────────────────────────────────────

def store_beats(post_id: str, beats_result: dict) -> None:
    """Saves the full beat manifest to output/{post_id}_beats.json."""
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    path = os.path.join(config.OUTPUT_DIR, f"{post_id}_beats.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(beats_result, f, indent=2, ensure_ascii=False)
    log.info(f"Beats stored: {path}")


def _store_approved_beats(post_id: str, beats_result: dict) -> None:
    """Saves the approved beat map to output/{post_id}_beats_approved.json."""
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    path = os.path.join(config.OUTPUT_DIR, f"{post_id}_beats_approved.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(beats_result, f, indent=2, ensure_ascii=False)
    log.info(f"Approved beats stored: {path}")


# ─── CSV Logging ──────────────────────────────────────────────────────────────

def log_beats(post_id: str, beats: list) -> None:
    """
    Appends one row per beat to beats_log.csv:
      post_id, beat_name, emotion, keywords, duration, script_position,
      speaker_pace, hold_duration, retention (blank)
    retention is populated later via an analytics fetch pass.
    """
    file_exists = os.path.exists(BEATS_LOG_CSV)
    with open(BEATS_LOG_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow([
                "post_id", "beat_name", "emotion", "keywords",
                "duration", "script_position", "speaker_pace",
                "hold_duration", "retention",
            ])
        for beat in beats:
            keywords_str = "|".join(beat.get("keywords", []))
            writer.writerow([
                post_id,
                beat.get("name", ""),
                beat.get("emotion", ""),
                keywords_str,
                beat.get("duration", 4),
                beat.get("script_position", ""),
                beat.get("speaker_pace", ""),
                beat.get("hold_duration", ""),
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
