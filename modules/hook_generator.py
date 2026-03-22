"""
hook_generator.py — Generates 5 typed hooks for a script using the Anthropic API.
Hooks are AI-generated assist content (CLAUDE.md rule #4 — stored separately).
"""

import csv
import json
import logging
import os
import re
import select
import sys
import threading
from datetime import date

import anthropic

import config

log = logging.getLogger(__name__)

HOOKS_LOG_CSV = "hooks_log.csv"
HOOKS_LOG_MIN_VIDEOS = 20  # query performance only after this many entries

_PROMPT = """\
You are a viral content expert.

Generate 5 hooks for this script.

RULES:
- strong first 5-second impact
- curiosity-driven or opinion-driven
- emotional or intriguing
- max 10-12 words
- natural sounding (not spammy clickbait)

Script: {script}"""

_EXPECTED_TYPES = ["curiosity", "opinion", "emotional", "shock", "confessional"]


def generate_hooks(script: str) -> dict:
    """
    Calls the Anthropic API and returns:
      {
        "hooks": [{"type": str, "text": str}, ...],  # 5 items
        "selected": 0
      }
    Falls back to placeholder hooks if the API call fails.
    """
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        log.warning("ANTHROPIC_API_KEY not set — using placeholder hooks.")
        return _fallback_hooks(script)

    try:
        client = anthropic.Anthropic(api_key=api_key)
        user_content = (
            _PROMPT.format(script=script[:2000])
            + '\n\nReturn JSON only:\n'
            '{\n'
            '  "hooks": [\n'
            '    {"type": "curiosity", "text": "..."},\n'
            '    {"type": "opinion",   "text": "..."},\n'
            '    {"type": "emotional", "text": "..."},\n'
            '    {"type": "shock",     "text": "..."},\n'
            '    {"type": "confessional", "text": "..."}\n'
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

        if "hooks" not in result or len(result["hooks"]) < 5:
            raise ValueError("Expected 5 hooks in response")
        result.setdefault("selected", 0)
        return result

    except Exception as e:
        log.warning(f"Hook generation failed: {e} — using fallback hooks.")
        return _fallback_hooks(script)


def _fallback_hooks(script: str) -> dict:
    """Non-AI fallback: one generic hook per type."""
    first_words = " ".join(script.split()[:8]).rstrip(".,!?")
    return {
        "hooks": [
            {"type": "curiosity",    "text": f"Wait... {first_words}?"},
            {"type": "opinion",      "text": "This is genuinely one of the wildest things I've seen."},
            {"type": "emotional",    "text": "I wasn't ready for how this one ends."},
            {"type": "shock",        "text": "Nobody in the comments saw this coming."},
            {"type": "confessional", "text": "Okay I have to talk about this."},
        ],
        "selected": 0,
    }


def prompt_hook_selection(hooks_result: dict) -> int:
    """
    Prints all 5 hooks and runs a 10-second countdown.
    Returns the 0-based index of the selected hook.
    User presses Enter for hook 1, or types 2-5 to pick another.
    """
    hooks = hooks_result["hooks"]

    print("\n[HOOKS] Generated hooks:")
    for i, h in enumerate(hooks, 1):
        print(f"  {i}. [{h['type'].upper():12s}] {h['text']}")

    print("\nPress Enter to use hook 1 or type 2-5 to select different (10s)...")

    selected_idx = [0]  # default
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
        print(f"\r  Auto-selecting hook 1 in {remaining - 1}s...  ", end="", flush=True)

    print()  # newline after countdown

    chosen = selected_idx[0]
    print(f"[HOOKS] Selected hook {chosen + 1}: [{hooks[chosen]['type']}] {hooks[chosen]['text']}")
    return chosen


def prepend_hook(script: str, hook_text: str) -> str:
    """Prepends the hook as the first line of the script."""
    return f"{hook_text}\n\n{script}"


def store_hooks(post_id: str, hooks_result: dict) -> None:
    """Saves all 5 hooks to output/{post_id}_hooks.json."""
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    path = os.path.join(config.OUTPUT_DIR, f"{post_id}_hooks.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(hooks_result, f, indent=2, ensure_ascii=False)
    log.info(f"Hooks stored: {path}")


def log_hook(post_id: str, hook: dict) -> None:
    """
    Appends one row to hooks_log.csv:
      post_id, hook_type, hook_text, date
    Creates the file with a header row if it doesn't exist.
    """
    file_exists = os.path.exists(HOOKS_LOG_CSV)
    with open(HOOKS_LOG_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["post_id", "hook_type", "hook_text", "date"])
        writer.writerow([post_id, hook["type"], hook["text"], date.today().isoformat()])


def query_hook_performance() -> None:
    """
    Reads hooks_log.csv and prints a summary of which hook type
    appears most frequently. Runs only after HOOKS_LOG_MIN_VIDEOS entries.
    """
    if not os.path.exists(HOOKS_LOG_CSV):
        return

    counts: dict[str, int] = {}
    total = 0
    with open(HOOKS_LOG_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            hook_type = row.get("hook_type", "unknown")
            counts[hook_type] = counts.get(hook_type, 0) + 1
            total += 1

    if total < HOOKS_LOG_MIN_VIDEOS:
        return

    print(f"\n{'=' * 60}")
    print(f"HOOK PERFORMANCE REPORT ({total} videos logged)")
    print(f"{'=' * 60}")
    for hook_type, count in sorted(counts.items(), key=lambda x: -x[1]):
        bar = "#" * count
        pct = count / total * 100
        print(f"  {hook_type:14s} {bar:20s} {count:3d} used ({pct:.0f}%)")
    best = max(counts, key=counts.__getitem__)
    print(f"\n  Best performing hook type: {best.upper()}")
    print(f"{'=' * 60}\n")
