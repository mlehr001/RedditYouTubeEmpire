"""
angle_selector.py — Commentary angle generator for the Story Engine.

Uses Claude (ANTHROPIC_API_KEY) to generate 3 opinionated commentary angles
for a given topic. Runs after story scoring (PASS only), before script generation.

AI-generated angles are stored separately from story content — CLAUDE.md rule #4.
Angles influence script framing/tone only; story body is never altered.
"""

import json
import logging
import os
import queue
import re
import sys
import threading

log = logging.getLogger(__name__)

_ANGLE_PROMPT = """\
You are a sharp, opinionated YouTube commentary creator.

Given a topic, generate 3 strong commentary angles.

Each angle must:
- feel original and not generic
- have a clear perspective or take
- be engaging or slightly provocative
- be easy to understand quickly

For each angle provide:
1. Angle Title (short)
2. Core Take (1-2 sentences)
3. Why It's Interesting (why people would watch)

Do NOT be neutral. Pick sides or highlight something weird, ironic, or flawed.

Topic: {topic_summary}

Return JSON only:
{{
  "angles": [
    {{
      "title": "...",
      "core_take": "...",
      "why_interesting": "...",
      "style": "outrage/irony/breakdown"
    }}
  ],
  "selected": 0
}}"""

_MODEL = "claude-sonnet-4-6"
_MAX_TOKENS = 1024
_ANGLE_TIMEOUT_SEC = 10


def generate_angles(topic_summary: str) -> dict:
    """
    Calls Claude to generate 3 commentary angles for the given topic.

    Args:
        topic_summary: Short description of the story/topic.

    Returns:
        Dict with shape: {"angles": [...], "selected": 0}

    Raises:
        RuntimeError: If ANTHROPIC_API_KEY is missing or anthropic is not installed.
        ValueError: If Claude returns fewer than 3 angles or malformed JSON.
    """
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY not set — cannot generate angles")

    try:
        import anthropic
    except ImportError:
        raise RuntimeError(
            "anthropic package not installed. Run: pip install anthropic"
        )

    client = anthropic.Anthropic(api_key=api_key)
    prompt = _ANGLE_PROMPT.format(topic_summary=topic_summary)

    log.debug("[angle_selector] Calling Claude for angles (model=%s)", _MODEL)
    response = client.messages.create(
        model=_MODEL,
        max_tokens=_MAX_TOKENS,
        messages=[{"role": "user", "content": prompt}],
    )

    raw = response.content[0].text.strip()
    # Strip markdown fences if the model added them
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw.strip())

    try:
        result = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(f"Claude returned invalid JSON for angles: {e}\nRaw: {raw[:300]}")

    angles = result.get("angles", [])
    if len(angles) < 3:
        raise ValueError(
            f"Expected 3 angles from Claude, got {len(angles)}. Raw: {raw[:300]}"
        )

    # Validate each angle has required keys
    required_keys = {"title", "core_take", "why_interesting", "style"}
    for i, angle in enumerate(angles):
        missing = required_keys - set(angle.keys())
        if missing:
            raise ValueError(f"Angle {i} missing keys: {missing}")

    result.setdefault("selected", 0)
    log.debug("[angle_selector] Generated %d angles successfully", len(angles))
    return result


def print_angles(result: dict) -> None:
    """Print all 3 angles to terminal in operator-readable format."""
    angles = result["angles"]
    selected = result.get("selected", 0)

    print("\n" + "=" * 62)
    print("  COMMENTARY ANGLES — Select a perspective for this video")
    print("=" * 62)
    for i, angle in enumerate(angles):
        marker = "  <-- AUTO-SELECTED" if i == selected else ""
        print(f"\n  [{i + 1}] {angle['title']}{marker}")
        print(f"       Take:  {angle['core_take']}")
        print(f"       Why:   {angle['why_interesting']}")
        print(f"       Style: {angle['style']}")
    print("\n" + "=" * 62)


def _timed_input(prompt_text: str, timeout: int) -> str | None:
    """
    Prompts for input with a live countdown. Returns the typed value or None on timeout.
    Uses threading so it works on Windows (no select/signal tricks).
    """
    result_queue: queue.Queue[str] = queue.Queue()

    def _read():
        try:
            val = input(prompt_text)
            result_queue.put(val)
        except EOFError:
            result_queue.put("")

    thread = threading.Thread(target=_read, daemon=True)
    thread.start()

    for remaining in range(timeout, 0, -1):
        if not result_queue.empty():
            break
        sys.stdout.write(
            f"\r  Auto-selecting angle 1 in {remaining:2d}s... "
            f"(or type 2/3 + Enter to override) "
        )
        sys.stdout.flush()
        thread.join(timeout=1.0)
        if not thread.is_alive():
            break

    # Clear the countdown line
    sys.stdout.write("\r" + " " * 70 + "\r")
    sys.stdout.flush()

    if not result_queue.empty():
        return result_queue.get_nowait()
    return None  # timed out — use default


def prompt_angle_selection(result: dict, timeout: int = _ANGLE_TIMEOUT_SEC) -> int:
    """
    Prints all angles, waits up to `timeout` seconds for an operator override,
    then returns the 0-based index of the chosen angle.

    Updates result["selected"] in-place.
    """
    print_angles(result)

    user_input = _timed_input(
        "  Press Enter to use angle 1, or type 2 / 3 to select: ",
        timeout,
    )

    if user_input is None or user_input.strip() in ("", "1"):
        chosen = 0
    elif user_input.strip() == "2":
        chosen = 1
    elif user_input.strip() == "3":
        chosen = 2
    else:
        log.warning(
            "[angle_selector] Unrecognised input '%s' — defaulting to angle 1",
            user_input.strip(),
        )
        chosen = 0

    result["selected"] = chosen
    angle = result["angles"][chosen]
    print(f"\n[ANGLE] Using angle {chosen + 1}: \"{angle['title']}\" ({angle['style']})")
    return chosen


def build_topic_summary(post: dict) -> str:
    """
    Build a concise topic string from a post dict for the angle prompt.
    Caps body preview at 400 chars to keep the prompt focused.
    """
    title = post.get("title", "").strip()
    body = " ".join(post.get("body", "").split())  # normalise whitespace
    body_preview = body[:400]
    if body_preview and not body_preview.endswith((".", "!", "?")):
        body_preview = body_preview.rsplit(" ", 1)[0] + "..."
    return f"{title}. {body_preview}" if body_preview else title
