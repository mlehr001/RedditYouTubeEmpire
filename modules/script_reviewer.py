"""
Script Reviewer — Human-in-the-loop review step.

Runs AFTER script generation, BEFORE TTS.

Flow:
  1. Save script to output/{id}_review.txt
  2. Open in default OS text editor
  3. Prompt user: Enter to accept, 'reject' to pull a new story
  4. On Enter: read back the (possibly edited) file, diff it, log edits
  5. On 'reject': return ("rejected",) so caller can restart
  6. Track edit categories; warn at 10 edits in same category
"""

import os
import re
import json
import difflib
import subprocess
from datetime import datetime, timezone

import config

# ─── Public API ───────────────────────────────────────────────────────────────

EDIT_STATS_PATH = os.path.join(config.OUTPUT_DIR, "_edit_stats.json")
EDIT_CATEGORIES = ("fact", "tone", "pacing", "cut", "add")
PATTERN_WARN_THRESHOLD = 10


def review_script(script: str, post_id: str) -> tuple[str, str]:
    """
    Present the generated script to the user for manual review.

    Returns:
        (final_script, "approved")  — user accepted (possibly with edits)
        (script,       "rejected")  — user rejected; caller should restart
    """
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)

    review_path = os.path.join(config.OUTPUT_DIR, f"{post_id}_review.txt")
    log_path    = os.path.join(config.OUTPUT_DIR, f"{post_id}_edits.log")

    # 1. Save script for editing
    with open(review_path, "w", encoding="utf-8") as f:
        f.write(script)

    # 2. Open in default text editor
    _open_in_editor(review_path)

    # 3. Prompt
    print("\n" + "=" * 60)
    print("SCRIPT REVIEW — Edit the file, save it, then press")
    print("Enter to continue or type 'reject' to get a new story")
    print(f"  File: {review_path}")
    print("=" * 60)

    try:
        response = input("> ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        response = ""

    if response == "reject":
        print("[REVIEW] Story rejected — fetching a new one...\n")
        return script, "rejected"

    # 4. Read back (may be edited)
    with open(review_path, "r", encoding="utf-8") as f:
        final_script = f.read()

    # 5. Diff and log
    if final_script != script:
        edits = _diff_scripts(script, final_script)
        _write_edit_log(log_path, edits)
        _update_edit_stats(edits)
        print(f"[REVIEW] {len(edits)} edit(s) logged → {log_path}")
    else:
        print("[REVIEW] No edits detected — script accepted as-is.")

    return final_script, "approved"


# ─── Editor launcher ──────────────────────────────────────────────────────────

def _open_in_editor(path: str) -> None:
    """Open a file in the OS default text editor (non-blocking)."""
    try:
        if os.name == "nt":
            os.startfile(os.path.abspath(path))  # Windows
        elif os.uname().sysname == "Darwin":
            subprocess.Popen(["open", path])      # macOS
        else:
            subprocess.Popen(["xdg-open", path]) # Linux
    except Exception as e:
        print(f"[REVIEW] Could not auto-open editor ({e}). Open manually:\n  {path}")


# ─── Diff engine ──────────────────────────────────────────────────────────────

def _diff_scripts(original: str, edited: str) -> list[dict]:
    """
    Compare original vs edited line-by-line with difflib.
    Returns a list of edit records ready for logging.
    """
    orig_lines   = original.splitlines()
    edited_lines = edited.splitlines()

    matcher = difflib.SequenceMatcher(None, orig_lines, edited_lines, autojunk=False)
    edits   = []
    ts      = datetime.now(timezone.utc).isoformat()

    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == "equal":
            continue

        removed = orig_lines[i1:i2]
        added   = edited_lines[j1:j2]

        category = _classify_edit(tag, removed, added)
        section  = _detect_section(orig_lines, i1)

        edits.append({
            "timestamp":     ts,
            "section":       section,
            "original_text": "\n".join(removed),
            "edited_text":   "\n".join(added),
            "edit_category": category,
        })

    return edits


def _classify_edit(tag: str, removed: list[str], added: list[str]) -> str:
    """
    Heuristic classification of an edit hunk into one of the five categories.

    cut    — lines deleted, nothing added
    add    — lines added, nothing removed
    fact   — numbers / dates / capitalised proper nouns changed
    pacing — sentence length changed significantly (split / merge)
    tone   — word-level substitution (default)
    """
    if tag == "delete" or (removed and not added):
        return "cut"
    if tag == "insert" or (added and not removed):
        return "add"

    # Both sides have content — figure out the nature of the change
    orig_text = " ".join(removed)
    new_text  = " ".join(added)

    # fact: digits, year-like numbers, or capitalised words changed
    fact_pattern = re.compile(r"\b(\d+|[A-Z][a-z]{2,})\b")
    orig_facts = set(fact_pattern.findall(orig_text))
    new_facts  = set(fact_pattern.findall(new_text))
    if orig_facts.symmetric_difference(new_facts):
        return "fact"

    # pacing: line-count or average length changed by ≥40 %
    orig_word_count = len(orig_text.split())
    new_word_count  = len(new_text.split())
    if orig_word_count > 0:
        ratio = abs(orig_word_count - new_word_count) / orig_word_count
        if ratio >= 0.40:
            return "pacing"

    return "tone"


def _detect_section(lines: list[str], line_idx: int) -> str:
    """
    Walk backwards from the edit to find the nearest non-blank,
    non-continuation line — used as the section label.
    Falls back to 'line {n}'.
    """
    for i in range(line_idx - 1, -1, -1):
        stripped = lines[i].strip()
        if stripped:
            # Prefer short labels (headings / first sentence snippet)
            label = stripped[:80]
            return label
    return f"line {line_idx + 1}"


# ─── Edit log writer ──────────────────────────────────────────────────────────

def _write_edit_log(log_path: str, edits: list[dict]) -> None:
    """Append edit records to the per-story edits.log (JSON-lines format)."""
    with open(log_path, "a", encoding="utf-8") as f:
        for edit in edits:
            f.write(json.dumps(edit, ensure_ascii=False) + "\n")


# ─── Edit stats + pattern detection ──────────────────────────────────────────

def _load_edit_stats() -> dict:
    if os.path.exists(EDIT_STATS_PATH):
        with open(EDIT_STATS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {cat: 0 for cat in EDIT_CATEGORIES}


def _save_edit_stats(stats: dict) -> None:
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    with open(EDIT_STATS_PATH, "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2)


def _update_edit_stats(edits: list[dict]) -> None:
    """
    Increment per-category counters and warn when a category hits the
    threshold — then offer to run the prompt optimizer.
    """
    stats = _load_edit_stats()

    for edit in edits:
        cat = edit.get("edit_category", "tone")
        stats[cat] = stats.get(cat, 0) + 1

    _save_edit_stats(stats)

    # Check for patterns that crossed the threshold
    for cat, count in stats.items():
        # Warn at exactly the threshold and at every multiple thereafter
        if count > 0 and count % PATTERN_WARN_THRESHOLD == 0:
            print(f"\n[REVIEW] Pattern detected: '{cat}' edited frequently "
                  f"({count} times total).")
            try:
                answer = input("Run prompt optimizer? Y/N > ").strip().lower()
            except (EOFError, KeyboardInterrupt):
                answer = "n"
            if answer == "y":
                _run_prompt_optimizer(cat, count)


def _run_prompt_optimizer(category: str, count: int) -> None:
    """
    Placeholder hook for the future prompt-optimizer stage.
    For now, prints guidance based on the edit category.
    """
    advice = {
        "fact":   "Review source data accuracy and scraper output before script generation.",
        "tone":   "Adjust the tone/voice instructions in script_writer.py prompts.",
        "pacing": "Shorten narration line targets in the beat mapper or script formatter.",
        "cut":    "Tighten the script prompt — instruct the model to be more concise.",
        "add":    "The script may be under-generating context. Increase MAX_SCRIPT_WORDS or expand the prompt.",
    }
    print(f"\n[PROMPT OPTIMIZER] Category '{category}' — {count} edits recorded.")
    print(f"  Suggestion: {advice.get(category, 'Review the relevant pipeline prompt.')}")
    print("  (Full optimizer not yet implemented — log this as a backlog item.)\n")
