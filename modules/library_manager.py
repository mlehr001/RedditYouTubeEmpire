"""
library_manager.py — Unified manager for both asset libraries.

Library 1: B-roll library  (assets/library/)
  - Reusable Pexels clips tagged by emotion, keywords, quality score.
  - Checked before hitting the Pexels API; clips used in the last 3
    videos are skipped to prevent repetition.
  - Index: assets/library/index.json (managed by broll.py; this module
    exposes a clean public API over broll.py's private functions).

Library 2: Real media library  (assets/real/)
  - Case-specific photos, audio recordings, and video notices.
  - Operator drops files here manually (or pipeline downloads photos).
  - Index: assets/real/index.json
  - Pipeline checks here first for real_media beats before hitting
    Wikimedia / Archive.org.

CLI usage:
  python -m modules.library_manager add \\
      --type photo --entry "Lars Mittank" \\
      --file path/to/photo.jpg --credit "Airport CCTV 2014"

  python -m modules.library_manager list [--type photo|video|audio]
  python -m modules.library_manager stats
  python -m modules.library_manager remove --file filename.jpg
"""

from __future__ import annotations

import argparse
import datetime
import json
import os
import shutil
import sys


# ── Path constants ─────────────────────────────────────────────────────────────

BROLL_LIBRARY_DIR   = os.path.join("assets", "library")
BROLL_LIBRARY_INDEX = os.path.join(BROLL_LIBRARY_DIR, "index.json")

REAL_LIBRARY_DIR    = os.path.join("assets", "real")
REAL_LIBRARY_INDEX  = os.path.join(REAL_LIBRARY_DIR, "index.json")

# Where each media type is stored on disk
REAL_TYPE_DIRS: dict[str, str] = {
    "photo": os.path.join(REAL_LIBRARY_DIR, "photos"),
    "video": os.path.join(REAL_LIBRARY_DIR, "video"),
    "audio": os.path.join(REAL_LIBRARY_DIR, "audio"),
}

VALID_TYPES = ("photo", "video", "audio")


# ══════════════════════════════════════════════════════════════════════════════
# Library 1 — B-roll library
# Wraps broll.py's private functions so callers get a stable public API.
# broll.py continues to own the library logic; we just surface it here.
# ══════════════════════════════════════════════════════════════════════════════

def broll_find(emotion: str, keywords: list,
               video_id: str) -> tuple[str | None, str | None, float]:
    """
    Check the B-roll library for a clip matching emotion + keywords.
    Skips clips used in the last 3 video runs.

    Returns (absolute_path, filename, quality_score)
    or      (None, None, 0) if nothing suitable found.
    """
    from modules.broll import _load_library, _find_in_library
    library = _load_library()
    return _find_in_library(emotion, keywords, video_id, library)


def broll_add(src_path: str, keywords_used: list, emotion: str,
              beat_position: str, quality_score: float, video_id: str) -> None:
    """
    Copy a clip into the B-roll library, tag it, and record it as used
    in video_id.  No-op if src_path does not exist.
    """
    from modules.broll import (
        _load_library, _save_library,
        _add_to_library, _mark_library_used,
    )
    if not os.path.exists(src_path):
        return
    library  = _load_library()
    library  = _add_to_library(src_path, keywords_used, emotion,
                                beat_position, quality_score, library)
    filename = os.path.basename(src_path)
    library  = _mark_library_used(filename, video_id, library)
    _save_library(library)


def broll_register_run(video_id: str) -> None:
    """
    Record a completed video run in the B-roll library's recent_videos
    list so that the last-3-videos dedup logic stays current.
    """
    from modules.broll import _load_library, _save_library, _register_video_run
    library = _load_library()
    library = _register_video_run(library, video_id)
    _save_library(library)


def broll_stats() -> dict:
    """Return a summary dict for the B-roll library."""
    from modules.broll import (
        _load_library, _library_clip_count, _get_recent_videos,
    )
    library = _load_library()
    entries = {k: v for k, v in library.items() if not k.startswith("_")}

    emotions: dict[str, int] = {}
    for v in entries.values():
        e = v.get("emotion", "unknown") or "unknown"
        emotions[e] = emotions.get(e, 0) + 1

    return {
        "count":         _library_clip_count(library),
        "total_used":    sum(v.get("times_used", 0) for v in entries.values()),
        "recent_videos": _get_recent_videos(library),
        "by_emotion":    emotions,
    }


# ══════════════════════════════════════════════════════════════════════════════
# Library 2 — Real media library
# ══════════════════════════════════════════════════════════════════════════════

# ── Index I/O ──────────────────────────────────────────────────────────────────

def _load_real_index() -> dict:
    """Load assets/real/index.json. Returns {} on missing or corrupt file."""
    if os.path.exists(REAL_LIBRARY_INDEX):
        try:
            with open(REAL_LIBRARY_INDEX, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except (json.JSONDecodeError, OSError):
            return {}
    return {}


def _save_real_index(index: dict) -> None:
    """Persist assets/real/index.json, stamping _meta.updated."""
    os.makedirs(REAL_LIBRARY_DIR, exist_ok=True)
    index.setdefault("_meta", {})
    index["_meta"]["updated"] = datetime.datetime.utcnow().isoformat()
    with open(REAL_LIBRARY_INDEX, "w", encoding="utf-8") as fh:
        json.dump(index, fh, indent=2, ensure_ascii=False)


# ── Schema helpers ─────────────────────────────────────────────────────────────

def _keywords_from_title(title: str) -> list[str]:
    """Derive lowercase keywords from an entry title, stripping stop-words."""
    stop = {
        "the", "a", "an", "in", "on", "at", "of", "and", "or",
        "is", "was", "are", "were", "to", "it", "its", "i",
    }
    return [
        w.lower().strip(".,!?\"'")
        for w in title.split()
        if len(w) > 2 and w.lower() not in stop
    ]


def _to_media_item(entry: dict) -> dict:
    """
    Convert a real-media index entry to the pipeline's standard media_item
    schema (as defined in media_fetcher.py).

    The extra '_library_filename' key lets callers call real_mark_used()
    after the clip has been consumed.
    """
    return {
        "type":               entry.get("media_type", "photo"),
        "url":                entry.get("local_path", ""),
        "embed_url":          "",
        "local_path":         entry.get("local_path", ""),
        "credit":             entry.get("credit", ""),
        "duration":           float(entry.get("duration") or 0.0),
        "transcript":         entry.get("transcript", ""),
        "entry_number":       0,          # attached by main.py
        "_library_filename":  entry.get("filename", ""),
    }


# ── CRUD ───────────────────────────────────────────────────────────────────────

def real_add(
    file_path: str,
    media_type: str,
    entry_title: str,
    credit: str,
    source: str       = "",
    transcript: str   = "",
    duration: float   = 0.0,
    keywords: list    = None,
) -> dict:
    """
    Register a file in the real media library.

    Copies the file to assets/real/{type}s/ (creates the directory if
    needed) and adds/updates an entry in assets/real/index.json.

    Returns the media_item dict (pipeline-ready).
    Raises FileNotFoundError if file_path does not exist.
    Raises ValueError if media_type is invalid.
    """
    if media_type not in VALID_TYPES:
        raise ValueError(
            f"media_type must be one of {VALID_TYPES}, got {media_type!r}"
        )
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    dest_dir  = REAL_TYPE_DIRS[media_type]
    os.makedirs(dest_dir, exist_ok=True)

    filename  = os.path.basename(file_path)
    dest_path = os.path.join(dest_dir, filename)

    if not os.path.exists(dest_path):
        shutil.copy2(file_path, dest_path)

    now  = datetime.datetime.utcnow().isoformat()
    kws  = keywords if keywords is not None else _keywords_from_title(entry_title)

    # Load existing index so we can preserve times_used if re-adding
    index = _load_real_index()
    prev  = index.get(filename, {})

    entry = {
        "filename":      filename,
        "local_path":    dest_path,
        "media_type":    media_type,
        "entry_title":   entry_title,
        "keywords":      kws,
        "source":        source or credit,
        "credit":        credit,
        "transcript":    transcript,
        "duration":      float(duration),
        "added":         prev.get("added", now),
        "times_used":    prev.get("times_used", 0),
        "last_used":     prev.get("last_used", None),
        "video_history": prev.get("video_history", []),
    }

    if "_meta" not in index:
        index["_meta"] = {"created": now}

    index[filename] = entry
    _save_real_index(index)

    return _to_media_item(entry)


def real_find(entry_title: str, media_type: str = None) -> list[dict]:
    """
    Search the real media library for items matching entry_title.

    Matching is intentionally broad — any of these triggers a hit:
      - entry_title is a substring of the indexed entry_title (or vice-versa)
      - At least one word from entry_title appears in the item's keywords

    Only returns items whose local_path exists on disk.
    Optionally filtered by media_type.
    """
    index       = _load_real_index()
    title_lower = entry_title.lower()
    title_words = set(title_lower.split())
    results: list[dict] = []

    for key, entry in index.items():
        if key.startswith("_"):
            continue
        if not os.path.exists(entry.get("local_path", "")):
            continue
        if media_type and entry.get("media_type") != media_type:
            continue

        idx_title = entry.get("entry_title", "").lower()
        idx_kws   = " ".join(entry.get("keywords", [])).lower()
        kw_words  = set(idx_kws.split())

        matched = (
            title_lower in idx_title
            or idx_title in title_lower
            or bool(title_words & kw_words)
        )
        if matched:
            results.append(_to_media_item(entry))

    return results


def real_mark_used(filename: str, video_id: str) -> None:
    """Record that a real media item was used in video_id."""
    index = _load_real_index()
    entry = index.get(filename)
    if not entry:
        return
    entry["times_used"]  = entry.get("times_used", 0) + 1
    entry["last_used"]   = datetime.datetime.utcnow().isoformat()
    history              = entry.get("video_history", [])
    history.append(video_id)
    entry["video_history"] = history[-20:]
    _save_real_index(index)


def real_list(media_type: str = None) -> list[dict]:
    """Return all raw index entries, sorted by date added. Optionally filtered."""
    index   = _load_real_index()
    entries = [
        v for k, v in index.items()
        if not k.startswith("_")
        and (not media_type or v.get("media_type") == media_type)
    ]
    entries.sort(key=lambda e: e.get("added", ""))
    return entries


def real_remove(filename: str) -> bool:
    """
    Remove an entry from the real media index.
    Does NOT delete the file from disk.
    Returns True if the entry existed and was removed.
    """
    index = _load_real_index()
    if filename in index and not filename.startswith("_"):
        del index[filename]
        _save_real_index(index)
        return True
    return False


def real_stats() -> dict:
    """Return summary statistics for the real media library."""
    entries    = real_list()
    by_type: dict[str, int] = {}
    for e in entries:
        t = e.get("media_type", "unknown")
        by_type[t] = by_type.get(t, 0) + 1
    return {
        "count":      len(entries),
        "by_type":    by_type,
        "total_used": sum(e.get("times_used", 0) for e in entries),
    }


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

def _cmd_add(args: argparse.Namespace) -> None:
    kws = args.keywords.split() if getattr(args, "keywords", None) else None
    try:
        item = real_add(
            file_path   = args.file,
            media_type  = args.type,
            entry_title = args.entry,
            credit      = args.credit,
            source      = getattr(args, "source", "") or "",
            transcript  = getattr(args, "transcript", "") or "",
            duration    = float(getattr(args, "duration", 0) or 0),
            keywords    = kws,
        )
    except (FileNotFoundError, ValueError) as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        sys.exit(1)

    print()
    print("[OK] Added to real media library")
    print(f"     Type     : {item['type']}")
    print(f"     File     : {item['local_path']}")
    print(f"     Entry    : {args.entry}")
    print(f"     Credit   : {item['credit']}")
    print()


def _cmd_list(args: argparse.Namespace) -> None:
    filter_type = getattr(args, "type", None)
    entries     = real_list(media_type=filter_type)

    if not entries:
        msg = f"(No {filter_type} entries)" if filter_type else "(Real media library is empty)"
        print(msg)
        return

    label = f" [{filter_type}]" if filter_type else ""
    sep   = "-" * 68
    print(f"\n{sep}")
    print(f"REAL MEDIA LIBRARY{label}  —  {len(entries)} item(s)")
    print(sep)

    type_tags = {"photo": "[PHOTO]", "video": "[VIDEO]", "audio": "[AUDIO]"}
    for e in entries:
        tag    = type_tags.get(e.get("media_type", ""), "[?????]")
        exists = "OK" if os.path.exists(e.get("local_path", "")) else "MISSING"
        print(f"\n  {tag} {exists:7s}  {e['filename']}")
        print(f"    Entry  : {e.get('entry_title', '')}")
        print(f"    Credit : {e.get('credit', '')}")
        if e.get("keywords"):
            print(f"    Tags   : {', '.join(e['keywords'])}")
        if e.get("transcript"):
            snip = e["transcript"][:80].replace("\n", " ")
            print(f"    Transcript: {snip}...")
        used_str = (
            f"{e.get('times_used', 0)}x  "
            f"last {e.get('last_used', 'never')[:10]}"
        )
        print(f"    Used   : {used_str}  |  Added: {e.get('added','')[:10]}")

    print(f"\n{sep}\n")


def _cmd_stats(args: argparse.Namespace) -> None:
    r = real_stats()
    b = broll_stats()

    sep = "=" * 50
    print(f"\n{sep}")
    print("LIBRARY STATISTICS")
    print(sep)

    print("\n  Real Media Library  (assets/real/)")
    print(f"  {'-' * 38}")
    print(f"  Total items : {r['count']}")
    for t in ("photo", "video", "audio"):
        n = r["by_type"].get(t, 0)
        bar = "#" * n if n else "(none)"
        print(f"    {t:6s} : {bar}  ({n})")
    print(f"  Total uses  : {r['total_used']}")

    print(f"\n  B-roll Library  (assets/library/)")
    print(f"  {'-' * 38}")
    print(f"  Total clips : {b['count']}")
    print(f"  Total uses  : {b['total_used']}")
    if b.get("by_emotion"):
        top5 = sorted(b["by_emotion"].items(), key=lambda x: -x[1])[:5]
        print(f"  Top emotions:")
        for emotion, count in top5:
            bar = "#" * min(count, 20)
            print(f"    {emotion:12s} {bar}  ({count})")
    recent = b.get("recent_videos", [])
    if recent:
        print(f"  Recent runs : {', '.join(recent[-3:])}")

    print(f"\n{sep}\n")


def _cmd_remove(args: argparse.Namespace) -> None:
    removed = real_remove(args.file)
    if removed:
        print(f"[OK] '{args.file}' removed from real media index.")
        print("     (File on disk was NOT deleted.)")
    else:
        print(f"[WARN] '{args.file}' not found in real media index.")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog        = "library_manager",
        description = "Manage the Story Engine asset libraries.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  python -m modules.library_manager add \\
      --type photo --entry "Lars Mittank" \\
      --file ~/Downloads/mittank.jpg --credit "Airport CCTV 2014"

  python -m modules.library_manager add \\
      --type audio --entry "Keddie Cabin Murders" \\
      --file ~/Downloads/911call.mp3 --credit "Plumas County 911, 1981" \\
      --transcript "There are bodies everywhere, please hurry"

  python -m modules.library_manager list --type photo
  python -m modules.library_manager stats
  python -m modules.library_manager remove --file mittank.jpg
""",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # ── add ───────────────────────────────────────────────────────────────────
    add_p = sub.add_parser("add", help="Add a file to the real media library")
    add_p.add_argument(
        "--type", required=True, choices=list(VALID_TYPES),
        help="Media type: photo | video | audio",
    )
    add_p.add_argument(
        "--entry", required=True, metavar="TITLE",
        help="Case or entry title this media belongs to",
    )
    add_p.add_argument(
        "--file", required=True, metavar="PATH",
        help="Path to the source file",
    )
    add_p.add_argument(
        "--credit", required=True,
        help="Attribution text shown as caption overlay in the video",
    )
    add_p.add_argument(
        "--source", default="",
        help="Source name (defaults to --credit value)",
    )
    add_p.add_argument(
        "--transcript", default="",
        help="Full transcript text for audio files",
    )
    add_p.add_argument(
        "--duration", type=float, default=0.0,
        help="Duration in seconds (video / audio files)",
    )
    add_p.add_argument(
        "--keywords", default="",
        help="Space-separated extra keywords for search matching",
    )

    # ── list ─────────────────────────────────────────────────────────────────
    list_p = sub.add_parser("list", help="List real media library entries")
    list_p.add_argument(
        "--type", choices=list(VALID_TYPES), default=None,
        help="Filter by media type",
    )

    # ── stats ─────────────────────────────────────────────────────────────────
    sub.add_parser("stats", help="Show statistics for both libraries")

    # ── remove ───────────────────────────────────────────────────────────────
    rm_p = sub.add_parser("remove", help="Remove an entry from the real media index")
    rm_p.add_argument(
        "--file", required=True, metavar="FILENAME",
        help="Filename (not full path) to remove from the index",
    )

    return parser


def main() -> None:
    # Ensure we can import sibling modules when run as __main__
    _project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if _project_root not in sys.path:
        sys.path.insert(0, _project_root)

    # Load .env so config is available (broll_stats needs it)
    try:
        from dotenv import load_dotenv
        load_dotenv(os.path.join(_project_root, ".env"))
    except ImportError:
        pass

    parser = _build_parser()
    args   = parser.parse_args()

    # Change to project root so relative asset paths resolve correctly
    os.chdir(_project_root)

    dispatch = {
        "add":    _cmd_add,
        "list":   _cmd_list,
        "stats":  _cmd_stats,
        "remove": _cmd_remove,
    }
    dispatch[args.command](args)


if __name__ == "__main__":
    main()
