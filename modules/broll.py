"""
broll.py — Downloads background video clips from Pexels
Caches clips locally in assets/broll/
Maintains a local asset library at assets/library/ for quality compound
"""

import os
import json
import shutil
import random
import datetime
import requests
import config


PEXELS_API_BASE = "https://api.pexels.com/videos/search"
LIBRARY_DIR     = os.path.join("assets", "library")
LIBRARY_INDEX   = os.path.join(LIBRARY_DIR, "index.json")

# Emotion → Pexels search modifiers
EMOTION_EXPANSIONS = {
    "dread":    "dark moody cinematic",
    "shock":    "dramatic intense",
    "mystery":  "eerie atmospheric fog",
    "suspense": "tension slow motion",
}

# beat script_position prefix → topic category append
POSITION_CATEGORIES = {
    "entry_1": "alien",
    "entry_2": "mystery",
    "entry_3": "crime",
    "entry_4": "conspiracy",
    "entry_5": "paranormal",
}


# ── Clip scoring ──────────────────────────────────────────────────────────────

def _score_clip(video: dict) -> float:
    """
    Score a Pexels video 0–3 (higher = better).
    Components: duration_score + orientation_score + quality_score.
    """
    duration = video.get("duration", 0)
    files    = video.get("video_files", [])
    width    = max((f.get("width",  0) for f in files), default=0)
    height   = max((f.get("height", 0) for f in files), default=0)

    # duration_score: 4–10 s ideal
    if 4 <= duration <= 10:
        duration_score = 1.0
    elif duration < 4:
        duration_score = 0.3
    else:
        duration_score = 0.6  # > 10 s, still usable

    # orientation_score: landscape preferred
    if width > 0 and height > 0:
        orientation_score = 1.0 if width >= height else 0.3
    else:
        orientation_score = 0.5

    # quality_score: HD preferred
    if width >= 1920:
        quality_score = 1.0
    elif width >= 1280:
        quality_score = 0.7
    else:
        quality_score = 0.3

    return duration_score + orientation_score + quality_score


# ── Keyword expansion ─────────────────────────────────────────────────────────

def _expand_keyword(keyword: str, beat: dict) -> str:
    """
    Expand a keyword using beat context for richer Pexels queries.

    Appends (in order):
    - Up to 3 long words from script_excerpt
    - Topic category derived from script_position
    - Emotion modifier from EMOTION_EXPANSIONS
    """
    parts = [keyword]

    excerpt = beat.get("script_excerpt", "") or ""
    if excerpt:
        words = [w.strip(".,!?\"'") for w in excerpt.split() if len(w) > 4]
        parts.extend(words[:3])

    position = (beat.get("script_position", "") or beat.get("name", "") or "").lower()
    for prefix, category in POSITION_CATEGORIES.items():
        if prefix in position:
            parts.append(category)
            break

    emotion = (beat.get("emotion", "") or "").lower()
    if emotion in EMOTION_EXPANSIONS:
        parts.append(EMOTION_EXPANSIONS[emotion])

    # Deduplicate, preserve order
    seen   = set()
    result = []
    for p in parts:
        key = p.lower()
        if key not in seen:
            seen.add(key)
            result.append(p)

    return " ".join(result)


# ── Local asset library ───────────────────────────────────────────────────────

def _load_library() -> dict:
    """Load library index from disk. Returns {} if not found."""
    if os.path.exists(LIBRARY_INDEX):
        try:
            with open(LIBRARY_INDEX, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return {}
    return {}


def _save_library(library: dict):
    """Persist library index to disk."""
    os.makedirs(LIBRARY_DIR, exist_ok=True)
    with open(LIBRARY_INDEX, "w") as f:
        json.dump(library, f, indent=2)


def _library_clip_count(library: dict) -> int:
    return sum(1 for k in library if not k.startswith("_"))


def _get_recent_videos(library: dict) -> list:
    return library.get("_meta", {}).get("recent_videos", [])


def _register_video_run(library: dict, video_id: str) -> dict:
    """Add video_id to the global recent_videos list (keep last 10)."""
    meta = library.setdefault("_meta", {"recent_videos": []})
    history = meta.get("recent_videos", [])
    if not history or history[-1] != video_id:
        history.append(video_id)
    meta["recent_videos"] = history[-10:]
    return library


def _add_to_library(src_path: str, keywords_used: list, emotion: str,
                    beat_position: str, quality_score: float, library: dict) -> dict:
    """Copy clip to library folder and register metadata. Returns updated library."""
    os.makedirs(LIBRARY_DIR, exist_ok=True)
    filename = os.path.basename(src_path)
    dest     = os.path.join(LIBRARY_DIR, filename)

    if not os.path.exists(dest):
        try:
            shutil.copy2(src_path, dest)
        except OSError as e:
            print(f"  [WARN] Library copy failed for {filename}: {e}")
            return library

    if filename not in library:
        library[filename] = {
            "filename":      filename,
            "keywords_used": keywords_used,
            "emotion":       emotion,
            "beat_position": beat_position,
            "times_used":    0,
            "last_used":     None,
            "quality_score": round(quality_score, 3),
            "video_history": [],
        }

    return library


def _mark_library_used(filename: str, video_id: str, library: dict) -> dict:
    """Record this clip was used in video_id. Returns updated library."""
    entry = library.get(filename)
    if not entry:
        return library
    entry["times_used"] = entry.get("times_used", 0) + 1
    entry["last_used"]  = datetime.datetime.utcnow().isoformat()
    history = entry.get("video_history", [])
    history.append(video_id)
    entry["video_history"] = history[-20:]
    return library


def _find_in_library(emotion: str, keywords: list, video_id: str,
                     library: dict):
    """
    Find the best-scoring library clip matching emotion + keywords.
    Skips clips used in the last 3 video runs.

    Returns (path, filename, quality_score) or (None, None, 0).
    """
    recent_videos = _get_recent_videos(library)
    last_3        = set(recent_videos[-3:]) if recent_videos else set()
    keyword_set   = {k.lower() for k in keywords}
    candidates    = []

    for filename, entry in library.items():
        if filename.startswith("_"):
            continue
        lib_path = os.path.join(LIBRARY_DIR, filename)
        if not os.path.exists(lib_path):
            continue

        # Skip if used in any of the last 3 videos
        clip_history = set(entry.get("video_history", []))
        if clip_history & last_3:
            continue

        score = 0.0
        if (entry.get("emotion", "") or "").lower() == emotion.lower():
            score += 2.0
        lib_keywords = {k.lower() for k in entry.get("keywords_used", [])}
        score += len(keyword_set & lib_keywords) * 0.5
        score += entry.get("quality_score", 0)

        if score > 0:
            candidates.append((score, filename, entry.get("quality_score", 0)))

    if not candidates:
        return None, None, 0

    candidates.sort(key=lambda x: x[0], reverse=True)
    best_score, best_file, best_q = candidates[0]
    return os.path.join(LIBRARY_DIR, best_file), best_file, best_q


# ── Pexels helpers ────────────────────────────────────────────────────────────

def _get_best_file(video: dict) -> tuple:
    """Return (url, width, height) for the highest-resolution video file."""
    files = sorted(video.get("video_files", []),
                   key=lambda x: x.get("width", 0), reverse=True)
    for f in files:
        if f.get("width", 0) >= 1920:
            return f["link"], f.get("width", 0), f.get("height", 0)
    if files:
        f = files[0]
        return f["link"], f.get("width", 0), f.get("height", 0)
    return None, 0, 0


def _get_cached_clips() -> list:
    """Returns list of already-downloaded clip paths in assets/broll/."""
    if not os.path.exists(config.ASSETS_DIR):
        os.makedirs(config.ASSETS_DIR, exist_ok=True)
    return [
        os.path.join(config.ASSETS_DIR, f)
        for f in os.listdir(config.ASSETS_DIR)
        if f.endswith(".mp4")
    ]


def _download_clip(video_url: str, filename: str, dest_dir: str = None) -> str:
    """Downloads a video file from Pexels to dest_dir (default: assets/broll/)."""
    if dest_dir is None:
        dest_dir = config.ASSETS_DIR
    path = os.path.join(dest_dir, filename)
    response = requests.get(video_url, stream=True, timeout=60)
    response.raise_for_status()
    with open(path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    return path


def _fetch_from_pexels_scored(search_term: str,
                               used_ids: set = None) -> tuple:
    """
    Search Pexels with per_page=20, score every result, return the top clip.

    Returns (video_url, filename, video_id, quality_score)
    or      (None, None, None, 0) on failure.
    Logs: keyword, clips_returned, clip_selected, scores.
    """
    api_key = os.getenv("PEXELS_API_KEY")
    if not api_key:
        raise ValueError("PEXELS_API_KEY not set in .env")

    used_ids = used_ids or set()
    headers  = {"Authorization": api_key}
    params   = {
        "query":       search_term,
        "per_page":    20,
        "orientation": "landscape",
        "size":        "large",
    }

    response = requests.get(PEXELS_API_BASE, headers=headers,
                             params=params, timeout=15)
    response.raise_for_status()
    data           = response.json()
    videos         = data.get("videos", [])
    clips_returned = len(videos)

    if not videos:
        print(f"  [PEXELS] keyword='{search_term}' clips_returned=0")
        return None, None, None, 0

    scored = []
    for video in videos:
        vid_id = video["id"]
        if vid_id in used_ids:
            continue
        scored.append((_score_clip(video), video))

    if not scored:
        print(f"  [PEXELS] keyword='{search_term}' clips_returned={clips_returned} "
              f"all_used=True")
        return None, None, None, 0

    scored.sort(key=lambda x: x[0], reverse=True)
    best_score, best_video = scored[0]
    video_id               = best_video["id"]
    url, width, height     = _get_best_file(best_video)

    if not url:
        return None, None, None, 0

    print(
        f"  [PEXELS] keyword='{search_term}' clips_returned={clips_returned} "
        f"clip_selected={video_id} score={best_score:.2f} "
        f"duration={best_video.get('duration')}s res={width}x{height}"
    )
    return url, f"{video_id}.mp4", video_id, best_score


def _fetch_from_pexels(search_term: str) -> tuple:
    """Thin wrapper for legacy callers — returns (url, filename)."""
    url, filename, _vid_id, _score = _fetch_from_pexels_scored(search_term)
    return url, filename


# ── Public API ────────────────────────────────────────────────────────────────

def get_clips_for_beats(beats: list, video_id: str = None) -> list:
    """
    Fetches one unique clip per beat.

    For each beat:
      1. Expand keywords using beat context (emotion, script_excerpt, position).
      2. If library has 50+ clips and a matching non-recently-used clip: use it.
      3. Otherwise search Pexels (per_page=20, scored), download the best result.
      4. After any Pexels download: copy to library and tag it.
      5. Never reuse the same Pexels video ID within one call.

    Args:
      beats:    list of beat dicts (name, emotion, duration, keywords, …)
      video_id: identifier for this video run (used for library dedup across runs)

    Returns list of dicts:
      [{"path": str, "duration": int, "beat_name": str, "emotion": str}, ...]
    """
    os.makedirs(config.ASSETS_DIR, exist_ok=True)
    os.makedirs(LIBRARY_DIR, exist_ok=True)

    library      = _load_library()
    lib_size     = _library_clip_count(library)
    lib_primary  = lib_size >= 50

    if video_id is None:
        video_id = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    used_pexels_ids: set = set()
    result = []

    for beat in beats:
        beat_name     = beat.get("name", "beat")
        emotion       = beat.get("emotion", "") or ""
        duration      = beat.get("duration", 4)
        keywords      = beat.get("keywords", []) or []
        beat_position = beat.get("script_position", "") or beat_name

        clip_path     = None
        quality_score = 0.0
        keywords_used = list(keywords)

        # ── 1. Library lookup ─────────────────────────────────────────────────
        if lib_size > 0:
            lib_path, lib_filename, lib_q = _find_in_library(
                emotion, keywords, video_id, library
            )
            if lib_path:
                clip_path     = lib_path
                quality_score = lib_q
                library       = _mark_library_used(lib_filename, video_id, library)
                source        = "LIBRARY" if lib_primary else "LIBRARY(early)"
                print(f"  [{source}] Beat '{beat_name}' [{emotion}]: {lib_filename} "
                      f"(q={lib_q:.2f})")

        # ── 2. Pexels search ──────────────────────────────────────────────────
        if clip_path is None:
            for keyword in keywords:
                expanded = _expand_keyword(keyword, beat)
                try:
                    vid_url, filename, pexels_id, q_score = _fetch_from_pexels_scored(
                        expanded, used_pexels_ids
                    )
                except Exception as e:
                    print(f"  [WARN] Pexels failed for beat '{beat_name}' / "
                          f"'{expanded}': {e}")
                    continue

                if not vid_url or not filename:
                    continue

                dest = os.path.join(config.ASSETS_DIR, filename)
                if os.path.exists(dest):
                    clip_path     = dest
                    quality_score = q_score
                    keywords_used = [expanded]
                    used_pexels_ids.add(pexels_id)
                    print(f"  [CACHE] Beat '{beat_name}' [{emotion}]: {filename}")
                    break
                else:
                    try:
                        print(f"  [DL] Beat '{beat_name}' [{emotion}] / "
                              f"'{expanded}': {filename}")
                        clip_path     = _download_clip(vid_url, filename)
                        quality_score = q_score
                        keywords_used = [expanded]
                        used_pexels_ids.add(pexels_id)
                        break
                    except Exception as e:
                        print(f"  [WARN] Download failed for beat "
                              f"'{beat_name}' / '{keyword}': {e}")

        # ── 3. Add to library after Pexels download ───────────────────────────
        if clip_path and not clip_path.startswith(LIBRARY_DIR):
            library = _add_to_library(
                clip_path, keywords_used, emotion,
                beat_position, quality_score, library
            )
            lib_filename = os.path.basename(clip_path)
            library      = _mark_library_used(lib_filename, video_id, library)

        # ── 4. Hard fallback: any cached clip ─────────────────────────────────
        if clip_path is None:
            used_paths = {r["path"] for r in result}
            cached     = [c for c in _get_cached_clips() if c not in used_paths]
            if not cached:
                cached = _get_cached_clips()
            if cached:
                clip_path = random.choice(cached)
                print(f"  [FALLBACK] Beat '{beat_name}': "
                      f"{os.path.basename(clip_path)}")
            else:
                print(f"  [ERROR] No clip for beat '{beat_name}' — skipping.")
                continue

        result.append({
            "path":      clip_path,
            "duration":  duration,
            "beat_name": beat_name,
            "emotion":   emotion,
        })

    # Persist library with this video_id in the run history
    library = _register_video_run(library, video_id)
    _save_library(library)

    if not result:
        raise RuntimeError(
            "No clips resolved for any beat. "
            "Check PEXELS_API_KEY and internet connection."
        )

    return result


def get_clips_for_keywords(keywords: list) -> list:
    """
    Fetches one Pexels clip per keyword. Returns a list of local file paths.
    Falls back to cached clips for any keyword that fails to download.
    """
    os.makedirs(config.ASSETS_DIR, exist_ok=True)
    result_paths = []

    for keyword in keywords:
        try:
            vid_url, filename, _vid_id, _score = _fetch_from_pexels_scored(keyword)
        except Exception as e:
            print(f"  [WARN] Pexels fetch failed for '{keyword}': {e}")
            vid_url, filename = None, None

        if not vid_url or not filename:
            print(f"  [SKIP] No clip found for '{keyword}'.")
            continue

        dest = os.path.join(config.ASSETS_DIR, filename)
        if os.path.exists(dest):
            result_paths.append(dest)
            print(f"  [CACHE] '{keyword}': {filename}")
        else:
            try:
                print(f"  [DL] '{keyword}': {filename}")
                path = _download_clip(vid_url, filename)
                result_paths.append(path)
            except Exception as e:
                print(f"  [WARN] Download failed for '{keyword}': {e}")

    if not result_paths:
        cached = _get_cached_clips()
        if cached:
            result_paths = cached[:1]
        else:
            raise RuntimeError(
                "No background clips available. "
                "Check your PEXELS_API_KEY and internet connection."
            )

    return result_paths


def get_background_clip() -> str:
    """
    Returns a path to a local background video clip.
    Uses cached clips first; downloads a new one if cache is low.
    """
    cached = _get_cached_clips()

    if len(cached) < config.BROLL_CACHE_COUNT:
        search_term = random.choice(config.BROLL_SEARCH_TERMS)
        print(f"  Searching Pexels for: '{search_term}'...")
        vid_url, filename = _fetch_from_pexels(search_term)
        if vid_url and filename:
            if filename not in [os.path.basename(c) for c in cached]:
                print(f"  Downloading clip: {filename}")
                new_clip = _download_clip(vid_url, filename)
                cached.append(new_clip)

    if not cached:
        raise RuntimeError(
            "No background clips available. "
            "Check your PEXELS_API_KEY and internet connection."
        )

    return random.choice(cached)
