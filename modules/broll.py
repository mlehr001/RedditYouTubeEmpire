"""
broll.py — Downloads background video clips from Pexels
Caches clips locally in assets/broll/ so we don't re-download every run
"""

import os
import random
import requests
import config


PEXELS_API_BASE = "https://api.pexels.com/videos/search"


def _get_cached_clips():
    """Returns list of already-downloaded clip paths."""
    if not os.path.exists(config.ASSETS_DIR):
        os.makedirs(config.ASSETS_DIR, exist_ok=True)
    clips = [
        os.path.join(config.ASSETS_DIR, f)
        for f in os.listdir(config.ASSETS_DIR)
        if f.endswith(".mp4")
    ]
    return clips


def _download_clip(video_url, filename):
    """Downloads a video file from Pexels."""
    path = os.path.join(config.ASSETS_DIR, filename)
    response = requests.get(video_url, stream=True, timeout=60)
    response.raise_for_status()
    with open(path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    return path


def _fetch_from_pexels(search_term):
    """
    Searches Pexels for a video matching the search term.
    Returns the download URL of the best quality file, or None.
    """
    api_key = os.getenv("PEXELS_API_KEY")
    if not api_key:
        raise ValueError("PEXELS_API_KEY not set in .env")

    headers = {"Authorization": api_key}
    params = {
        "query": search_term,
        "per_page": 10,
        "orientation": "landscape",
        "size": "large",
    }

    response = requests.get(PEXELS_API_BASE, headers=headers, params=params, timeout=15)
    response.raise_for_status()
    data = response.json()

    videos = data.get("videos", [])
    if not videos:
        return None, None

    # Pick a random video from results
    video = random.choice(videos)
    video_id = video["id"]

    # Get the highest resolution file (prefer 1080p)
    files = sorted(video.get("video_files", []), key=lambda x: x.get("width", 0), reverse=True)
    for f in files:
        if f.get("width", 0) >= 1920:
            return f["link"], f"{video_id}.mp4"

    # Fall back to best available
    if files:
        return files[0]["link"], f"{video_id}.mp4"

    return None, None


def _fetch_from_pexels_unique(search_term: str, used_ids: set) -> tuple:
    """
    Like _fetch_from_pexels but skips video IDs already in used_ids.
    Iterates all results before giving up, so uniqueness is best-effort.
    Returns (video_url, filename) or (None, None).
    """
    api_key = os.getenv("PEXELS_API_KEY")
    if not api_key:
        raise ValueError("PEXELS_API_KEY not set in .env")

    headers = {"Authorization": api_key}
    params = {
        "query": search_term,
        "per_page": 15,
        "orientation": "landscape",
        "size": "large",
    }

    response = requests.get(PEXELS_API_BASE, headers=headers, params=params, timeout=15)
    response.raise_for_status()
    data = response.json()

    videos = data.get("videos", [])
    random.shuffle(videos)

    for video in videos:
        video_id = video["id"]
        if video_id in used_ids:
            continue

        files = sorted(video.get("video_files", []), key=lambda x: x.get("width", 0), reverse=True)
        for f in files:
            if f.get("width", 0) >= 1920:
                return f["link"], f"{video_id}.mp4", video_id
        if files:
            return files[0]["link"], f"{video_id}.mp4", video_id

    return None, None, None


def get_clips_for_keywords(keywords: list) -> list:
    """
    Fetches one Pexels clip per keyword. Returns a list of local file paths.
    Falls back to cached clips for any keyword that fails to download.
    Guarantees at least one clip is returned (raises if nothing available).
    """
    os.makedirs(config.ASSETS_DIR, exist_ok=True)
    result_paths = []

    for keyword in keywords:
        try:
            video_url, filename = _fetch_from_pexels(keyword)
        except Exception as e:
            print(f"  [WARN] Pexels fetch failed for '{keyword}': {e}")
            video_url, filename = None, None

        if not video_url or not filename:
            print(f"  [SKIP] No clip found for '{keyword}'.")
            continue

        dest = os.path.join(config.ASSETS_DIR, filename)
        if os.path.exists(dest):
            result_paths.append(dest)
            print(f"  [CACHE] '{keyword}': {filename}")
        else:
            try:
                print(f"  [DL] '{keyword}': {filename}")
                path = _download_clip(video_url, filename)
                result_paths.append(path)
            except Exception as e:
                print(f"  [WARN] Download failed for '{keyword}': {e}")

    if not result_paths:
        # Hard fallback: use whatever is cached
        cached = _get_cached_clips()
        if cached:
            result_paths = cached[:1]
        else:
            raise RuntimeError(
                "No background clips available. "
                "Check your PEXELS_API_KEY and internet connection."
            )

    return result_paths


def get_clips_for_beats(beats: list) -> list:
    """
    Fetches one unique Pexels clip per beat.

    For each beat:
    - Tries keywords[0] first, then keywords[1], etc. until a fresh clip is found.
    - Never reuses the same Pexels video ID within one call.
    - Falls back to cached clips if all keyword attempts fail.

    Returns a list of dicts, one per beat:
      [{"path": str, "duration": int, "beat_name": str, "emotion": str}, ...]
    """
    os.makedirs(config.ASSETS_DIR, exist_ok=True)
    used_ids: set = set()
    result = []

    for beat in beats:
        beat_name = beat.get("name", "beat")
        emotion = beat.get("emotion", "")
        duration = beat.get("duration", 4)
        keywords = beat.get("keywords", [])

        clip_path = None

        for keyword in keywords:
            try:
                video_url, filename, video_id = _fetch_from_pexels_unique(keyword, used_ids)
            except Exception as e:
                print(f"  [WARN] Pexels fetch failed for beat '{beat_name}' / '{keyword}': {e}")
                continue

            if not video_url or not filename:
                print(f"  [SKIP] No unique clip for beat '{beat_name}' / '{keyword}'.")
                continue

            dest = os.path.join(config.ASSETS_DIR, filename)
            if os.path.exists(dest):
                clip_path = dest
                used_ids.add(video_id)
                print(f"  [CACHE] Beat '{beat_name}' [{emotion}]: {filename}")
                break
            else:
                try:
                    print(f"  [DL] Beat '{beat_name}' [{emotion}] / '{keyword}': {filename}")
                    clip_path = _download_clip(video_url, filename)
                    used_ids.add(video_id)
                    break
                except Exception as e:
                    print(f"  [WARN] Download failed for beat '{beat_name}' / '{keyword}': {e}")

        if clip_path is None:
            # Best-effort fallback: pick a cached clip not yet used this run
            cached = [c for c in _get_cached_clips() if c not in [r["path"] for r in result]]
            if not cached:
                cached = _get_cached_clips()
            if cached:
                clip_path = random.choice(cached)
                print(f"  [FALLBACK] Beat '{beat_name}': using cached {os.path.basename(clip_path)}")
            else:
                print(f"  [ERROR] No clip available for beat '{beat_name}' — skipping.")
                continue

        result.append({
            "path":      clip_path,
            "duration":  duration,
            "beat_name": beat_name,
            "emotion":   emotion,
        })

    if not result:
        raise RuntimeError(
            "No clips resolved for any beat. "
            "Check PEXELS_API_KEY and internet connection."
        )

    return result


def get_background_clip():
    """
    Returns a path to a local background video clip.
    Uses cached clips first; downloads a new one if cache is low.
    """
    cached = _get_cached_clips()

    # Refresh cache if running low
    if len(cached) < config.BROLL_CACHE_COUNT:
        search_term = random.choice(config.BROLL_SEARCH_TERMS)
        print(f"  Searching Pexels for: '{search_term}'...")

        video_url, filename = _fetch_from_pexels(search_term)
        if video_url and filename:
            # Don't re-download if we already have this clip
            if filename not in [os.path.basename(c) for c in cached]:
                print(f"  Downloading clip: {filename}")
                new_clip = _download_clip(video_url, filename)
                cached.append(new_clip)

    if not cached:
        raise RuntimeError(
            "No background clips available. "
            "Check your PEXELS_API_KEY and internet connection."
        )

    return random.choice(cached)
