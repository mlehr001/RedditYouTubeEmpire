"""
music_manager.py — Downloads and manages royalty-free background music.

Primary:   Pixabay API (requires PIXABAY_API_KEY)
Secondary: Freesound API (requires FREESOUND_CLIENT_ID)
Fallback:  Returns None (editor will proceed with narration only)

Music is downloaded to assets/music/ and cached across runs.

Volume settings (applied in editor):
  - Normal: 0.08–0.12 (subtle under voice)
  - Under number cards: duck to 0.04
  - Fade in: first 3 seconds
  - Fade out: last 5 seconds
"""

import logging
import os
import re
import requests

log = logging.getLogger(__name__)

MUSIC_DIR = os.path.join("assets", "music")
PIXABAY_API = "https://pixabay.com/api/"
FREESOUND_API = "https://freesound.org/apiv2"

# Default music volume level (used externally by editor)
MUSIC_VOLUME = 0.10
MUSIC_VOLUME_DUCK = 0.04   # under number cards

# Search terms per mystery category
CATEGORY_SEARCH_TERMS = {
    "alien_sightings": ["eerie atmospheric space drone", "mysterious ambient ufo"],
    "unsolved_disappearances": ["true crime piano suspense", "missing dark ambient"],
    "unexplained_photos": ["dark ambient mystery drone", "eerie atmospheric tension"],
    "mysterious_deaths": ["ominous orchestral tension", "dark mystery suspense"],
    "strange_cold_cases": ["suspenseful documentary strings", "cold case thriller piano"],
    "default": ["dark ambient mystery", "eerie atmospheric drone"],
}


def _slugify(text: str) -> str:
    slug = re.sub(r"[^\w\s-]", "", text.lower())
    slug = re.sub(r"[\s-]+", "_", slug)
    return slug[:50]


def _get_cached_tracks() -> list:
    """Return list of already-downloaded .mp3 paths in MUSIC_DIR."""
    if not os.path.exists(MUSIC_DIR):
        return []
    return [
        os.path.join(MUSIC_DIR, f)
        for f in os.listdir(MUSIC_DIR)
        if f.endswith((".mp3", ".ogg", ".wav"))
    ]


def _download_file(url: str, dest_path: str) -> str:
    """Download a file from url to dest_path. Returns dest_path."""
    headers = {"User-Agent": "MysteryEngine/1.0"}
    resp = requests.get(url, headers=headers, stream=True, timeout=60)
    resp.raise_for_status()
    with open(dest_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)
    return dest_path


def _fetch_from_pixabay(search_term: str) -> str | None:
    """
    Search Pixabay for a royalty-free music track matching search_term.
    Downloads it to MUSIC_DIR and returns the local path.
    Requires PIXABAY_API_KEY in environment.
    """
    api_key = os.getenv("PIXABAY_API_KEY")
    if not api_key:
        return None

    try:
        params = {
            "key": api_key,
            "q": search_term,
            "video_type": "music",  # Pixabay music endpoint flag
            "per_page": 10,
        }
        # Pixabay music uses the /api/ endpoint with video_type=music
        resp = requests.get(PIXABAY_API, params=params, timeout=15)
        resp.raise_for_status()
        hits = resp.json().get("hits", [])

        if not hits:
            return None

        # Pick first result
        hit = hits[0]
        audio_url = hit.get("audio", {}).get("url", "") or hit.get("pageURL", "")
        track_id = hit.get("id", "unknown")
        tags = hit.get("tags", "track").replace(",", "_").replace(" ", "")[:30]

        if not audio_url or not audio_url.startswith("http"):
            return None

        slug = _slugify(f"pixabay_{track_id}_{tags}")
        ext = ".mp3" if ".mp3" in audio_url else ".ogg"
        dest_path = os.path.join(MUSIC_DIR, f"{slug}{ext}")

        if os.path.exists(dest_path):
            return dest_path

        print(f"    [MUSIC] Downloading Pixabay track: {slug}{ext}")
        return _download_file(audio_url, dest_path)

    except Exception as e:
        log.warning(f"Pixabay music fetch failed for '{search_term}': {e}")
        return None


def _fetch_from_freesound(search_term: str) -> str | None:
    """
    Search Freesound.org for a CC-licensed ambient track.
    Requires FREESOUND_CLIENT_ID in environment (client credentials flow).
    """
    client_id = os.getenv("FREESOUND_CLIENT_ID")
    client_secret = os.getenv("FREESOUND_CLIENT_SECRET")
    if not client_id or not client_secret:
        return None

    try:
        # Get OAuth token via client credentials
        token_resp = requests.post(
            f"{FREESOUND_API}/oauth2/access_token/",
            data={
                "client_id": client_id,
                "client_secret": client_secret,
                "grant_type": "client_credentials",
            },
            timeout=15,
        )
        token_resp.raise_for_status()
        access_token = token_resp.json().get("access_token", "")
        if not access_token:
            return None

        headers = {"Authorization": f"Bearer {access_token}"}

        # Search for ambient/drone/mystery tracks
        search_params = {
            "query": search_term,
            "filter": 'license:("Creative Commons 0" OR "Attribution" OR "Attribution NonCommercial")',
            "fields": "id,name,previews,duration,license",
            "sort": "rating_desc",
            "page_size": 5,
        }
        search_resp = requests.get(
            f"{FREESOUND_API}/search/text/",
            params=search_params,
            headers=headers,
            timeout=15,
        )
        search_resp.raise_for_status()
        results = search_resp.json().get("results", [])

        if not results:
            return None

        # Pick a track with duration > 60s for looping
        for sound in results:
            duration = float(sound.get("duration", 0))
            if duration < 30:
                continue

            preview_url = sound.get("previews", {}).get("preview-hq-mp3", "")
            if not preview_url:
                preview_url = sound.get("previews", {}).get("preview-lq-mp3", "")
            if not preview_url:
                continue

            sound_id = sound.get("id", "unknown")
            sound_name = _slugify(sound.get("name", "track"))
            dest_path = os.path.join(MUSIC_DIR, f"freesound_{sound_id}_{sound_name}.mp3")

            if os.path.exists(dest_path):
                return dest_path

            print(f"    [MUSIC] Downloading Freesound track: {sound_name}")
            return _download_file(preview_url, dest_path)

    except Exception as e:
        log.warning(f"Freesound fetch failed for '{search_term}': {e}")
    return None


def get_music_for_category(category: str = "default") -> str | None:
    """
    Select or download a background music track for the given mystery category.

    Returns:
        Local file path to an audio file (.mp3/.ogg), or None if no music available.
        When None, the editor should proceed with narration-only audio.
    """
    os.makedirs(MUSIC_DIR, exist_ok=True)

    search_terms = CATEGORY_SEARCH_TERMS.get(category, CATEGORY_SEARCH_TERMS["default"])

    # Check cache first
    cached = _get_cached_tracks()
    if cached:
        import random
        # Pick a cached track at random (variety across videos)
        track = random.choice(cached)
        print(f"  [MUSIC] Using cached track: {os.path.basename(track)}")
        return track

    # Try Pixabay
    for term in search_terms:
        print(f"  [MUSIC] Trying Pixabay: '{term}'...")
        path = _fetch_from_pixabay(term)
        if path:
            print(f"  [MUSIC] Pixabay track ready: {os.path.basename(path)}")
            return path

    # Try Freesound
    for term in search_terms:
        print(f"  [MUSIC] Trying Freesound: '{term}'...")
        path = _fetch_from_freesound(term)
        if path:
            print(f"  [MUSIC] Freesound track ready: {os.path.basename(path)}")
            return path

    print("  [MUSIC] No music API keys configured — proceeding without background music.")
    print("          Add PIXABAY_API_KEY or FREESOUND_CLIENT_ID to .env to enable music.")
    return None
