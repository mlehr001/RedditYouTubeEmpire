"""
media_fetcher.py — Fetches real footage and photos for mystery video entries.

Sources (in priority order):
  1. Known public footage library (YouTube embed-only, declassified/FOIA)
  2. Wikimedia Commons API (CC-licensed historical photos)
  3. Archive.org (public domain video clips)
  4. Pexels (atmospheric B-roll fallback — royalty-free)

AI: OpenAI (GPT-4o-mini) generates search queries per entry.
    Falls back to Anthropic if OpenAI key is missing.

NEVER downloads copyrighted news broadcast footage.
Only public domain, Creative Commons, or embed-only content.

Media item schema (all items returned by this module):
  {
    "type":         "photo" / "video" / "audio" / "broll",
    "url":          str,          # direct file URL or YouTube watch URL
    "embed_url":    str,          # YouTube embed URL (video only, else "")
    "local_path":   str,          # downloaded file path (photo/audio only, else "")
    "credit":       str,          # source attribution for caption overlay
    "duration":     float,        # seconds (0 if unknown)
    "transcript":   str,          # audio only — full text (empty if unavailable)
    "entry_number": int,          # which Top 5 entry: 1–5 (0 if unknown)
  }
"""

import json
import logging
import os
import re
import random

import requests

from modules.pipeline_logger import log_pipeline

log = logging.getLogger(__name__)

WIKIMEDIA_API    = "https://commons.wikimedia.org/w/api.php"
ARCHIVE_API      = "https://archive.org/advancedsearch.php"
PEXELS_API       = "https://api.pexels.com/videos/search"
HEADERS          = {"User-Agent": "MysteryEngine/1.0 (+https://github.com/story-engine)"}

REAL_PHOTOS_DIR  = os.path.join("assets", "real", "photos")
REAL_AUDIO_DIR   = os.path.join("assets", "real", "audio")

# Maximum file size to download (10 MB for photos — keeps it reasonable)
MAX_PHOTO_BYTES  = 10 * 1024 * 1024


# ─── Known Public Footage Library ─────────────────────────────────────────────
# Embed-only YouTube links — all FOIA releases or public domain.
# NEVER downloaded — only referenced as embed URLs for display.
# type is "video" (normalized schema).

KNOWN_PUBLIC_FOOTAGE = {
    "nimitz": {
        "type":         "video",
        "url":          "https://www.youtube.com/watch?v=BZEU3YCFuNg",
        "embed_url":    "https://www.youtube.com/embed/BZEU3YCFuNg",
        "local_path":   "",
        "credit":       "US Navy — FOIA declassified footage",
        "duration":     40.0,
        "transcript":   "",
        "description":  "USS Nimitz UFO FLIR footage — declassified 2017",
    },
    "tic tac": {
        "type":         "video",
        "url":          "https://www.youtube.com/watch?v=BZEU3YCFuNg",
        "embed_url":    "https://www.youtube.com/embed/BZEU3YCFuNg",
        "local_path":   "",
        "credit":       "US Navy — FOIA declassified footage",
        "duration":     40.0,
        "transcript":   "",
        "description":  "Tic-Tac UAP — US Navy FLIR camera footage",
    },
    "gimbal": {
        "type":         "video",
        "url":          "https://www.youtube.com/watch?v=5oHX-h2IGBE",
        "embed_url":    "https://www.youtube.com/embed/5oHX-h2IGBE",
        "local_path":   "",
        "credit":       "US Navy — FOIA declassified footage",
        "duration":     35.0,
        "transcript":   "",
        "description":  "Gimbal UAP — US Navy FLIR footage, declassified 2017",
    },
    "go fast": {
        "type":         "video",
        "url":          "https://www.youtube.com/watch?v=wxVRg7LLaQA",
        "embed_url":    "https://www.youtube.com/embed/wxVRg7LLaQA",
        "local_path":   "",
        "credit":       "US Navy — FOIA declassified footage",
        "duration":     35.0,
        "transcript":   "",
        "description":  "Go Fast UAP — US Navy FLIR footage, declassified",
    },
    "patterson": {
        "type":         "video",
        "url":          "https://www.youtube.com/watch?v=DqaOaaMR6j4",
        "embed_url":    "https://www.youtube.com/embed/DqaOaaMR6j4",
        "local_path":   "",
        "credit":       "Patterson-Gimlin Film — 1967 (public embed)",
        "duration":     60.0,
        "transcript":   "",
        "description":  "Patterson-Gimlin Bigfoot film — original 1967",
    },
    "bigfoot": {
        "type":         "video",
        "url":          "https://www.youtube.com/watch?v=DqaOaaMR6j4",
        "embed_url":    "https://www.youtube.com/embed/DqaOaaMR6j4",
        "local_path":   "",
        "credit":       "Patterson-Gimlin Film — 1967 (public embed)",
        "duration":     60.0,
        "transcript":   "",
        "description":  "Patterson-Gimlin Bigfoot film — original 1967",
    },
    "rendlesham": {
        "type":         "video",
        "url":          "https://www.youtube.com/watch?v=hU-v7-SqAhQ",
        "embed_url":    "https://www.youtube.com/embed/hU-v7-SqAhQ",
        "local_path":   "",
        "credit":       "BBC Archive — public domain documentary segment",
        "duration":     60.0,
        "transcript":   "",
        "description":  "Rendlesham Forest Incident — RAF Bentwaters 1980",
    },
    "skinwalker": {
        "type":         "video",
        "url":          "https://www.youtube.com/watch?v=3oeBsNzd2Ek",
        "embed_url":    "https://www.youtube.com/embed/3oeBsNzd2Ek",
        "local_path":   "",
        "credit":       "History Channel — public embed",
        "duration":     90.0,
        "transcript":   "",
        "description":  "Skinwalker Ranch — documented encounters",
    },
    "dyatlov": {
        "type":         "video",
        "url":          "https://www.youtube.com/watch?v=Y5X9B4YjN58",
        "embed_url":    "https://www.youtube.com/embed/Y5X9B4YjN58",
        "local_path":   "",
        "credit":       "Documentary — public embed",
        "duration":     60.0,
        "transcript":   "",
        "description":  "Dyatlov Pass Incident — 1959 Soviet investigation footage",
    },
    "zodiac": {
        "type":         "video",
        "url":          "https://www.youtube.com/watch?v=EhU_wMGTsgU",
        "embed_url":    "https://www.youtube.com/embed/EhU_wMGTsgU",
        "local_path":   "",
        "credit":       "Public domain news archive embed",
        "duration":     45.0,
        "transcript":   "",
        "description":  "Zodiac Killer — archival news footage",
    },
    "mh370": {
        "type":         "video",
        "url":          "https://www.youtube.com/watch?v=qQQMcgOiIHY",
        "embed_url":    "https://www.youtube.com/embed/qQQMcgOiIHY",
        "local_path":   "",
        "credit":       "Documentary — public embed",
        "duration":     60.0,
        "transcript":   "",
        "description":  "MH370 disappearance — documentary footage",
    },
}


# ─── Schema helpers ────────────────────────────────────────────────────────────

def _empty_item(entry_number: int = 0) -> dict:
    """Return a blank media item with all required fields."""
    return {
        "type":         "",
        "url":          "",
        "embed_url":    "",
        "local_path":   "",
        "credit":       "",
        "duration":     0.0,
        "transcript":   "",
        "entry_number": entry_number,
    }


def _normalize(item: dict, entry_number: int = 0) -> dict:
    """
    Ensure every media item has the full standard schema.
    Fills missing fields with safe defaults.
    Translates legacy type names (real_footage → video).
    """
    base = _empty_item(entry_number)
    base.update(item)

    # Normalise legacy type names
    type_map = {"real_footage": "video", "broll": "broll"}
    base["type"] = type_map.get(base["type"], base["type"])

    # For video items, ensure embed_url is populated if url looks like YouTube
    if base["type"] == "video" and not base["embed_url"]:
        url = base.get("url", "")
        yt_match = re.search(r"(?:v=|embed/|youtu\.be/)([A-Za-z0-9_-]{11})", url)
        if yt_match:
            vid_id = yt_match.group(1)
            base["embed_url"] = f"https://www.youtube.com/embed/{vid_id}"

    base["entry_number"] = entry_number
    base["duration"]     = float(base.get("duration") or 0.0)
    base["transcript"]   = base.get("transcript") or ""
    return base


# ─── Download helper ───────────────────────────────────────────────────────────

def _download_media_file(url: str, dest_path: str,
                         max_bytes: int = MAX_PHOTO_BYTES) -> bool:
    """
    Download a file from url to dest_path.
    Returns True on success, False on failure.
    Skips download if dest_path already exists.
    Enforces max_bytes limit to avoid large accidental downloads.
    """
    if os.path.exists(dest_path):
        return True

    try:
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        resp = requests.get(url, stream=True, timeout=30, headers=HEADERS)
        resp.raise_for_status()

        downloaded = 0
        with open(dest_path, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=65536):
                downloaded += len(chunk)
                if downloaded > max_bytes:
                    log.warning(f"Download aborted — exceeded {max_bytes} bytes: {url}")
                    fh.close()
                    os.remove(dest_path)
                    return False
                fh.write(chunk)

        return True

    except Exception as e:
        log.warning(f"Download failed for {url}: {e}")
        if os.path.exists(dest_path):
            try:
                os.remove(dest_path)
            except OSError:
                pass
        return False


# ─── Known footage ─────────────────────────────────────────────────────────────

def _check_known_footage(entry: dict, entry_number: int) -> dict | None:
    """Check if entry title matches any known public footage."""
    title_lower   = entry.get("title", "").lower()
    summary_lower = entry.get("summary", "").lower()[:200]
    combined      = title_lower + " " + summary_lower

    for key, footage in KNOWN_PUBLIC_FOOTAGE.items():
        if key in combined:
            item = footage.copy()
            item.pop("description", None)
            return _normalize(item, entry_number)
    return None


# ─── AI search query generation ───────────────────────────────────────────────

def _get_search_queries(entry: dict) -> list:
    """
    Use OpenAI (GPT-4o-mini) to generate 3 targeted media search queries.
    Falls back to Anthropic, then simple keyword extraction.
    """
    api_key       = os.getenv("OPENAI_API_KEY")
    anthropic_key = os.getenv("ANTHROPIC_API_KEY")

    title   = entry.get("title", "")
    summary = entry.get("summary", "")

    prompt = f"""Generate 3 media search queries to find footage or photos for this mystery topic.

Title: {title}
Summary: {summary[:300]}

Rules:
- Query 1: specific event name or person (e.g. "Dyatlov Pass 1959 photographs")
- Query 2: broader category (e.g. "unexplained death mountain Russia")
- Query 3: atmospheric b-roll (e.g. "dark forest night fog eerie")
- Avoid copyrighted news broadcasts
- Prefer archival, documentary, or atmospheric content

Return JSON only: {{"queries": ["specific", "broader", "atmospheric"]}}"""

    topic_id = title[:30].replace(" ", "_")

    if api_key:
        try:
            from openai import OpenAI
            client   = OpenAI(api_key=api_key)
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
                max_tokens=150,
            )
            log_pipeline(topic_id, "media_search_query", "openai/gpt-4o-mini",
                         response.usage.total_tokens)
            raw     = response.choices[0].message.content.strip()
            raw     = re.sub(r"^```(?:json)?\s*", "", raw)
            raw     = re.sub(r"\s*```$", "", raw)
            queries = json.loads(raw).get("queries", [])
            if queries:
                return queries
        except Exception as e:
            log.warning(f"OpenAI query generation failed: {e}")

    if anthropic_key:
        try:
            import anthropic
            client   = anthropic.Anthropic(api_key=anthropic_key)
            response = client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=150,
                messages=[{"role": "user", "content": prompt}],
            )
            log_pipeline(topic_id, "media_search_query", "anthropic/claude-sonnet-4-6",
                         response.usage.input_tokens + response.usage.output_tokens)
            raw     = response.content[0].text.strip()
            raw     = re.sub(r"^```(?:json)?\s*", "", raw)
            raw     = re.sub(r"\s*```$", "", raw)
            queries = json.loads(raw).get("queries", [])
            if queries:
                return queries
        except Exception as e:
            log.warning(f"Anthropic query generation failed: {e}")

    # Keyword fallback
    words = title.lower().split()[:4]
    return [" ".join(words), words[0] if words else "mystery",
            "dark mysterious fog eerie"]


# ─── Source fetchers ───────────────────────────────────────────────────────────

def _fetch_wikimedia_photos(search_query: str, entry_number: int,
                             limit: int = 3) -> list:
    """
    Search Wikimedia Commons for CC-licensed historical photos.
    Downloads each photo to assets/real/photos/entry_{n}/.
    Returns list of normalized media items.
    """
    try:
        params = {
            "action":       "query",
            "generator":    "search",
            "gsrsearch":    f"File:{search_query}",
            "gsrnamespace": 6,
            "gsrlimit":     limit,
            "prop":         "imageinfo",
            "iiprop":       "url|extmetadata",
            "format":       "json",
        }
        resp = requests.get(WIKIMEDIA_API, params=params, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        pages = resp.json().get("query", {}).get("pages", {})

        results = []
        dest_dir = os.path.join(REAL_PHOTOS_DIR, f"entry_{entry_number}")
        os.makedirs(dest_dir, exist_ok=True)

        for page in pages.values():
            info = page.get("imageinfo", [{}])[0]
            url  = info.get("url", "")
            if not url:
                continue
            if not url.lower().endswith((".jpg", ".jpeg", ".png", ".gif", ".webp")):
                continue

            metadata      = info.get("extmetadata", {})
            license_short = metadata.get("LicenseShortName", {}).get("value", "")
            artist        = metadata.get("Artist", {}).get("value", "Wikimedia Commons")
            artist        = re.sub(r"<[^>]+>", "", artist)[:60]

            # Only CC or public domain
            if not any(cc in license_short for cc in
                       ["CC", "Public domain", "PD", "cc"]):
                continue

            # Download to local file
            filename   = re.sub(r"[^A-Za-z0-9._-]", "_", os.path.basename(url))[:80]
            dest_path  = os.path.join(dest_dir, filename)
            downloaded = _download_media_file(url, dest_path)

            item = _normalize({
                "type":       "photo",
                "url":        url,
                "embed_url":  "",
                "local_path": dest_path if downloaded else "",
                "credit":     f"Wikimedia Commons — {license_short} — {artist}",
                "duration":   5.0,
                "transcript": "",
            }, entry_number)
            results.append(item)

        return results[:limit]

    except Exception as e:
        log.warning(f"Wikimedia fetch failed for '{search_query}': {e}")
        return []


def _fetch_archive_clips(search_query: str, entry_number: int,
                          limit: int = 2) -> list:
    """
    Search Archive.org for public domain video clips.
    Returns embed-only items (no download — Archive.org embeds are reliable).
    """
    try:
        params = {
            "q":     (f"({search_query}) AND mediatype:movies "
                      "AND subject:(documentary OR news OR archive)"),
            "fl[]":  ["identifier", "title"],
            "rows":  limit,
            "output":"json",
        }
        resp = requests.get(ARCHIVE_API, params=params, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        docs = resp.json().get("response", {}).get("docs", [])

        results = []
        for doc in docs:
            identifier = doc.get("identifier", "")
            title      = doc.get("title", "Archive.org clip")
            if not identifier:
                continue
            embed_url  = f"https://archive.org/embed/{identifier}"
            item = _normalize({
                "type":       "video",
                "url":        f"https://archive.org/details/{identifier}",
                "embed_url":  embed_url,
                "local_path": "",
                "credit":     f"Archive.org — Public Domain — {title[:50]}",
                "duration":   60.0,
                "transcript": "",
            }, entry_number)
            results.append(item)
        return results

    except Exception as e:
        log.warning(f"Archive.org fetch failed for '{search_query}': {e}")
        return []


def _fetch_pexels_broll(search_query: str, entry_number: int = 0) -> dict | None:
    """Fetch atmospheric B-roll from Pexels as final fallback."""
    api_key = os.getenv("PEXELS_API_KEY")
    if not api_key:
        return None

    try:
        headers = {"Authorization": api_key}
        params  = {
            "query":       search_query,
            "per_page":    5,
            "orientation": "landscape",
        }
        resp   = requests.get(PEXELS_API, headers=headers, params=params, timeout=15)
        resp.raise_for_status()
        videos = resp.json().get("videos", [])
        if not videos:
            return None

        video = random.choice(videos)
        files = sorted(
            video.get("video_files", []),
            key=lambda x: x.get("width", 0),
            reverse=True,
        )
        for f in files:
            if f.get("width", 0) >= 1280:
                return _normalize({
                    "type":       "broll",
                    "url":        f["link"],
                    "embed_url":  "",
                    "local_path": "",
                    "credit":     (
                        f"Pexels — "
                        f"{video.get('user', {}).get('name', 'Pexels contributor')} "
                        f"— Royalty Free"
                    ),
                    "duration":   float(min(int(video.get("duration", 10)), 30)),
                    "transcript": "",
                }, entry_number)

        if files:
            f = files[0]
            return _normalize({
                "type":       "broll",
                "url":        f["link"],
                "embed_url":  "",
                "local_path": "",
                "credit":     (
                    f"Pexels — "
                    f"{video.get('user', {}).get('name', 'Pexels contributor')} "
                    f"— Royalty Free"
                ),
                "duration":   float(min(int(video.get("duration", 10)), 30)),
                "transcript": "",
            }, entry_number)

    except Exception as e:
        log.warning(f"Pexels B-roll fetch failed for '{search_query}': {e}")
    return None


# ─── Public API ────────────────────────────────────────────────────────────────

def fetch_media_for_entry(entry: dict, entry_number: int = 0) -> list:
    """
    Fetch real footage and photos for a single mystery entry.

    Priority:
      0. Operator real media library (assets/real/ — manually tagged files)
      1. Known public footage (embed-only video)
      2. Wikimedia Commons (CC photos — downloaded locally)
      3. Archive.org (public domain video — embed-only)
      4. Pexels (royalty-free B-roll fallback)

    All returned items conform to the standard media item schema.
    Returns list of media items: at least one item, or [] only if all sources fail.
    """
    media_items = []
    title       = entry.get("title", "")

    # 0. Operator real media library — check before hitting any external API
    try:
        from modules.library_manager import real_find
        library_items = real_find(title)
        if library_items:
            # Stamp entry_number onto items sourced from the library
            for item in library_items:
                item["entry_number"] = entry_number
            media_items.extend(library_items)
            print(f"    [LIBRARY] {len(library_items)} item(s) from real media library")
    except Exception as exc:
        log.warning(f"Library check failed for '{title}': {exc}")

    # 1. Known footage library
    known = _check_known_footage(entry, entry_number)
    if known:
        media_items.append(known)
        desc = next(
            (v["description"] for k, v in KNOWN_PUBLIC_FOOTAGE.items()
             if k in entry.get("title", "").lower()),
            known["credit"]
        )
        print(f"    [MEDIA] Known footage: {desc[:60]}")

    # 2. AI-generated search queries
    search_queries = _get_search_queries(entry)
    print(f"    [MEDIA] Queries: {search_queries}")

    # 3. Wikimedia photos (try top 2 queries)
    for query in search_queries[:2]:
        photos = _fetch_wikimedia_photos(query, entry_number, limit=2)
        media_items.extend(photos)
        if photos:
            print(f"    [MEDIA] Wikimedia: {len(photos)} photos for '{query}'")
        if len(media_items) >= 3:
            break

    # 4. Archive.org clips (if still short)
    if len(media_items) < 2:
        clips = _fetch_archive_clips(search_queries[0], entry_number, limit=1)
        media_items.extend(clips)
        if clips:
            print(f"    [MEDIA] Archive.org: {len(clips)} clip(s)")

    # 5. Pexels atmospheric B-roll fallback
    if not media_items:
        atmospheric = (search_queries[-1]
                       if len(search_queries) >= 3
                       else "dark mysterious fog")
        broll = _fetch_pexels_broll(atmospheric, entry_number)
        if broll:
            media_items.append(broll)
            print(f"    [MEDIA] Pexels fallback: '{atmospheric}'")

    # 6. Last resort: generic mystery B-roll
    if not media_items:
        broll = _fetch_pexels_broll("dark night mystery fog atmospheric", entry_number)
        if broll:
            media_items.append(broll)
            print("    [MEDIA] Generic mystery B-roll")

    return media_items


def fetch_media_for_topic(topic: dict) -> dict:
    """
    Fetch media for all entries in a topic dict.
    Adds entry_number (1–5) to each entry and all its media items.
    Returns the topic dict with media_items[] added to each entry.
    """
    entries = topic.get("entries", [])
    print(f"  [MEDIA] Fetching media for {len(entries)} entries...")

    for i, entry in enumerate(entries):
        entry_number = len(entries) - i  # countdown order: 5→1
        entry["entry_number"] = entry_number
        print(f"  [MEDIA] Entry {entry_number} ({i + 1}/{len(entries)}): "
              f"'{entry['title'][:55]}'")
        entry["media_items"] = fetch_media_for_entry(entry, entry_number)
        print(f"    -> {len(entry['media_items'])} media items found")

    topic["entries"] = entries
    return topic
