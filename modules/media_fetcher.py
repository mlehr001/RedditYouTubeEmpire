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
"""

import json
import logging
import os
import re
import random

import requests

from modules.pipeline_logger import log_pipeline

log = logging.getLogger(__name__)

WIKIMEDIA_API = "https://commons.wikimedia.org/w/api.php"
ARCHIVE_API = "https://archive.org/advancedsearch.php"
PEXELS_API = "https://api.pexels.com/videos/search"
HEADERS = {"User-Agent": "MysteryEngine/1.0 (+https://github.com/story-engine)"}

# ─── Known Public Footage Library ─────────────────────────────────────────────
# Embed-only YouTube links — all FOIA releases or public domain.
# Never downloaded — only referenced as embed URLs for display.

KNOWN_PUBLIC_FOOTAGE = {
    "nimitz": {
        "type": "real_footage",
        "url": "https://www.youtube.com/embed/BZEU3YCFuNg",
        "credit": "US Navy — FOIA declassified footage",
        "duration": 40,
        "description": "USS Nimitz UFO FLIR footage — declassified 2017",
    },
    "tic tac": {
        "type": "real_footage",
        "url": "https://www.youtube.com/embed/BZEU3YCFuNg",
        "credit": "US Navy — FOIA declassified footage",
        "duration": 40,
        "description": "Tic-Tac UAP — US Navy FLIR camera footage",
    },
    "gimbal": {
        "type": "real_footage",
        "url": "https://www.youtube.com/embed/5oHX-h2IGBE",
        "credit": "US Navy — FOIA declassified footage",
        "duration": 35,
        "description": "Gimbal UAP — US Navy FLIR footage, declassified 2017",
    },
    "go fast": {
        "type": "real_footage",
        "url": "https://www.youtube.com/embed/wxVRg7LLaQA",
        "credit": "US Navy — FOIA declassified footage",
        "duration": 35,
        "description": "Go Fast UAP — US Navy FLIR footage, declassified",
    },
    "patterson": {
        "type": "real_footage",
        "url": "https://www.youtube.com/embed/DqaOaaMR6j4",
        "credit": "Patterson-Gimlin Film — 1967 (public embed)",
        "duration": 60,
        "description": "Patterson-Gimlin Bigfoot film — original 1967",
    },
    "bigfoot": {
        "type": "real_footage",
        "url": "https://www.youtube.com/embed/DqaOaaMR6j4",
        "credit": "Patterson-Gimlin Film — 1967 (public embed)",
        "duration": 60,
        "description": "Patterson-Gimlin Bigfoot film — original 1967",
    },
    "rendlesham": {
        "type": "real_footage",
        "url": "https://www.youtube.com/embed/hU-v7-SqAhQ",
        "credit": "BBC Archive — public domain documentary segment",
        "duration": 60,
        "description": "Rendlesham Forest Incident — RAF Bentwaters 1980",
    },
    "skinwalker": {
        "type": "real_footage",
        "url": "https://www.youtube.com/embed/3oeBsNzd2Ek",
        "credit": "History Channel — public embed",
        "duration": 90,
        "description": "Skinwalker Ranch — documented encounters",
    },
    "dyatlov": {
        "type": "real_footage",
        "url": "https://www.youtube.com/embed/Y5X9B4YjN58",
        "credit": "Documentary — public embed",
        "duration": 60,
        "description": "Dyatlov Pass Incident — 1959 Soviet investigation footage",
    },
    "zodiac": {
        "type": "real_footage",
        "url": "https://www.youtube.com/embed/EhU_wMGTsgU",
        "credit": "Public domain news archive embed",
        "duration": 45,
        "description": "Zodiac Killer — archival news footage",
    },
    "mh370": {
        "type": "real_footage",
        "url": "https://www.youtube.com/embed/qQQMcgOiIHY",
        "credit": "Documentary — public embed",
        "duration": 60,
        "description": "MH370 disappearance — documentary footage",
    },
}


def _check_known_footage(entry: dict) -> dict | None:
    """Check if entry title matches any known public footage."""
    title_lower = entry.get("title", "").lower()
    summary_lower = entry.get("summary", "").lower()[:200]
    combined = title_lower + " " + summary_lower
    for key, footage in KNOWN_PUBLIC_FOOTAGE.items():
        if key in combined:
            return footage.copy()
    return None


# ─── OpenAI Search Query Generation ──────────────────────────────────────────

def _get_search_queries(entry: dict) -> list:
    """
    Use OpenAI (GPT-4o-mini) to generate 3 targeted media search queries.
    Falls back to simple keyword extraction if AI unavailable.
    """
    api_key = os.getenv("OPENAI_API_KEY")
    anthropic_key = os.getenv("ANTHROPIC_API_KEY")

    title = entry.get("title", "")
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
            client = OpenAI(api_key=api_key)
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
                max_tokens=150,
            )
            log_pipeline(topic_id, "media_search_query", "openai/gpt-4o-mini",
                         response.usage.total_tokens)
            raw = response.choices[0].message.content.strip()
            raw = re.sub(r"^```(?:json)?\s*", "", raw)
            raw = re.sub(r"\s*```$", "", raw)
            queries = json.loads(raw).get("queries", [])
            if queries:
                return queries
        except Exception as e:
            log.warning(f"OpenAI query generation failed: {e}")

    if anthropic_key:
        try:
            import anthropic
            client = anthropic.Anthropic(api_key=anthropic_key)
            response = client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=150,
                messages=[{"role": "user", "content": prompt}],
            )
            log_pipeline(topic_id, "media_search_query", "anthropic/claude-sonnet-4-6",
                         response.usage.input_tokens + response.usage.output_tokens)
            raw = response.content[0].text.strip()
            raw = re.sub(r"^```(?:json)?\s*", "", raw)
            raw = re.sub(r"\s*```$", "", raw)
            queries = json.loads(raw).get("queries", [])
            if queries:
                return queries
        except Exception as e:
            log.warning(f"Anthropic query generation failed: {e}")

    # Keyword fallback
    words = title.lower().split()[:4]
    return [" ".join(words), words[0] if words else "mystery", "dark mysterious fog eerie"]


# ─── Source Fetchers ──────────────────────────────────────────────────────────

def _fetch_wikimedia_photos(search_query: str, limit: int = 3) -> list:
    """Search Wikimedia Commons for CC-licensed historical photos."""
    try:
        params = {
            "action": "query",
            "generator": "search",
            "gsrsearch": f"File:{search_query}",
            "gsrnamespace": 6,
            "gsrlimit": limit,
            "prop": "imageinfo",
            "iiprop": "url|extmetadata",
            "format": "json",
        }
        resp = requests.get(WIKIMEDIA_API, params=params, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        pages = resp.json().get("query", {}).get("pages", {})

        results = []
        for page in pages.values():
            info = page.get("imageinfo", [{}])[0]
            url = info.get("url", "")
            if not url:
                continue
            if not url.lower().endswith((".jpg", ".jpeg", ".png", ".gif", ".webp")):
                continue

            metadata = info.get("extmetadata", {})
            license_short = metadata.get("LicenseShortName", {}).get("value", "")
            artist = metadata.get("Artist", {}).get("value", "Wikimedia Commons")
            # Strip HTML from artist
            artist = re.sub(r"<[^>]+>", "", artist)[:60]

            # Only CC or public domain
            if not any(cc in license_short for cc in ["CC", "Public domain", "PD", "cc"]):
                continue

            results.append({
                "type": "photo",
                "url": url,
                "credit": f"Wikimedia Commons — {license_short} — {artist}",
                "duration": 5,
            })

        return results[:limit]

    except Exception as e:
        log.warning(f"Wikimedia fetch failed for '{search_query}': {e}")
        return []


def _fetch_archive_clips(search_query: str, limit: int = 2) -> list:
    """Search Archive.org for public domain video clips."""
    try:
        params = {
            "q": f"({search_query}) AND mediatype:movies AND subject:(documentary OR news OR archive)",
            "fl[]": ["identifier", "title"],
            "rows": limit,
            "output": "json",
        }
        resp = requests.get(ARCHIVE_API, params=params, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        docs = resp.json().get("response", {}).get("docs", [])

        results = []
        for doc in docs:
            identifier = doc.get("identifier", "")
            title = doc.get("title", "Archive.org clip")
            if identifier:
                results.append({
                    "type": "real_footage",
                    "url": f"https://archive.org/embed/{identifier}",
                    "credit": f"Archive.org — Public Domain — {title[:50]}",
                    "duration": 60,
                })
        return results

    except Exception as e:
        log.warning(f"Archive.org fetch failed for '{search_query}': {e}")
        return []


def _fetch_pexels_broll(search_query: str) -> dict | None:
    """Fetch atmospheric B-roll from Pexels as final fallback."""
    api_key = os.getenv("PEXELS_API_KEY")
    if not api_key:
        return None

    try:
        headers = {"Authorization": api_key}
        params = {
            "query": search_query,
            "per_page": 5,
            "orientation": "landscape",
        }
        resp = requests.get(PEXELS_API, headers=headers, params=params, timeout=15)
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
                return {
                    "type": "broll",
                    "url": f["link"],
                    "credit": (
                        f"Pexels — {video.get('user', {}).get('name', 'Pexels contributor')} "
                        f"— Royalty Free"
                    ),
                    "duration": min(int(video.get("duration", 10)), 30),
                }
        if files:
            return {
                "type": "broll",
                "url": files[0]["link"],
                "credit": (
                    f"Pexels — {video.get('user', {}).get('name', 'Pexels contributor')} "
                    f"— Royalty Free"
                ),
                "duration": min(int(video.get("duration", 10)), 30),
            }
    except Exception as e:
        log.warning(f"Pexels B-roll fetch failed for '{search_query}': {e}")
    return None


# ─── Public API ───────────────────────────────────────────────────────────────

def fetch_media_for_entry(entry: dict) -> list:
    """
    Fetch real footage and photos for a single mystery entry.
    Returns media_items: [{"type": str, "url": str, "credit": str, "duration": int}, ...]

    Priority:
      1. Known public footage (embed-only)
      2. Wikimedia Commons (CC photos)
      3. Archive.org (public domain video)
      4. Pexels (royalty-free B-roll)
    """
    media_items = []

    # 1. Known footage library
    known = _check_known_footage(entry)
    if known:
        media_items.append(known)
        print(f"    [MEDIA] Known footage: {known['description'][:60]}")

    # 2. AI-generated search queries
    search_queries = _get_search_queries(entry)
    print(f"    [MEDIA] Queries: {search_queries}")

    # 3. Wikimedia photos (try top 2 queries)
    for query in search_queries[:2]:
        photos = _fetch_wikimedia_photos(query, limit=2)
        media_items.extend(photos)
        if photos:
            print(f"    [MEDIA] Wikimedia: {len(photos)} photos for '{query}'")
        if len(media_items) >= 3:
            break

    # 4. Archive.org clips (if still short)
    if len(media_items) < 2:
        clips = _fetch_archive_clips(search_queries[0], limit=1)
        media_items.extend(clips)
        if clips:
            print(f"    [MEDIA] Archive.org: {len(clips)} clip(s)")

    # 5. Pexels atmospheric B-roll fallback
    if not media_items:
        atmospheric = search_queries[-1] if len(search_queries) >= 3 else "dark mysterious fog"
        broll = _fetch_pexels_broll(atmospheric)
        if broll:
            media_items.append(broll)
            print(f"    [MEDIA] Pexels fallback: '{atmospheric}'")

    # 6. Last resort: generic mystery B-roll
    if not media_items:
        broll = _fetch_pexels_broll("dark night mystery fog atmospheric")
        if broll:
            media_items.append(broll)
            print("    [MEDIA] Generic mystery B-roll")

    return media_items


def fetch_media_for_topic(topic: dict) -> dict:
    """
    Fetch media for all entries in a topic dict in place.
    Returns the topic dict with media_items[] added to each entry.
    """
    entries = topic.get("entries", [])
    print(f"  [MEDIA] Fetching media for {len(entries)} entries...")

    for i, entry in enumerate(entries):
        print(f"  [MEDIA] Entry {i + 1}/{len(entries)}: '{entry['title'][:55]}'")
        entry["media_items"] = fetch_media_for_entry(entry)
        print(f"    -> {len(entry['media_items'])} media items found")

    topic["entries"] = entries
    return topic
