"""
mystery_scraper.py — Scrapes mystery content from Reddit and Wikipedia.
Returns a structured topic dict with credibility-scored entries.

AI: OpenAI (GPT-4o-mini) for credibility scoring and metadata extraction.
Fallback: Anthropic (Claude) if OpenAI key is missing.
"""

import json
import logging
import os
import re
import time
from datetime import datetime

import requests

from modules.pipeline_logger import log_pipeline

log = logging.getLogger(__name__)

# ─── Category Configuration ───────────────────────────────────────────────────

CATEGORY_CONFIG = {
    "alien_sightings": {
        "subreddits": ["UFOs", "aliens"],
        "wikipedia_search": "UFO sighting unexplained aerial phenomenon",
        "title": "Top 5 Most Convincing Alien Sightings Ever Recorded",
    },
    "unsolved_disappearances": {
        "subreddits": ["UnresolvedMysteries"],
        "wikipedia_search": "missing persons unexplained disappearance unsolved",
        "title": "Top 5 Most Baffling Unsolved Disappearances",
    },
    "unexplained_photos": {
        "subreddits": ["Thetruthishere"],
        "wikipedia_search": "unexplained photograph paranormal anomaly",
        "title": "Top 5 Most Unsettling Unexplained Photos Ever Taken",
    },
    "mysterious_deaths": {
        "subreddits": ["UnresolvedMysteries"],
        "wikipedia_search": "unexplained mysterious death unsolved homicide",
        "title": "Top 5 Most Mysterious Deaths That Were Never Solved",
    },
    "strange_cold_cases": {
        "subreddits": ["UnresolvedMysteries"],
        "wikipedia_search": "unsolved cold case murder strange evidence",
        "title": "Top 5 Strangest Cold Cases That Defy Explanation",
    },
}

REDDIT_BASE = "https://www.reddit.com/r/{subreddit}/top.json"
WIKIPEDIA_SEARCH_API = "https://en.wikipedia.org/w/api.php"
WIKIPEDIA_SUMMARY_API = "https://en.wikipedia.org/api/rest_v1/page/summary/{title}"
HEADERS = {"User-Agent": "MysteryEngine/1.0 (+https://github.com/story-engine)"}


# ─── Reddit Fetching ──────────────────────────────────────────────────────────

def _fetch_reddit_entries(subreddit: str, limit: int = 25) -> list:
    """Fetch top posts from a mystery subreddit using public JSON API."""
    url = REDDIT_BASE.format(subreddit=subreddit)
    params = {"limit": limit, "t": "month", "raw_json": 1}
    try:
        resp = requests.get(url, headers=HEADERS, params=params, timeout=15)
        resp.raise_for_status()
        posts = resp.json().get("data", {}).get("children", [])

        entries = []
        for post in posts:
            p = post.get("data", {})
            if p.get("score", 0) < 50:
                continue
            if p.get("over_18", False):
                continue
            title = p.get("title", "").strip()
            body = (p.get("selftext", "") or "").strip()
            if len(title) < 10:
                continue

            summary = body[:500] if body else title
            entries.append({
                "title": title,
                "summary": summary,
                "source_url": f"https://reddit.com{p.get('permalink', '')}",
                "media_url": p.get("url", ""),
                "wikipedia_url": "",
                "credibility_score": 0.0,
                "source": f"r/{subreddit}",
            })
        return entries

    except Exception as e:
        log.warning(f"Reddit fetch failed for r/{subreddit}: {e}")
        return []


# ─── Wikipedia Fetching ───────────────────────────────────────────────────────

def _fetch_wikipedia_entries(search_query: str, limit: int = 10) -> list:
    """Search Wikipedia and return page summaries as candidate entries."""
    try:
        search_params = {
            "action": "query",
            "list": "search",
            "srsearch": search_query,
            "srlimit": limit,
            "format": "json",
            "utf8": 1,
        }
        resp = requests.get(
            WIKIPEDIA_SEARCH_API, params=search_params, headers=HEADERS, timeout=15
        )
        resp.raise_for_status()
        results = resp.json().get("query", {}).get("search", [])

        entries = []
        for result in results[:limit]:
            page_title = result.get("title", "")
            wiki_url = f"https://en.wikipedia.org/wiki/{page_title.replace(' ', '_')}"

            # Fetch full summary
            try:
                summary_resp = requests.get(
                    WIKIPEDIA_SUMMARY_API.format(title=page_title.replace(" ", "_")),
                    headers=HEADERS,
                    timeout=10,
                )
                if summary_resp.status_code == 200:
                    summary_data = summary_resp.json()
                    summary = summary_data.get("extract", "")[:600]
                    thumbnail = summary_data.get("thumbnail", {}).get("source", "")
                else:
                    summary = result.get("snippet", "")
                    thumbnail = ""
            except Exception:
                summary = result.get("snippet", "")
                thumbnail = ""

            # Skip stub articles
            if len(summary) < 50:
                continue

            entries.append({
                "title": page_title,
                "summary": summary,
                "source_url": wiki_url,
                "media_url": thumbnail,
                "wikipedia_url": wiki_url,
                "credibility_score": 0.0,
                "source": "wikipedia",
            })
        return entries

    except Exception as e:
        log.warning(f"Wikipedia fetch failed: {e}")
        return []


# ─── Credibility Scoring ──────────────────────────────────────────────────────

def _score_entries_with_openai(entries: list, category: str) -> list:
    """
    Use OpenAI (GPT-4o-mini) to score credibility of each mystery entry.
    Falls back to Anthropic if OpenAI key is missing.
    Returns entries with credibility_score set (1.0–10.0).
    """
    api_key = os.getenv("OPENAI_API_KEY")
    anthropic_key = os.getenv("ANTHROPIC_API_KEY")

    if not api_key and not anthropic_key:
        log.warning("No AI keys set — assigning default credibility scores.")
        for entry in entries:
            entry["credibility_score"] = 5.0
        return entries

    entries_text = "\n\n".join([
        f"Entry {i + 1}: {e['title']}\nSummary: {e['summary'][:200]}"
        for i, e in enumerate(entries)
    ])

    prompt = f"""You are evaluating mystery entries for a YouTube Top 5 countdown in category: {category}.

Score each entry 1-10 on:
- Credibility: documented evidence, witnesses, official records
- Mystery factor: genuinely unexplained, not definitively debunked
- Visual potential: footage or photos likely exist
- Audience appeal: would a general YouTube audience find this compelling

Entries to score:
{entries_text}

Return JSON only:
{{
  "scores": [
    {{"index": 1, "score": 8.5, "reason": "one sentence"}},
    {{"index": 2, "score": 7.0, "reason": "one sentence"}}
  ]
}}"""

    topic_id = f"{category}_scoring"

    # Try OpenAI first
    if api_key:
        try:
            from openai import OpenAI
            client = OpenAI(api_key=api_key)
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.3,
                max_tokens=800,
            )
            tokens_used = response.usage.total_tokens
            log_pipeline(topic_id, "credibility_scoring", "openai/gpt-4o-mini", tokens_used)

            raw = response.choices[0].message.content.strip()
            raw = re.sub(r"^```(?:json)?\s*", "", raw)
            raw = re.sub(r"\s*```$", "", raw)
            scores = json.loads(raw).get("scores", [])

            for s in scores:
                idx = s.get("index", 0) - 1
                if 0 <= idx < len(entries):
                    entries[idx]["credibility_score"] = float(s.get("score", 5.0))
                    entries[idx]["credibility_reason"] = s.get("reason", "")
            return entries

        except Exception as e:
            log.warning(f"OpenAI credibility scoring failed: {e} — trying Anthropic.")

    # Fallback: Anthropic
    if anthropic_key:
        try:
            import anthropic
            client = anthropic.Anthropic(api_key=anthropic_key)
            response = client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=800,
                messages=[{"role": "user", "content": prompt}],
            )
            tokens_used = response.usage.input_tokens + response.usage.output_tokens
            log_pipeline(topic_id, "credibility_scoring", "anthropic/claude-sonnet-4-6", tokens_used)

            raw = response.content[0].text.strip()
            raw = re.sub(r"^```(?:json)?\s*", "", raw)
            raw = re.sub(r"\s*```$", "", raw)
            scores = json.loads(raw).get("scores", [])

            for s in scores:
                idx = s.get("index", 0) - 1
                if 0 <= idx < len(entries):
                    entries[idx]["credibility_score"] = float(s.get("score", 5.0))
                    entries[idx]["credibility_reason"] = s.get("reason", "")
            return entries

        except Exception as e:
            log.warning(f"Anthropic credibility scoring also failed: {e} — using defaults.")

    # Hard fallback
    for entry in entries:
        if entry.get("credibility_score", 0) == 0.0:
            entry["credibility_score"] = 5.0
    return entries


# ─── Public API ───────────────────────────────────────────────────────────────

def get_mystery_topic(category: str = "alien_sightings") -> dict:
    """
    Fetches mystery entries from Reddit and Wikipedia for the given category.
    Scores entries with OpenAI (GPT-4o-mini) and returns top entries as a topic dict.

    Args:
        category: One of the CATEGORY_CONFIG keys.

    Returns:
        {
          "topic_id": str,
          "category": str,
          "title": str,
          "source_type": "mystery",
          "entries": [
            {
              "title": str,
              "summary": str,
              "source_url": str,
              "media_url": str,
              "wikipedia_url": str,
              "credibility_score": float,
              "source": str
            },
            ...
          ]
        }
    """
    cfg = CATEGORY_CONFIG.get(category, CATEGORY_CONFIG["alien_sightings"])
    print(f"  [MYSTERY] Category: {category}")

    all_entries = []

    # Pull from Reddit subreddits
    for subreddit in cfg["subreddits"]:
        print(f"  [REDDIT] Fetching r/{subreddit}...")
        reddit_entries = _fetch_reddit_entries(subreddit, limit=25)
        all_entries.extend(reddit_entries)
        print(f"    -> {len(reddit_entries)} entries")

    # Pull from Wikipedia
    print(f"  [WIKI] Searching: '{cfg['wikipedia_search']}'...")
    wiki_entries = _fetch_wikipedia_entries(cfg["wikipedia_search"], limit=10)
    all_entries.extend(wiki_entries)
    print(f"    -> {len(wiki_entries)} Wikipedia entries")

    if not all_entries:
        raise RuntimeError(f"No entries found for category '{category}' — check network.")

    # Deduplicate by normalized title
    seen = set()
    unique_entries = []
    for e in all_entries:
        key = e["title"].lower().strip()
        if key not in seen:
            seen.add(key)
            unique_entries.append(e)

    # Score with AI (cap at 20 to keep prompt size manageable)
    batch = unique_entries[:20]
    print(f"  [SCORE] Credibility scoring {len(batch)} entries with OpenAI...")
    scored = _score_entries_with_openai(batch, category)

    # Sort by credibility, keep top 10
    scored.sort(key=lambda x: x.get("credibility_score", 0), reverse=True)
    top_entries = scored[:10]

    topic_id = f"{category}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    topic = {
        "topic_id": topic_id,
        "category": category,
        "title": cfg["title"],
        "source_type": "mystery",
        "entries": top_entries,
    }

    print(f"  [OK] Topic: '{cfg['title']}' — {len(top_entries)} entries (best score: "
          f"{top_entries[0]['credibility_score']:.1f})")
    return topic
