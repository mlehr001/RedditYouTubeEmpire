"""
Threads (Meta) adapter.
Uses the Threads API v1.0 (released June 2024).
Requires: THREADS_ACCESS_TOKEN (long-lived user token from Meta Developer).

Get access:
1. Create a Meta App at developers.facebook.com
2. Add Threads API product
3. Generate long-lived access token
4. No app review needed for basic reading of public content

API docs: https://developers.facebook.com/docs/threads
"""

import logging
from datetime import datetime
from typing import Iterator, List, Optional

import requests

from story_engine.adapters.base import BaseAdapter, RawStory
from story_engine.utils.retry import retry

logger = logging.getLogger(__name__)

THREADS_API = "https://graph.threads.net/v1.0"

# Search keywords targeting story-type content on Threads
STORY_KEYWORDS = [
    "storytime",
    "story time",
    "confession",
    "true story",
    "this happened to me",
    "not making this up",
    "i cant believe",
    "relationship advice",
    "cheating",
    "divorce",
    "fired today",
    "quit my job",
    "my ex",
    "AITA",
    "am i wrong",
    "petty revenge",
    "workplace drama",
    "family drama",
    "rent free in my head",
    "i need to vent",
]


class ThreadsAdapter(BaseAdapter):
    """
    Fetches story-type posts from Threads (Meta) using the official API.
    Searches public posts by keywords — no following required.

    NOTE: Threads API keyword search requires the app to go through
    Meta's App Review for the threads_keyword_search permission.
    This adapter works immediately for your own posts and followers,
    full keyword search requires review approval (~1-2 weeks).
    """

    source_name = "threads"

    def __init__(self):
        import os
        self.access_token = os.environ.get("THREADS_ACCESS_TOKEN", "")
        if not self.access_token:
            logger.warning("[threads] THREADS_ACCESS_TOKEN not set — adapter will be skipped")
        logger.info("ThreadsAdapter initialized")

    def fetch(self) -> Iterator[RawStory]:
        if not self.access_token:
            logger.warning("[threads] skipping — no access token")
            return

        # Try keyword search for each story keyword
        for keyword in STORY_KEYWORDS[:10]:  # limit to 10 to conserve rate limit
            try:
                yield from self._search_keyword(keyword)
            except Exception as e:
                logger.warning("[threads] keyword '%s' failed: %s", keyword, e)
                continue

    @retry(max_attempts=3, delay=5.0, exceptions=(requests.RequestException,))
    def _search_keyword(self, keyword: str) -> Iterator[RawStory]:
        resp = requests.get(
            f"{THREADS_API}/threads",
            params={
                "q": keyword,
                "fields": "id,text,timestamp,username,media_type,permalink",
                "access_token": self.access_token,
                "limit": 25,
            },
            timeout=15,
        )

        if resp.status_code == 400:
            # Keyword search not yet approved — fall back to timeline
            logger.info("[threads] keyword search not approved — trying timeline")
            yield from self._fetch_timeline()
            return
        if resp.status_code in (401, 403):
            logger.error("[threads] auth failed: %d", resp.status_code)
            return

        resp.raise_for_status()
        posts = resp.json().get("data", [])

        fetched = 0
        for post in posts:
            story = self._post_to_raw_story(post)
            if story and self.validate_story(story):
                fetched += 1
                yield story

        logger.info("[threads] keyword='%s' fetched=%d", keyword, fetched)

    @retry(max_attempts=2, delay=3.0, exceptions=(requests.RequestException,))
    def _fetch_timeline(self) -> Iterator[RawStory]:
        """Fallback: fetch own timeline — works without app review."""
        resp = requests.get(
            f"{THREADS_API}/me/threads",
            params={
                "fields": "id,text,timestamp,username,media_type,permalink",
                "access_token": self.access_token,
                "limit": 50,
            },
            timeout=15,
        )
        resp.raise_for_status()
        posts = resp.json().get("data", [])

        for post in posts:
            story = self._post_to_raw_story(post)
            if story and self.validate_story(story):
                yield story

    def _post_to_raw_story(self, post: dict) -> Optional[RawStory]:
        post_id = post.get("id", "")
        text = (post.get("text") or "").strip()
        media_type = post.get("media_type", "")

        # Only text posts
        if media_type not in ("TEXT_POST", ""):
            return None

        if not text or len(text.split()) < 50:
            return None

        timestamp = post.get("timestamp", "")
        try:
            fetched_at = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S%z").replace(tzinfo=None)
        except Exception:
            fetched_at = datetime.utcnow()

        username = post.get("username", "threads_user")
        url = post.get("permalink", f"https://www.threads.net/post/{post_id}")

        # Build title from first line
        lines = [l.strip() for l in text.split("\n") if l.strip()]
        title = lines[0][:200] if lines else text[:100]

        return RawStory(
            external_id=post_id,
            url=url,
            title=title,
            body=text,
            source_name=self.source_name,
            author=username,
            fetched_at=fetched_at,
            raw_payload={
                "media_type": media_type,
                "username": username,
            },
        )