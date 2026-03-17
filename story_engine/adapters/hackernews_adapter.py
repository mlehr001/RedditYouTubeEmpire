"""
Hacker News adapter.
Fetches "Ask HN" stories and top personal stories from the HN API.
No API key required — fully public.
Verbatim content only — no modifications.
"""

import logging
from datetime import datetime
from typing import Iterator, List, Optional

import requests

from story_engine.adapters.base import BaseAdapter, RawStory
from story_engine.utils.retry import retry

logger = logging.getLogger(__name__)

HN_API = "https://hacker-news.firebaseio.com/v0"
HEADERS = {"User-Agent": "StoryEngine/1.0"}

# Ask HN and story-type keywords worth ingesting
STORY_KEYWORDS = [
    "ask hn",
    "i quit",
    "i was fired",
    "i got fired",
    "i left",
    "my story",
    "confession",
    "i built",
    "show hn",
    "lessons learned",
    "what i learned",
    "failed",
    "scammed",
    "divorced",
    "burnout",
    "left my job",
    "moved abroad",
    "quit my job",
    "changed my life",
]


class HackerNewsAdapter(BaseAdapter):
    """
    Fetches top + Ask HN stories from Hacker News Firebase API.
    Filters for narrative/personal story content using keyword matching.
    No API key required.
    """

    source_name = "hackernews"
    MIN_SCORE = 50       # HN points threshold
    MIN_COMMENTS = 10    # Stories with discussion tend to be more engaging
    MAX_STORIES = 200    # How many top stories to check

    def fetch(self) -> Iterator[RawStory]:
        logger.info("[hn] fetching Hacker News stories")

        # Fetch from multiple feeds
        for feed in ["topstories", "askstories", "showstories"]:
            try:
                yield from self._fetch_feed(feed)
            except Exception as e:
                logger.warning("[hn] feed '%s' failed: %s", feed, e)

    @retry(max_attempts=3, delay=3.0, exceptions=(requests.RequestException,))
    def _fetch_feed(self, feed_name: str) -> Iterator[RawStory]:
        resp = requests.get(f"{HN_API}/{feed_name}.json", headers=HEADERS, timeout=10)
        resp.raise_for_status()
        story_ids: List[int] = resp.json()[:self.MAX_STORIES]

        logger.info("[hn] feed=%s ids=%d", feed_name, len(story_ids))
        fetched = 0
        skipped = 0

        for story_id in story_ids:
            try:
                story = self._fetch_item(story_id, feed_name)
                if story is None:
                    skipped += 1
                    continue
                if not self.validate_story(story):
                    skipped += 1
                    continue
                fetched += 1
                yield story
            except Exception as e:
                logger.debug("[hn] error on item %d: %s", story_id, e)
                skipped += 1

        logger.info("[hn] feed=%s done | fetched=%d skipped=%d", feed_name, fetched, skipped)

    @retry(max_attempts=2, delay=1.0, exceptions=(requests.RequestException,))
    def _fetch_item(self, item_id: int, feed_name: str) -> Optional[RawStory]:
        resp = requests.get(f"{HN_API}/item/{item_id}.json", headers=HEADERS, timeout=8)
        resp.raise_for_status()
        item = resp.json()

        if not item or item.get("deleted") or item.get("dead"):
            return None
        if item.get("type") not in ("story", "ask"):
            return None

        score = item.get("score", 0)
        if score < self.MIN_SCORE:
            return None

        title = (item.get("title") or "").strip()
        url = item.get("url") or f"https://news.ycombinator.com/item?id={item_id}"
        text = (item.get("text") or "").strip()

        # For Ask HN, use the post text as body
        # For link stories, use title + fetch top comments as narrative
        if item.get("type") == "ask" and text:
            body = self._strip_html(text)
        elif item.get("type") == "story" and text:
            body = self._strip_html(text)
        else:
            # No body text — try to build from top comments
            body = self._build_body_from_comments(item, title)

        if not body or len(body.split()) < 80:
            return None

        # Filter: must be story-like content
        if not self._is_story_content(title, body):
            return None

        return RawStory(
            external_id=str(item_id),
            url=url,
            title=title,
            body=body,
            source_name=self.source_name,
            author=item.get("by"),
            upvotes=score,
            comment_count=item.get("descendants", 0),
            fetched_at=datetime.utcfromtimestamp(item.get("time", 0)),
            raw_payload={
                "id": item_id,
                "score": score,
                "type": item.get("type"),
                "feed": feed_name,
                "kids": (item.get("kids") or [])[:5],
            },
        )

    def _build_body_from_comments(self, item: dict, title: str) -> str:
        """
        For link-type stories with no text body, build narrative from top comments.
        This gives us the community's reaction/experience — still verbatim.
        """
        kids = (item.get("kids") or [])[:10]
        if not kids:
            return ""

        comments = []
        for kid_id in kids:
            try:
                resp = requests.get(
                    f"{HN_API}/item/{kid_id}.json", headers=HEADERS, timeout=5
                )
                kid = resp.json()
                if not kid or kid.get("deleted") or kid.get("dead"):
                    continue
                text = self._strip_html(kid.get("text") or "")
                if text and len(text.split()) > 20:
                    comments.append(text)
            except Exception:
                continue

        if len(comments) < 3:
            return ""

        return "\n\n".join(comments)

    def _is_story_content(self, title: str, body: str) -> bool:
        """Filter for narrative/personal story content."""
        title_lower = title.lower()
        body_lower = body.lower()[:500]
        combined = title_lower + " " + body_lower

        return any(keyword in combined for keyword in STORY_KEYWORDS)

    @staticmethod
    def _strip_html(html: str) -> str:
        """Strip HTML tags from HN post text."""
        import re
        # Replace <p> with newlines
        text = re.sub(r"<p>", "\n\n", html, flags=re.IGNORECASE)
        # Replace <br> with newline
        text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
        # Strip remaining tags
        text = re.sub(r"<[^>]+>", "", text)
        # Decode HTML entities
        import html as html_module
        text = html_module.unescape(text)
        return text.strip()