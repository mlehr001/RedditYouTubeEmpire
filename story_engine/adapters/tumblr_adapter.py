"""
Tumblr adapter.
Uses the Tumblr API v2 to fetch tagged posts.
Requires: TUMBLR_API_KEY (consumer key — free, no review needed for read access).
Get one at: https://www.tumblr.com/oauth/apps

Story-rich tags: confession, true story, storytime, rant, relationship advice, etc.
"""

import logging
from datetime import datetime
from typing import Iterator, List, Optional

import requests
from bs4 import BeautifulSoup

from story_engine.adapters.base import BaseAdapter, RawStory
from story_engine.config.settings import cfg
from story_engine.utils.retry import retry

logger = logging.getLogger(__name__)

TUMBLR_API = "https://api.tumblr.com/v2"

# High-yield story tags for Tumblr
STORY_TAGS = [
    "confession",
    "true story",
    "storytime",
    "story time",
    "personal story",
    "my story",
    "rant",
    "personal",
    "real talk",
    "relationship",
    "toxic relationship",
    "cheating",
    "breakup story",
    "revenge",
    "family drama",
    "workplace drama",
    "fired",
    "coming out story",
    "mental health",
    "trauma",
    "recovery story",
    "heartbreak",
    "long post",
    "important",
    "tw vent",
]

MIN_BODY_WORDS = 150


class TumblrAdapter(BaseAdapter):
    """
    Fetches text posts from Tumblr using tagged search API.
    Tumblr is full of long-form personal confessions and story posts.
    No copyright issues — user-generated public posts.
    """

    source_name = "tumblr"

    def __init__(self):
        self.api_key = cfg.tumblr.api_key if hasattr(cfg, "tumblr") else ""
        if not self.api_key:
            import os
            self.api_key = os.environ.get("TUMBLR_API_KEY", "")
        if not self.api_key:
            logger.warning("[tumblr] TUMBLR_API_KEY not set — adapter will be skipped")
        self.tags = STORY_TAGS
        logger.info("TumblrAdapter initialized | tags=%d", len(self.tags))

    def fetch(self) -> Iterator[RawStory]:
        if not self.api_key:
            logger.warning("[tumblr] skipping — no API key configured")
            return

        for tag in self.tags:
            try:
                yield from self._fetch_tag(tag)
            except Exception as e:
                logger.warning("[tumblr] tag '%s' failed: %s", tag, e)
                continue

    @retry(max_attempts=3, delay=3.0, exceptions=(requests.RequestException,))
    def _fetch_tag(self, tag: str) -> Iterator[RawStory]:
        resp = requests.get(
            f"{TUMBLR_API}/tagged",
            params={
                "tag": tag,
                "api_key": self.api_key,
                "filter": "text",   # plain text, not HTML
                "limit": 20,
            },
            timeout=15,
        )

        if resp.status_code == 401:
            logger.error("[tumblr] invalid API key")
            return
        if resp.status_code == 429:
            logger.warning("[tumblr] rate limited on tag '%s'", tag)
            return

        resp.raise_for_status()
        data = resp.json()

        posts = data.get("response", [])
        if not posts:
            return

        fetched = 0
        for post in posts:
            try:
                story = self._post_to_raw_story(post, tag)
                if story and self.validate_story(story):
                    fetched += 1
                    yield story
            except Exception as e:
                logger.debug("[tumblr] error on post %s: %s", post.get("id"), e)

        logger.info("[tumblr] tag='%s' fetched=%d", tag, fetched)

    def _post_to_raw_story(self, post: dict, tag: str) -> Optional[RawStory]:
        post_type = post.get("type")
        post_id = str(post.get("id", ""))

        if not post_id:
            return None

        # Only text posts — skip photo/video/audio/link
        if post_type != "text":
            return None

        title = (post.get("title") or "").strip()
        body_html = post.get("body", "")

        if not body_html:
            return None

        # Strip HTML — verbatim text only
        body = self._strip_html(body_html)

        if not body or len(body.split()) < MIN_BODY_WORDS:
            return None

        # Build title from post title or first sentence
        if not title or len(title) < 5:
            first_line = body.split("\n")[0].strip()
            title = first_line[:200] if first_line else body[:100]

        # Get best tag as category hint
        post_tags = post.get("tags", [])

        url = post.get("post_url", f"https://tumblr.com/post/{post_id}")
        blog_name = post.get("blog_name", "")
        timestamp = post.get("timestamp", 0)

        return RawStory(
            external_id=post_id,
            url=url,
            title=title,
            body=body,
            source_name=self.source_name,
            author=blog_name or "tumblr_user",
            fetched_at=datetime.utcfromtimestamp(timestamp) if timestamp else datetime.utcnow(),
            raw_payload={
                "blog_name": blog_name,
                "tags": post_tags[:20],
                "note_count": post.get("note_count", 0),
                "fetched_tag": tag,
                "type": post_type,
            },
        )

    @staticmethod
    def _strip_html(html_text: str) -> str:
        """Strip HTML — preserve paragraph structure as newlines."""
        soup = BeautifulSoup(html_text, "html.parser")
        # Replace block elements with newlines
        for tag in soup.find_all(["p", "div", "br", "h1", "h2", "h3", "h4", "li"]):
            tag.insert_before("\n")
        text = soup.get_text(separator="", strip=False)
        # Normalize newlines
        import re
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()