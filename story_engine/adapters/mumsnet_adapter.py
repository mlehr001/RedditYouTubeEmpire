"""
Mumsnet adapter.
Fetches AIBU (Am I Being Unreasonable) threads — UK's equivalent of Reddit AITA.
Uses Mumsnet's public RSS feed + thread scraper.
No API key required.

AIBU is massively popular for story content:
- Long detailed posts with full narrative context
- Strong community engagement (thousands of replies)
- Unique British perspective not found on US platforms
- Very high word count per post
"""

import logging
import re
from datetime import datetime
from typing import Iterator, List, Optional

import feedparser
import requests
from bs4 import BeautifulSoup

from story_engine.adapters.base import BaseAdapter, RawStory
from story_engine.utils.retry import retry

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

# Mumsnet talk sections with strong story content
MUMSNET_FEEDS = [
    {
        "name": "mumsnet_aibu",
        "rss": "https://www.mumsnet.com/Talk/am_i_being_unreasonable.rss",
        "label": "Am I Being Unreasonable",
    },
    {
        "name": "mumsnet_relationships",
        "rss": "https://www.mumsnet.com/Talk/relationships.rss",
        "label": "Relationships",
    },
    {
        "name": "mumsnet_chat",
        "rss": "https://www.mumsnet.com/Talk/chat.rss",
        "label": "Chat",
    },
    {
        "name": "mumsnet_employment",
        "rss": "https://www.mumsnet.com/Talk/employment_issues.rss",
        "label": "Employment Issues",
    },
]

MIN_BODY_WORDS = 150


class MumsnetAdapter(BaseAdapter):
    """
    Fetches story-rich threads from Mumsnet AIBU and Relationships sections.
    Scrapes full OP post text from thread page — verbatim only.
    """

    source_name = "mumsnet"

    def __init__(self, feeds: List[dict] = None):
        self.feeds = feeds or MUMSNET_FEEDS
        logger.info("MumsnetAdapter initialized | feeds=%d", len(self.feeds))

    def fetch(self) -> Iterator[RawStory]:
        for feed in self.feeds:
            try:
                yield from self._fetch_feed(feed)
            except Exception as e:
                logger.warning("[mumsnet] feed '%s' failed: %s", feed["name"], e)
                continue

    @retry(max_attempts=2, delay=5.0, exceptions=(Exception,))
    def _fetch_feed(self, feed_config: dict) -> Iterator[RawStory]:
        feed_name = feed_config["name"]
        rss_url = feed_config["rss"]

        logger.info("[mumsnet] fetching %s", feed_name)
        parsed = feedparser.parse(rss_url)

        if not parsed.entries:
            logger.warning("[mumsnet] no entries in %s", feed_name)
            return

        fetched = 0
        for entry in parsed.entries[:25]:
            try:
                story = self._entry_to_story(entry, feed_name)
                if story and self.validate_story(story):
                    fetched += 1
                    yield story
            except Exception as e:
                logger.debug("[mumsnet] entry error in %s: %s", feed_name, e)
                continue

        logger.info("[mumsnet] %s done | fetched=%d", feed_name, fetched)

    def _entry_to_story(self, entry, feed_name: str) -> Optional[RawStory]:
        url = getattr(entry, "link", "")
        title = getattr(entry, "title", "").strip()

        if not url or not title:
            return None

        # Skip mega-threads and stickies
        if any(x in title.lower() for x in ["megathread", "weekly thread", "monthly thread"]):
            return None

        external_id = getattr(entry, "id", url)

        # Try to get body from RSS summary first
        summary = getattr(entry, "summary", "")
        if summary:
            body = self._strip_html(summary)
            if len(body.split()) >= MIN_BODY_WORDS:
                return self._make_story(external_id, url, title, body, entry, feed_name)

        # Fallback: scrape the thread page for OP post
        body = self._scrape_op_post(url)
        if not body or len(body.split()) < MIN_BODY_WORDS:
            return None

        return self._make_story(external_id, url, title, body, entry, feed_name)

    @retry(max_attempts=2, delay=3.0, exceptions=(requests.RequestException,))
    def _scrape_op_post(self, url: str) -> str:
        """Scrape the original post (OP) from a Mumsnet thread page."""
        resp = requests.get(url, headers=HEADERS, timeout=12)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Mumsnet post containers
        selectors = [
            '[data-testid="message-text"]',
            ".talk-post-message",
            ".post-body",
            '[class*="PostBody"]',
            '[class*="message-text"]',
        ]

        for selector in selectors:
            posts = soup.select(selector)
            if posts:
                # Get the first post (OP)
                op_text = posts[0].get_text(separator=" ", strip=True)
                if len(op_text.split()) >= 50:
                    return op_text

        # Fallback: find first long paragraph block
        paragraphs = soup.find_all("p")
        body_parts = []
        for p in paragraphs:
            text = p.get_text(strip=True)
            if len(text.split()) > 15:
                body_parts.append(text)
            if sum(len(b.split()) for b in body_parts) > 200:
                break

        return " ".join(body_parts)

    def _make_story(self, external_id, url, title, body, entry, feed_name) -> RawStory:
        published = getattr(entry, "published_parsed", None)
        fetched_at = datetime(*published[:6]) if published else datetime.utcnow()
        author = getattr(entry, "author", "MumsnetUser")

        return RawStory(
            external_id=external_id,
            url=url,
            title=title,
            body=body,
            source_name=feed_name,
            author=author,
            fetched_at=fetched_at,
            raw_payload={
                "feed": feed_name,
                "summary_preview": body[:300],
            },
        )

    @staticmethod
    def _strip_html(html_text: str) -> str:
        soup = BeautifulSoup(html_text, "html.parser")
        return soup.get_text(separator=" ", strip=True)