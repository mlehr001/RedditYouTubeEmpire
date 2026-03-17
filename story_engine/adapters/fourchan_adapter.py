"""
4chan adapter.
Uses the public 4chan JSON API — no API key, no auth, fully anonymous.
Best boards for story content: r9k, adv, fit, biz, x.

API docs: https://github.com/4chan/4chan-API
Endpoints:
  GET https://a.4cdn.org/{board}/threads.json   — list all active threads
  GET https://a.4cdn.org/{board}/thread/{no}.json — full thread with replies
"""

import html
import logging
import re
import time
from datetime import datetime
from typing import Iterator, List, Optional

import requests

from story_engine.adapters.base import BaseAdapter, RawStory
from story_engine.utils.retry import retry

logger = logging.getLogger(__name__)

BASE_URL = "https://a.4cdn.org"
HEADERS = {"User-Agent": "StoryEngine/1.0"}

# ── Board config ──────────────────────────────────────────────────────────────
BOARDS = [
    {
        "board": "r9k",
        "label": "4chan_r9k",
        "description": "Personal stories, relationships, loneliness, life events",
        "max_threads": 30,
    },
    {
        "board": "adv",
        "label": "4chan_adv",
        "description": "Relationship and life advice — long story-driven posts",
        "max_threads": 30,
    },
    {
        "board": "fit",
        "label": "4chan_fit",
        "description": "Fitness transformations, health journeys, personal wins",
        "max_threads": 20,
    },
    {
        "board": "biz",
        "label": "4chan_biz",
        "description": "Money stories, career wins/losses, crypto drama",
        "max_threads": 20,
    },
    {
        "board": "x",
        "label": "4chan_x",
        "description": "Paranormal, creepy encounters, unexplained events",
        "max_threads": 20,
    },
    {
        "board": "tv",
        "label": "4chan_tv",
        "description": "Pop culture, viral moments, entertainment drama",
        "max_threads": 15,
    },
]

# Keywords that signal a genuine personal story (OP post)
STORY_SIGNALS = [
    "greentext", "story time", "storytime", "true story",
    "happened to me", "my ex", "my girlfriend", "my boyfriend",
    "my wife", "my husband", "my mom", "my dad", "my boss",
    "i was", "i am", "i have", "i got", "i quit", "i lost",
    "i found", "i met", "i fucked up", "fucked up",
    "confession", "vent", "need advice", "advice needed",
    "what do i do", "am i wrong", "help me", "feel",
    "relationship", "cheating", "revenge", "fired", "job",
    "money", "debt", "family", "friend", "coworker",
]


class FourChanAdapter(BaseAdapter):
    """
    Fetches story-rich threads from 4chan using the public JSON API.
    Builds story body from OP post + top replies.
    Anonymous posts = no copyright claimants.
    NOTE: Content moderation burden is high — censor stage handles it.
    """

    source_name = "4chan"

    def __init__(self, boards: List[dict] = None):
        self.boards = boards or BOARDS
        logger.info("FourChanAdapter initialized | boards=%d", len(self.boards))

    def fetch(self) -> Iterator[RawStory]:
        for board_cfg in self.boards:
            try:
                yield from self._fetch_board(board_cfg)
                time.sleep(1)  # 4chan rate limit: be polite
            except Exception as e:
                logger.warning("[4chan] board /%s/ failed: %s", board_cfg["board"], e)
                continue

    @retry(max_attempts=3, delay=5.0, exceptions=(requests.RequestException,))
    def _fetch_board(self, board_cfg: dict) -> Iterator[RawStory]:
        board = board_cfg["board"]
        label = board_cfg["label"]
        max_threads = board_cfg["max_threads"]

        logger.info("[4chan] fetching /%s/", board)

        # Get thread catalog
        resp = requests.get(
            f"{BASE_URL}/{board}/threads.json",
            headers=HEADERS,
            timeout=10,
        )
        resp.raise_for_status()

        # Flatten pages → thread list, sorted by reply count (most active first)
        all_threads = []
        for page in resp.json():
            for thread in page.get("threads", []):
                all_threads.append(thread)

        # Sort by replies descending — more replies = more engaging story
        all_threads.sort(key=lambda t: t.get("replies", 0), reverse=True)
        all_threads = all_threads[:max_threads]

        logger.info("[4chan] /%s/ checking %d threads", board, len(all_threads))
        fetched = 0

        for thread_meta in all_threads:
            thread_no = thread_meta.get("no")
            if not thread_no:
                continue
            try:
                story = self._fetch_thread(board, label, thread_no)
                if story and self.validate_story(story):
                    fetched += 1
                    yield story
                time.sleep(0.5)  # polite rate limiting
            except Exception as e:
                logger.debug("[4chan] /%s/ thread %d error: %s", board, thread_no, e)
                continue

        logger.info("[4chan] /%s/ done | fetched=%d", board, fetched)

    @retry(max_attempts=2, delay=3.0, exceptions=(requests.RequestException,))
    def _fetch_thread(self, board: str, label: str, thread_no: int) -> Optional[RawStory]:
        resp = requests.get(
            f"{BASE_URL}/{board}/thread/{thread_no}.json",
            headers=HEADERS,
            timeout=10,
        )
        if resp.status_code == 404:
            return None  # Thread deleted — normal on 4chan
        resp.raise_for_status()

        posts = resp.json().get("posts", [])
        if not posts:
            return None

        op = posts[0]

        # Skip stickied/mod posts
        if op.get("sticky") or op.get("closed"):
            return None

        op_text = self._clean_post(op.get("com", ""))
        op_subject = self._clean_post(op.get("sub", "")).strip()

        if not op_text or len(op_text.split()) < 50:
            return None

        # Must have story signals in OP
        combined = (op_subject + " " + op_text).lower()
        if not any(signal in combined for signal in STORY_SIGNALS):
            return None

        # Build title from subject or first sentence of OP
        if op_subject and len(op_subject) > 10:
            title = op_subject[:200]
        else:
            first_sent = re.split(r'(?<=[.!?])\s', op_text)[0]
            title = first_sent[:200] if first_sent else op_text[:100]

        # Build body: OP + top replies (verbatim, labeled by post number)
        body_parts = [op_text]
        reply_count = 0
        for post in posts[1:min(len(posts), 20)]:  # top 19 replies max
            post_text = self._clean_post(post.get("com", ""))
            if not post_text or len(post_text.split()) < 10:
                continue
            # Skip greentext-only replies (purely meme responses)
            if post_text.startswith(">") and len(post_text.split("\n")) <= 2:
                continue
            body_parts.append(post_text)
            reply_count += 1
            if reply_count >= 10:
                break

        body = "\n\n".join(body_parts)

        return RawStory(
            external_id=f"{board}_{thread_no}",
            url=f"https://boards.4channel.org/{board}/thread/{thread_no}",
            title=title,
            body=body,
            source_name=label,
            author="Anonymous",
            comment_count=op.get("replies", 0),
            fetched_at=datetime.utcfromtimestamp(op.get("time", 0)),
            raw_payload={
                "board": board,
                "thread_no": thread_no,
                "op_id": op.get("no"),
                "replies": op.get("replies", 0),
                "images": op.get("images", 0),
                "unique_ips": op.get("unique_ips", 0),
            },
        )

    @staticmethod
    def _clean_post(html_text: str) -> str:
        """
        Clean 4chan HTML post text to plain text.
        Preserves verbatim content — only strips HTML markup.
        """
        if not html_text:
            return ""

        # Replace <br> with newline
        text = re.sub(r"<br\s*/?>", "\n", html_text, flags=re.IGNORECASE)

        # Replace greentext spans (keep the > prefix, strip span)
        text = re.sub(r'<span class="quote">', "", text)

        # Strip quote links (>>12345)
        text = re.sub(r'<a[^>]*>&gt;&gt;\d+</a>', "", text)

        # Strip all remaining HTML tags
        text = re.sub(r"<[^>]+>", "", text)

        # Decode HTML entities
        text = html.unescape(text)

        # Normalize whitespace
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = text.strip()

        return text