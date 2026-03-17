"""
Ingestion coordinator.
Runs all source adapters, deduplicates, saves raw stories, pushes to normalize queue.
Optionally uses TrendScanner to boost priority of trending topics.
"""

import json
import logging
from typing import List, Optional, Type

from story_engine.adapters.base import BaseAdapter, RawStory
from story_engine.adapters.fourchan_adapter import FourChanAdapter
from story_engine.adapters.hackernews_adapter import HackerNewsAdapter
from story_engine.adapters.mumsnet_adapter import MumsnetAdapter
from story_engine.adapters.newsapi_adapter import NewsAPIAdapter
from story_engine.adapters.reddit_adapter import RedditAdapter
from story_engine.adapters.threads_adapter import ThreadsAdapter
from story_engine.adapters.tumblr_adapter import TumblrAdapter
from story_engine.db.database import get_db
from story_engine.queue.job_queue import get_queue

logger = logging.getLogger(__name__)

# ── Registered adapters ───────────────────────────────────────────────────────
# All user-generated content sources — low legal risk
ADAPTERS: List[Type[BaseAdapter]] = [
    RedditAdapter,       # 65+ subreddits — top + hot posts daily
    FourChanAdapter,     # /r9k/ /adv/ /fit/ /biz/ /x/ — no API key needed
    TumblrAdapter,       # Tagged posts — requires TUMBLR_API_KEY
    MumsnetAdapter,      # AIBU + Relationships — UK's Reddit AITA
    HackerNewsAdapter,   # Ask HN + top stories — no API key needed
    ThreadsAdapter,      # Meta Threads — requires THREADS_ACCESS_TOKEN
    NewsAPIAdapter,      # Signal-only: used for trend keywords, not video content
]


class Ingestion:
    """
    Runs all registered adapters, deduplicates, persists raw stories,
    and pushes to the normalize queue.
    """

    def __init__(self):
        self.db = get_db()
        self.queue = get_queue()

    def run(
        self,
        adapters: Optional[List[Type[BaseAdapter]]] = None,
        use_trend_signals: bool = True,
    ) -> dict:
        """
        Run all (or specified) adapters.
        Returns full stats dict with per-adapter breakdown.
        """
        active_adapters = adapters or ADAPTERS
        stats = {
            "total": {"fetched": 0, "new": 0, "duplicate": 0, "error": 0},
            "by_adapter": {},
        }

        # Optionally scan trends to log hot keywords (informational only for now)
        if use_trend_signals:
            try:
                from story_engine.adapters.trend_signals import TrendScanner
                scanner = TrendScanner()
                hot_keywords = scanner.get_hot_keywords(top_n=10)
                logger.info("[ingestion] trending keywords: %s", hot_keywords[:10])
            except Exception as e:
                logger.debug("[ingestion] trend scan failed (non-fatal): %s", e)

        for AdapterClass in active_adapters:
            adapter_name = AdapterClass.__name__
            adapter_stats = {"fetched": 0, "new": 0, "duplicate": 0, "error": 0}

            try:
                adapter = AdapterClass()
                for raw_story in adapter.fetch():
                    adapter_stats["fetched"] += 1
                    result = self._save_raw_story(raw_story)
                    adapter_stats[result] = adapter_stats.get(result, 0) + 1
            except Exception as e:
                logger.exception("[ingestion] adapter %s crashed: %s", adapter_name, e)
                adapter_stats["error"] += 1

            stats["by_adapter"][adapter_name] = adapter_stats
            for key in ("fetched", "new", "duplicate", "error"):
                stats["total"][key] += adapter_stats[key]

            logger.info(
                "[ingestion] %-25s fetched=%-4d new=%-4d dup=%-4d err=%d",
                adapter_name,
                adapter_stats["fetched"],
                adapter_stats["new"],
                adapter_stats["duplicate"],
                adapter_stats["error"],
            )

        logger.info("[ingestion] ── TOTAL: %s", stats["total"])
        return stats

    def _save_raw_story(self, story: RawStory) -> str:
        """
        Persist a raw story. Deduplicates by (source_name, external_id).
        Returns: 'new' | 'duplicate' | 'error'
        """
        try:
            source_id = self._get_or_create_source(story.source_name)

            existing = self.db.execute_one(
                "SELECT id FROM raw_stories WHERE source_id=%s AND external_id=%s",
                (source_id, story.external_id),
            )
            if existing:
                return "duplicate"

            rows = self.db.execute(
                """
                INSERT INTO raw_stories
                    (source_id, external_id, url, title, body, author,
                     subreddit, upvotes, comment_count, fetched_at, raw_payload)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    source_id,
                    story.external_id,
                    story.url,
                    story.title,
                    story.body,
                    story.author,
                    story.subreddit,
                    story.upvotes,
                    story.comment_count,
                    story.fetched_at,
                    json.dumps(story.raw_payload),
                ),
            )
            raw_id = rows[0]["id"]
            self.queue.push("normalize", str(raw_id))
            logger.debug("[ingestion] saved %s (%s)", raw_id, story.external_id[:40])
            return "new"

        except Exception as e:
            logger.error("[ingestion] failed to save %s: %s", story.external_id[:40], e)
            return "error"

    def _get_or_create_source(self, source_name: str) -> str:
        row = self.db.execute_one("SELECT id FROM sources WHERE name=%s", (source_name,))
        if row:
            return row["id"]
        rows = self.db.execute(
            "INSERT INTO sources (name, adapter) VALUES (%s, %s) RETURNING id",
            (source_name, source_name.split("_")[0]),
        )
        return rows[0]["id"]