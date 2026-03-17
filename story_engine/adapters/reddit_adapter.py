"""
Reddit source adapter using PRAW.
Fetches top posts from configured subreddits.
Verbatim content only — no modifications.
"""

import logging
from typing import Iterator

import praw
from praw.exceptions import PRAWException

from story_engine.adapters.base import BaseAdapter, RawStory
from story_engine.config.settings import cfg
from story_engine.utils.retry import retry

logger = logging.getLogger(__name__)


class RedditAdapter(BaseAdapter):
    """
    Fetches stories from Reddit using the official PRAW library.
    Targets self-posts (text posts) only — no link posts.
    Respects min_upvotes and min_word_count thresholds from config.
    """

    source_name = "reddit"

    # ── High-volume narrative story subreddits ────────────────────────────────
    DEFAULT_SUBREDDITS = [
        # ── Relationship / Family drama ────────────────────────────────────────
        "AmItheAsshole",
        "AITAH",
        "relationship_advice",
        "survivinginfidelity",
        "TrueOffMyChest",
        "offmychest",
        "confession",
        "BestofRedditorUpdates",
        "BestOfAITA",
        "AITA_WIBTA_PUBLIC",
        "Marriage",
        "Divorce",
        "DeadBedrooms",
        "adultery",
        "cheating_stories",
        "tifu",

        # ── Family / In-laws ───────────────────────────────────────────────────
        "raisedbynarcissists",
        "JustNoMIL",
        "JustNoFamily",
        "entitledparents",
        "raisedbyborderlines",
        "EstrangedAdultChildren",
        "narcissisticparents",

        # ── Work / Money ───────────────────────────────────────────────────────
        "antiwork",
        "WorkReform",
        "TalesFromRetail",
        "TalesFromTechSupport",
        "talesfromcallcenters",
        "IDontWorkHereLady",
        "personalfinance",
        "povertyfinance",
        "legaladvice",
        "Scams",

        # ── Revenge ───────────────────────────────────────────────────────────
        "pettyrevenge",
        "ProRevenge",
        "NuclearRevenge",
        "maliciouscompliance",
        "RevengeStories",
        "ChoosingBeggars",
        "entitledpeople",
        "weddingshaming",

        # ── Crime / True crime ────────────────────────────────────────────────
        "TrueCrime",
        "UnresolvedMysteries",
        "RBI",
        "LetsNotMeet",
        "creepyencounters",

        # ── Emotional / Uplifting ──────────────────────────────────────────────
        "MadeMeSmile",
        "HumansBeingBros",
        "wholesome",
        "GoodNews",
        "UpliftingNews",
        "inspirational",
        "AskReddit",           # fetch top voted answers to emotional prompts

        # ── Weird / Viral ──────────────────────────────────────────────────────
        "nottheonion",
        "Unexpected",
        "WTF",
        "mildlyinfuriating",
        "facepalm",
        "insanepeoplefacebook",
        "trashy",
        "iamatotalpieceofshit",
        "PublicFreakout",

        # ── Life stories ──────────────────────────────────────────────────────
        "self",
        "CasualConversation",
        "Showerthoughts",
        "LifeAdvice",
        "Advice",
        "needadvice",

        # ── Horror / Nosleep ──────────────────────────────────────────────────
        "nosleep",
        "Glitch_in_the_Matrix",
        "Paranormal",
        "Thetruthishere",
        "Ghosts",
    ]

    def __init__(self):
        self.reddit = praw.Reddit(
            client_id=cfg.reddit.client_id,
            client_secret=cfg.reddit.client_secret,
            user_agent=cfg.reddit.user_agent,
            read_only=True,
        )
        self.subreddits = cfg.reddit.subreddits or self.DEFAULT_SUBREDDITS
        self.post_limit = cfg.reddit.post_limit
        self.min_upvotes = cfg.reddit.min_upvotes
        self.min_word_count = cfg.reddit.min_word_count
        logger.info(
            "RedditAdapter initialized | subreddits=%d | limit=%d | min_upvotes=%d",
            len(self.subreddits), self.post_limit, self.min_upvotes
        )

    def fetch(self) -> Iterator[RawStory]:
        for sub_name in self.subreddits:
            try:
                yield from self._fetch_subreddit(sub_name)
            except Exception as e:
                logger.error("[reddit] failed on r/%s: %s", sub_name, e)
                continue

    @retry(max_attempts=3, delay=5.0, exceptions=(PRAWException, Exception))
    def _fetch_subreddit(self, sub_name: str) -> Iterator[RawStory]:
        logger.info("[reddit] fetching r/%s (limit=%d)", sub_name, self.post_limit)
        subreddit = self.reddit.subreddit(sub_name)
        fetched = 0
        skipped = 0

        # Fetch both top (day) and hot — maximizes story variety
        post_sources = [
            subreddit.top(time_filter="day", limit=self.post_limit),
            subreddit.hot(limit=max(10, self.post_limit // 2)),
        ]

        seen_ids = set()
        for post_source in post_sources:
            for post in post_source:
                if post.id in seen_ids:
                    continue
                seen_ids.add(post.id)
                try:
                    story = self._post_to_raw_story(post, sub_name)
                    if story is None:
                        skipped += 1
                        continue
                    if not self.validate_story(story):
                        skipped += 1
                        continue
                    fetched += 1
                    yield story
                except Exception as e:
                    logger.warning("[reddit] error processing post %s: %s",
                                   getattr(post, "id", "?"), e)
                    continue

        logger.info("[reddit] r/%s done | fetched=%d skipped=%d", sub_name, fetched, skipped)

    def _post_to_raw_story(self, post, sub_name: str) -> RawStory | None:
        if post.is_self is False:
            return None
        if not post.selftext or post.selftext in ("[deleted]", "[removed]", ""):
            return None
        if post.score < self.min_upvotes:
            return None
        if getattr(post, "over_18", False):
            # Skip NSFW — censor stage would catch it but better to not ingest
            return None

        body = post.selftext.strip()
        title = post.title.strip()

        story = RawStory(
            external_id=post.id,
            url=f"https://reddit.com{post.permalink}",
            title=title,
            body=body,
            source_name=self.source_name,
            author=str(post.author) if post.author else "[deleted]",
            subreddit=sub_name,
            upvotes=post.score,
            comment_count=post.num_comments,
            raw_payload={
                "id": post.id,
                "score": post.score,
                "upvote_ratio": post.upvote_ratio,
                "num_comments": post.num_comments,
                "created_utc": post.created_utc,
                "flair": post.link_flair_text,
                "over_18": post.over_18,
                "awards": post.total_awards_received,
                "distinguished": post.distinguished,
            },
        )

        if not story.is_long_enough(self.min_word_count):
            logger.debug("[reddit] post %s too short (%d words)", post.id, story.word_count())
            return None

        return story

    def fetch_by_id(self, post_id: str) -> RawStory | None:
        """Fetch a single post by ID — for re-ingestion or debugging."""
        try:
            post = self.reddit.submission(id=post_id)
            return self._post_to_raw_story(post, post.subreddit.display_name)
        except Exception as e:
            logger.error("[reddit] could not fetch post %s: %s", post_id, e)
            return None

    def fetch_subreddit_top_week(self, sub_name: str) -> Iterator[RawStory]:
        """Fetch weekly top posts for a subreddit — for backfill runs."""
        subreddit = self.reddit.subreddit(sub_name)
        for post in subreddit.top(time_filter="week", limit=self.post_limit * 2):
            story = self._post_to_raw_story(post, sub_name)
            if story and self.validate_story(story):
                yield story