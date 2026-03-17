"""
Central configuration — loaded from environment variables with sane defaults.
Use: from story_engine.config.settings import cfg
"""

import os
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class RedditConfig:
    client_id: str = field(default_factory=lambda: os.environ.get("REDDIT_CLIENT_ID", ""))
    client_secret: str = field(default_factory=lambda: os.environ.get("REDDIT_CLIENT_SECRET", ""))
    user_agent: str = field(default_factory=lambda: os.environ.get("REDDIT_USER_AGENT", "StoryEngine/1.0"))
    subreddits: list = field(default_factory=lambda: os.environ.get(
        "REDDIT_SUBREDDITS", ""
    ).split(",") if os.environ.get("REDDIT_SUBREDDITS") else [])
    # Empty list = use RedditAdapter.DEFAULT_SUBREDDITS (65+ subreddits)
    post_limit: int = field(default_factory=lambda: int(os.environ.get("REDDIT_POST_LIMIT", "50")))
    min_upvotes: int = field(default_factory=lambda: int(os.environ.get("REDDIT_MIN_UPVOTES", "100")))
    min_word_count: int = field(default_factory=lambda: int(os.environ.get("REDDIT_MIN_WORD_COUNT", "200")))


@dataclass
class NewsAPIConfig:
    api_key: str = field(default_factory=lambda: os.environ.get("NEWSAPI_KEY", ""))
    sources: list = field(default_factory=lambda: os.environ.get(
        "NEWSAPI_SOURCES", "bbc-news,cnn,the-guardian-uk"
    ).split(","))
    page_size: int = field(default_factory=lambda: int(os.environ.get("NEWSAPI_PAGE_SIZE", "20")))


@dataclass
class AIConfig:
    provider: str = field(default_factory=lambda: os.environ.get("AI_PROVIDER", "anthropic"))
    anthropic_key: str = field(default_factory=lambda: os.environ.get("ANTHROPIC_API_KEY", ""))
    openai_key: str = field(default_factory=lambda: os.environ.get("OPENAI_API_KEY", ""))
    model_anthropic: str = field(default_factory=lambda: os.environ.get("AI_MODEL_ANTHROPIC", "claude-opus-4-6"))
    model_openai: str = field(default_factory=lambda: os.environ.get("AI_MODEL_OPENAI", "gpt-4o"))
    max_retries: int = field(default_factory=lambda: int(os.environ.get("AI_MAX_RETRIES", "3")))
    retry_delay: float = field(default_factory=lambda: float(os.environ.get("AI_RETRY_DELAY", "2.0")))
    prompt_version: str = "v1.0"


@dataclass
class ScoringConfig:
    min_score: float = field(default_factory=lambda: float(os.environ.get("MIN_SCORE", "7.0")))
    auto_reject_below: float = field(default_factory=lambda: float(os.environ.get("AUTO_REJECT_SCORE", "7.0")))


@dataclass
class VideoConfig:
    output_dir: str = field(default_factory=lambda: os.environ.get("VIDEO_OUTPUT_DIR", "/data/videos"))
    assets_dir: str = field(default_factory=lambda: os.environ.get("ASSETS_DIR", "/app/assets"))
    short_max_words: int = field(default_factory=lambda: int(os.environ.get("SHORT_MAX_WORDS", "300")))
    long_min_words: int = field(default_factory=lambda: int(os.environ.get("LONG_MIN_WORDS", "301")))
    resolution_short: str = "1080x1920"   # 9:16 portrait for TikTok/Shorts/Reels
    resolution_long: str = "1920x1080"    # 16:9 landscape for YouTube
    font_path: str = field(default_factory=lambda: os.environ.get("FONT_PATH", "/app/assets/fonts/Roboto-Bold.ttf"))
    tts_voice: str = field(default_factory=lambda: os.environ.get("TTS_VOICE", "alloy"))


@dataclass
class QueueConfig:
    redis_url: str = field(default_factory=lambda: os.environ.get("REDIS_URL", "redis://localhost:6379/0"))
    job_timeout: int = field(default_factory=lambda: int(os.environ.get("JOB_TIMEOUT", "300")))
    max_retries: int = field(default_factory=lambda: int(os.environ.get("QUEUE_MAX_RETRIES", "3")))
    retry_ttl: int = field(default_factory=lambda: int(os.environ.get("QUEUE_RETRY_TTL", "3600")))


@dataclass
class TumblrConfig:
    api_key: str = field(default_factory=lambda: os.environ.get("TUMBLR_API_KEY", ""))


@dataclass
class FourChanConfig:
    # No auth needed — config controls which boards to fetch
    boards: list = field(default_factory=lambda: os.environ.get(
        "FOURCHAN_BOARDS", "r9k,adv,fit,biz,x,tv"
    ).split(","))
    max_threads_per_board: int = field(
        default_factory=lambda: int(os.environ.get("FOURCHAN_MAX_THREADS", "25"))
    )


@dataclass
class PublishConfig:
    youtube_client_secret: str = field(default_factory=lambda: os.environ.get("YOUTUBE_CLIENT_SECRET_PATH", ""))
    tiktok_access_token: str = field(default_factory=lambda: os.environ.get("TIKTOK_ACCESS_TOKEN", ""))
    instagram_access_token: str = field(default_factory=lambda: os.environ.get("INSTAGRAM_ACCESS_TOKEN", ""))
    enabled_platforms: list = field(default_factory=lambda: os.environ.get(
        "ENABLED_PLATFORMS", "youtube"
    ).split(","))


@dataclass
class AppConfig:
    env: str = field(default_factory=lambda: os.environ.get("APP_ENV", "development"))
    log_level: str = field(default_factory=lambda: os.environ.get("LOG_LEVEL", "INFO"))
    database_url: str = field(default_factory=lambda: os.environ.get("DATABASE_URL", ""))
    workers: int = field(default_factory=lambda: int(os.environ.get("WORKERS", "4")))

    reddit: RedditConfig = field(default_factory=RedditConfig)
    newsapi: NewsAPIConfig = field(default_factory=NewsAPIConfig)
    tumblr: TumblrConfig = field(default_factory=TumblrConfig)
    fourchan: FourChanConfig = field(default_factory=FourChanConfig)
    ai: AIConfig = field(default_factory=AIConfig)
    scoring: ScoringConfig = field(default_factory=ScoringConfig)
    video: VideoConfig = field(default_factory=VideoConfig)
    queue: QueueConfig = field(default_factory=QueueConfig)
    publish: PublishConfig = field(default_factory=PublishConfig)

    def validate(self):
        errors = []
        if not self.database_url:
            errors.append("DATABASE_URL is required")
        if self.ai.provider == "anthropic" and not self.ai.anthropic_key:
            errors.append("ANTHROPIC_API_KEY is required when AI_PROVIDER=anthropic")
        if self.ai.provider == "openai" and not self.ai.openai_key:
            errors.append("OPENAI_API_KEY is required when AI_PROVIDER=openai")
        if errors:
            raise ValueError("Config validation failed:\n" + "\n".join(f"  - {e}" for e in errors))


# Singleton
cfg = AppConfig()
