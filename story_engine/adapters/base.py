"""
Base adapter interface. All source adapters implement this.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterator, Optional


@dataclass
class RawStory:
    """
    Verbatim story as fetched from source. NOTHING is modified here.
    This maps directly to the raw_stories DB table.
    """
    external_id: str
    url: str
    title: str
    body: str
    source_name: str          # matches sources.name in DB
    author: Optional[str] = None
    subreddit: Optional[str] = None
    upvotes: Optional[int] = None
    comment_count: Optional[int] = None
    fetched_at: datetime = field(default_factory=datetime.utcnow)
    raw_payload: dict = field(default_factory=dict)

    def word_count(self) -> int:
        return len(self.body.split())

    def is_long_enough(self, min_words: int) -> bool:
        return self.word_count() >= min_words


class BaseAdapter(ABC):
    """
    All adapters must implement fetch().
    fetch() is a generator — yields RawStory one at a time.
    Adapters must NOT modify story content.
    """

    source_name: str = ""

    @abstractmethod
    def fetch(self) -> Iterator[RawStory]:
        """Yield RawStory objects from the source."""

    def validate_story(self, story: RawStory) -> bool:
        """Basic sanity check — subclasses can override to add source-specific rules."""
        if not story.external_id:
            return False
        if not story.title or not story.body:
            return False
        if len(story.body.strip()) < 50:
            return False
        return True
