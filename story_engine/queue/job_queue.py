"""
Redis-backed job queue for pipeline stage handoffs.
Each pipeline stage pushes completed story IDs to the next stage's queue.
Workers pop from their queue, process, push to next.

Queue names follow: pipeline:{stage}
Dead-letter queue: pipeline:dead
"""

import json
import logging
import time
from typing import Any, Optional
from uuid import UUID

import redis

from story_engine.config.settings import cfg

logger = logging.getLogger(__name__)


class JobQueue:
    """Thread-safe Redis job queue. Supports push, pop, retry, and DLQ."""

    DEAD_LETTER = "pipeline:dead"
    KEY_PREFIX = "pipeline:"

    def __init__(self, redis_url: Optional[str] = None):
        url = redis_url or cfg.queue.redis_url
        self._r = redis.from_url(url, decode_responses=True)
        logger.info("JobQueue connected to Redis: %s", url.split("@")[-1])

    def push(self, stage: str, story_id: str, meta: dict = None) -> None:
        """Push a job to a stage queue."""
        payload = json.dumps({
            "story_id": str(story_id),
            "stage": stage,
            "enqueued_at": time.time(),
            "attempt": 1,
            "meta": meta or {},
        })
        key = self.KEY_PREFIX + stage
        self._r.rpush(key, payload)
        logger.debug("[queue] pushed %s → %s", story_id, stage)

    def pop(self, stage: str, timeout: int = 5) -> Optional[dict]:
        """
        Blocking pop from a stage queue. Returns parsed job dict or None.
        Uses BLPOP so workers sleep efficiently instead of polling.
        """
        key = self.KEY_PREFIX + stage
        result = self._r.blpop(key, timeout=timeout)
        if result is None:
            return None
        _, payload = result
        job = json.loads(payload)
        logger.debug("[queue] popped %s from %s", job.get("story_id"), stage)
        return job

    def requeue(self, job: dict, next_stage: str) -> None:
        """Move a job to the next stage queue."""
        job["stage"] = next_stage
        job["attempt"] = job.get("attempt", 1)
        payload = json.dumps(job)
        key = self.KEY_PREFIX + next_stage
        self._r.rpush(key, payload)
        logger.debug("[queue] requeued %s → %s", job.get("story_id"), next_stage)

    def dead_letter(self, job: dict, reason: str) -> None:
        """Send a failed job to the dead-letter queue."""
        job["dead_reason"] = reason
        job["dead_at"] = time.time()
        self._r.rpush(self.DEAD_LETTER, json.dumps(job))
        logger.error("[queue] DLQ: %s | reason: %s", job.get("story_id"), reason)

    def retry(self, job: dict, stage: str, max_retries: int = None) -> bool:
        """
        Increment attempt counter and requeue, or DLQ if max exceeded.
        Returns True if requeued, False if dead-lettered.
        """
        max_r = max_retries or cfg.queue.max_retries
        job["attempt"] = job.get("attempt", 1) + 1
        if job["attempt"] > max_r:
            self.dead_letter(job, f"exceeded max retries ({max_r})")
            return False
        wait = 2 ** (job["attempt"] - 1)
        logger.warning(
            "[queue] retry %d/%d for %s in %ds",
            job["attempt"], max_r, job.get("story_id"), wait
        )
        time.sleep(wait)
        self.requeue(job, stage)
        return True

    def queue_length(self, stage: str) -> int:
        return self._r.llen(self.KEY_PREFIX + stage)

    def dead_letter_length(self) -> int:
        return self._r.llen(self.DEAD_LETTER)

    def flush_stage(self, stage: str) -> int:
        """Flush all jobs from a stage queue. Returns count deleted."""
        key = self.KEY_PREFIX + stage
        length = self._r.llen(key)
        self._r.delete(key)
        logger.warning("[queue] flushed %d jobs from %s", length, stage)
        return length

    def health_check(self) -> dict:
        stages = [
            "normalize", "clean", "score", "hook",
            "validate", "censor", "format", "store",
            "video_build", "publish"
        ]
        return {
            "status": "ok" if self._r.ping() else "unreachable",
            "queues": {s: self.queue_length(s) for s in stages},
            "dead_letter": self.dead_letter_length(),
        }


# Singleton
_queue_instance: Optional[JobQueue] = None

def get_queue() -> JobQueue:
    global _queue_instance
    if _queue_instance is None:
        _queue_instance = JobQueue()
    return _queue_instance
```

---

**Ctrl+S**, close. Now open the second one:

**FILE 16b — `story_engine/queue/worker.py`**
```
notepad G:\RedditYouTubeEmpire\story_engine\queue\worker.py