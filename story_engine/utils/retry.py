"""
Retry decorator with exponential backoff.
Usage:
    @retry(max_attempts=3, delay=2.0, exceptions=(RateLimitError,))
    def call_api(): ...
"""

import functools
import logging
import time
from typing import Tuple, Type

logger = logging.getLogger(__name__)


def retry(
    max_attempts: int = 3,
    delay: float = 2.0,
    backoff: float = 2.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 0
            wait = delay
            while attempt < max_attempts:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    attempt += 1
                    if attempt >= max_attempts:
                        logger.error(
                            "[retry] %s failed after %d attempts: %s",
                            func.__name__, max_attempts, e
                        )
                        raise
                    logger.warning(
                        "[retry] %s attempt %d/%d failed: %s — retrying in %.1fs",
                        func.__name__, attempt, max_attempts, e, wait
                    )
                    time.sleep(wait)
                    wait *= backoff
        return wrapper
    return decorator
```

---

**Ctrl+S**, close. Now:

**FILE 20b — `story_engine/utils/logging_setup.py`**
```
notepad G:\RedditYouTubeEmpire\story_engine\utils\logging_setup.py

"""
Structured logging setup. Call setup_logging() once at app entry point.
All pipeline stages use: logger = logging.getLogger(__name__)
"""

import logging
import logging.handlers
import os
import sys
from pathlib import Path


def setup_logging(log_level: str = "INFO", log_dir: str = "logs"):
    Path(log_dir).mkdir(parents=True, exist_ok=True)

    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    root = logging.getLogger()
    root.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    # Console
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    root.addHandler(ch)

    # Rotating file — 50MB per file, keep 10
    fh = logging.handlers.RotatingFileHandler(
        filename=os.path.join(log_dir, "pipeline.log"),
        maxBytes=50 * 1024 * 1024,
        backupCount=10,
        encoding="utf-8",
    )
    fh.setFormatter(fmt)
    root.addHandler(fh)

    # Errors-only file
    eh = logging.handlers.RotatingFileHandler(
        filename=os.path.join(log_dir, "errors.log"),
        maxBytes=20 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    eh.setLevel(logging.ERROR)
    eh.setFormatter(fmt)
    root.addHandler(eh)

    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)