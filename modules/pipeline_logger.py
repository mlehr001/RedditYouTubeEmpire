"""
pipeline_logger.py — Logs AI task usage to pipeline_log.csv.
Tracks: post_id, task, model_used, tokens_used, timestamp
"""

import csv
import os
from datetime import datetime

PIPELINE_LOG_CSV = "pipeline_log.csv"


def log_pipeline(post_id: str, task: str, model_used: str, tokens_used: int) -> None:
    """Append one row to pipeline_log.csv."""
    file_exists = os.path.exists(PIPELINE_LOG_CSV)
    with open(PIPELINE_LOG_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["post_id", "task", "model_used", "tokens_used", "timestamp"])
        writer.writerow([
            post_id,
            task,
            model_used,
            tokens_used,
            datetime.utcnow().isoformat(),
        ])
