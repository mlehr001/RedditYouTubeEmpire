"""
pipelines/shared.py — Helpers used by both story and mystery pipelines.
"""

import os
import json

import config


def _store_json(entity_id: str, label: str, data: dict) -> None:
    """Save a JSON sidecar to output/ for debugging and audit trail."""
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    path = os.path.join(config.OUTPUT_DIR, f"{entity_id}_{label}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def _mark_post_used(post_id: str) -> None:
    with open("used_posts.txt", "a") as f:
        f.write(post_id + "\n")
