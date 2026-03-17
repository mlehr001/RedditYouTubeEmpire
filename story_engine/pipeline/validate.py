"""
Validate stage.
Hard rules — story fails here if ANY critical check fails.
All checks logged to validation_log table.
"""

import logging
import re
from dataclasses import dataclass
from typing import List, Optional

from story_engine.db.database import get_db
from story_engine.queue.worker import BaseWorker

logger = logging.getLogger(__name__)


@dataclass
class Check:
    rule: str
    passed: bool
    detail: str
    critical: bool = True


class StoryValidator:
    """
    Runs all validation checks against a story.
    Returns (passed: bool, checks: List[Check], failure_reason: Optional[str])
    """

    MIN_WORD_COUNT = 150
    MAX_WORD_COUNT = 5000
    MIN_SCORE = 7.0
    REQUIRED_PIPELINE_STATUS = {"hooked"}

    def validate(self, story: dict, ai_assist: dict) -> tuple[bool, List[Check], Optional[str]]:
        checks = []

        # 1. Word count
        wc = story.get("word_count", 0)
        checks.append(Check(
            rule="min_word_count",
            passed=wc >= self.MIN_WORD_COUNT,
            detail=f"word_count={wc} min={self.MIN_WORD_COUNT}",
            critical=True,
        ))
        checks.append(Check(
            rule="max_word_count",
            passed=wc <= self.MAX_WORD_COUNT,
            detail=f"word_count={wc} max={self.MAX_WORD_COUNT}",
            critical=True,
        ))

        # 2. Body is not empty
        body = story.get("body", "")
        checks.append(Check(
            rule="body_not_empty",
            passed=bool(body and body.strip()),
            detail=f"body_length={len(body)}",
            critical=True,
        ))

        # 3. Score threshold
        score = float(ai_assist.get("score") or 0)
        checks.append(Check(
            rule="min_score",
            passed=score >= self.MIN_SCORE,
            detail=f"score={score:.1f} threshold={self.MIN_SCORE}",
            critical=True,
        ))

        # 4. Hook exists
        hook = ai_assist.get("hook_text") or ""
        checks.append(Check(
            rule="hook_present",
            passed=bool(hook and len(hook.split()) >= 5),
            detail=f"hook_words={len(hook.split()) if hook else 0}",
            critical=True,
        ))

        # 5. Hook is verbatim in body
        if hook:
            checks.append(Check(
                rule="hook_verbatim",
                passed=hook in body or " ".join(hook.split()) in " ".join(body.split()),
                detail="hook text verified against body",
                critical=True,
            ))

        # 6. Language is English
        lang = story.get("language", "en")
        checks.append(Check(
            rule="language_english",
            passed=lang == "en",
            detail=f"detected_language={lang}",
            critical=False,  # warn only
        ))

        # 7. No duplicate content signature (basic)
        checks.append(Check(
            rule="not_placeholder",
            passed=not self._is_placeholder(body),
            detail="content not placeholder/lorem ipsum",
            critical=True,
        ))

        # 8. AI title exists
        ai_title = ai_assist.get("ai_title") or ""
        checks.append(Check(
            rule="ai_title_present",
            passed=bool(ai_title and len(ai_title) >= 5),
            detail=f"ai_title_length={len(ai_title)}",
            critical=False,
        ))

        # Evaluate
        critical_failures = [c for c in checks if not c.passed and c.critical]
        passed = len(critical_failures) == 0
        failure_reason = (
            "; ".join(f"{c.rule}: {c.detail}" for c in critical_failures)
            if critical_failures else None
        )
        return passed, checks, failure_reason

    def _is_placeholder(self, body: str) -> bool:
        placeholder_signals = ["lorem ipsum", "test story", "placeholder content"]
        body_lower = body.lower()
        return any(s in body_lower for s in placeholder_signals)


class ValidateWorker(BaseWorker):
    stage_name = "validate"
    next_stage = "censor"

    def __init__(self):
        super().__init__()
        self.validator = StoryValidator()

    def process(self, story_id: str, meta: dict) -> dict:
        db = get_db()

        story = db.execute_one("SELECT * FROM stories WHERE id=%s", (story_id,))
        if not story:
            raise ValueError(f"story {story_id} not found")

        ai_assist = db.execute_one("SELECT * FROM ai_assist WHERE story_id=%s", (story_id,))
        if not ai_assist:
            raise ValueError(f"ai_assist not found for story {story_id}")

        passed, checks, failure_reason = self.validator.validate(story, ai_assist)

        # Log all checks to DB
        import json
        db.execute(
            """
            INSERT INTO validation_log (story_id, stage, passed, checks)
            VALUES (%s, 'validate', %s, %s)
            """,
            (
                story_id,
                passed,
                json.dumps([
                    {"rule": c.rule, "passed": c.passed, "detail": c.detail, "critical": c.critical}
                    for c in checks
                ]),
            ),
        )

        if not passed:
            db.execute(
                """
                UPDATE stories
                SET pipeline_status='rejected', rejection_reason=%s, updated_at=NOW()
                WHERE id=%s
                """,
                (failure_reason, story_id),
            )
            logger.warning("[validate] story=%s reason=%s", story_id, failure_reason)
            return {"success": False, "reason": failure_reason}

        db.execute(
            "UPDATE stories SET pipeline_status='validated', updated_at=NOW() WHERE id=%s",
            (story_id,),
        )
        passed_count = sum(1 for c in checks if c.passed)
        logger.info("[validate] story=%s checks=%d/%d", story_id, passed_count, len(checks))
        return {"success": True, "meta": {}}