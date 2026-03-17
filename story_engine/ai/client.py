"""
AI client wrapper. Supports Anthropic (Claude) and OpenAI.
Strictly enforces prompt library rules:
  - extraction, scoring, hook, formatting, title prompts only
  - No fabrication, no rewriting, no embellishment
"""

import json
import logging
from typing import Optional

from story_engine.config.settings import cfg
from story_engine.utils.retry import retry

logger = logging.getLogger(__name__)

# ── Sanctioned prompt templates ──────────────────────────────────────────────

SCORING_PROMPT = """You are a content retention analyst. Rate the following story on its retention potential for short-form video content.

STORY TITLE: {title}

STORY BODY:
{body}

Rate this story from 1 to 10 on retention potential. Consider:
- Emotional hook strength
- Narrative tension or conflict
- Relatability or shock value
- Clarity of the core event
- Payoff or resolution

Respond ONLY with valid JSON in this exact format:
{{
  "score": <number 1-10, one decimal place>,
  "rationale": "<2-3 sentences explaining the score>",
  "category": "<one of: drama, revenge, confession, relationship, humor, crime, tragedy, inspiration>",
  "tags": ["<tag1>", "<tag2>", "<tag3>"]
}}

Do not include any other text."""

HOOK_PROMPT = """You are a video hook specialist. Given the story below, identify which EXISTING sentence or phrase from the story would make the strongest opening hook for a short-form video.

IMPORTANT RULES:
- You MUST use verbatim text from the story. Do NOT write new text.
- Only reorder or select existing elements. Do not add, remove, or modify any words.
- The hook should be a complete sentence or short passage from the story body.

STORY TITLE: {title}

STORY BODY:
{body}

Respond ONLY with valid JSON:
{{
  "hook_text": "<verbatim text from the story>",
  "hook_start_char": <integer — character index where hook starts in body>,
  "hook_end_char": <integer — character index where hook ends in body>,
  "rationale": "<one sentence explaining why this is the best hook>"
}}

Do not include any other text."""

FORMATTING_PROMPT = """You are a video script formatter. Break the following story into short narration lines suitable for video pacing (2-8 seconds per line at normal reading speed).

IMPORTANT RULES:
- Use ONLY verbatim text from the story. Do NOT add, remove, or change any words.
- Split only at natural sentence boundaries or clause breaks.
- Each line should be 10-25 words maximum.
- Do not combine or rewrite content across lines.

STORY BODY:
{body}

Respond ONLY with valid JSON:
{{
  "lines": [
    "<verbatim line 1>",
    "<verbatim line 2>",
    ...
  ],
  "total_lines": <integer>,
  "estimated_duration_sec": <integer — at ~130 words per minute>
}}

Do not include any other text."""

TITLE_PROMPT = """You are a title specialist. Generate a curiosity-driven title for the following story for use on social media video platforms.

IMPORTANT RULES:
- The title must NOT summarize or reveal the outcome/ending.
- The title should create curiosity or tension.
- Maximum 10 words.
- Do not use clickbait phrases like "You won't believe..." or "This will shock you..."
- Do not use the story's original title verbatim.

STORY TITLE: {title}
STORY CATEGORY: {category}
STORY BODY (first 300 chars): {body_preview}

Respond ONLY with valid JSON:
{{
  "ai_title": "<your title here>"
}}

Do not include any other text."""


class AIClient:
    """
    Single AI client interface. Provider-agnostic.
    Only accepts sanctioned prompt types.
    """

    SANCTIONED_PROMPTS = {"scoring", "hook", "formatting", "title", "extraction"}

    def __init__(self):
        self.provider = cfg.ai.provider
        self._client = self._init_client()

    def _init_client(self):
        if self.provider == "anthropic":
            import anthropic
            return anthropic.Anthropic(api_key=cfg.ai.anthropic_key)
        elif self.provider == "openai":
            import openai
            return openai.OpenAI(api_key=cfg.ai.openai_key)
        else:
            raise ValueError(f"Unknown AI provider: {self.provider}")

    @retry(max_attempts=3, delay=2.0, exceptions=(Exception,))
    def _call(self, prompt: str, max_tokens: int = 1024) -> str:
        """Raw API call — returns response text."""
        if self.provider == "anthropic":
            response = self._client.messages.create(
                model=cfg.ai.model_anthropic,
                max_tokens=max_tokens,
                messages=[{"role": "user", "content": prompt}],
            )
            return response.content[0].text
        elif self.provider == "openai":
            response = self._client.chat.completions.create(
                model=cfg.ai.model_openai,
                max_tokens=max_tokens,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
            )
            return response.choices[0].message.content

    def _parse_json(self, text: str) -> dict:
        """Parse JSON from AI response, stripping markdown fences if needed."""
        text = text.strip()
        if text.startswith("```"):
            lines = text.split("\n")
            text = "\n".join(lines[1:-1] if lines[-1] == "```" else lines[1:])
        return json.loads(text)

    def score_story(self, title: str, body: str) -> dict:
        prompt = SCORING_PROMPT.format(title=title, body=body[:4000])
        raw = self._call(prompt, max_tokens=512)
        result = self._parse_json(raw)
        logger.debug("[ai:score] score=%.1f category=%s", result.get("score"), result.get("category"))
        return result

    def generate_hook(self, title: str, body: str) -> dict:
        prompt = HOOK_PROMPT.format(title=title, body=body[:4000])
        raw = self._call(prompt, max_tokens=512)
        result = self._parse_json(raw)
        # Validate: hook_text must exist verbatim in body
        hook_text = result.get("hook_text", "")
        if hook_text and hook_text not in body:
            logger.warning("[ai:hook] hook_text not found verbatim in body — discarding")
            result["hook_text"] = None
            result["verbatim_validated"] = False
        else:
            result["verbatim_validated"] = True
        return result

    def format_story(self, body: str) -> dict:
        prompt = FORMATTING_PROMPT.format(body=body[:8000])
        raw = self._call(prompt, max_tokens=2048)
        result = self._parse_json(raw)
        # Validate: each line must exist (approximately) in body
        lines = result.get("lines", [])
        validated_lines = []
        for line in lines:
            if line.strip() and line.strip() in body:
                validated_lines.append(line.strip())
            else:
                # Soft validate — line may have minor whitespace normalization
                normalized = " ".join(line.split())
                normalized_body = " ".join(body.split())
                if normalized in normalized_body:
                    validated_lines.append(line.strip())
                else:
                    logger.warning("[ai:format] line not verbatim — dropping: %s...", line[:50])
        result["lines"] = validated_lines
        result["total_lines"] = len(validated_lines)
        return result

    def generate_title(self, title: str, body: str, category: str = "") -> dict:
        prompt = TITLE_PROMPT.format(
            title=title,
            category=category,
            body_preview=body[:300],
        )
        raw = self._call(prompt, max_tokens=128)
        return self._parse_json(raw)


# Singleton
_ai_instance: Optional[AIClient] = None

def get_ai() -> AIClient:
    global _ai_instance
    if _ai_instance is None:
        _ai_instance = AIClient()
    return _ai_instance