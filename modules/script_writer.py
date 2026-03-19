"""
script_writer.py — Formats a Reddit post into a conversational TTS-ready script.
AI is used for narration framing, TTS pacing, keyword extraction, and title generation.
Story body is preserved verbatim — AI adds only structural narration, not story content.
"""

import re
import json
import logging
import os
import config

log = logging.getLogger(__name__)


def _clean_text(text: str) -> str:
    """Remove markdown, URLs, excessive whitespace, and Reddit-isms."""
    text = re.sub(r"http\S+", "", text)
    text = re.sub(r"\*{1,3}(.*?)\*{1,3}", r"\1", text)
    text = re.sub(r"^#{1,6}\s+", "", text, flags=re.MULTILINE)
    text = re.sub(r"u/\w+", "someone", text)
    text = re.sub(r"r/\w+", "this community", text)
    text = re.sub(r"(?i)(edit\s*\d*[:.]?|update[:.]?|tldr[:.]?).*", "", text)
    text = re.sub(r"\n{2,}", "\n\n", text)
    text = re.sub(r" {2,}", " ", text)
    return text.strip()


def _trim_to_word_limit(text: str, max_words: int) -> str:
    words = text.split()
    if len(words) <= max_words:
        return text
    trimmed = " ".join(words[:max_words])
    last_period = max(trimmed.rfind("."), trimmed.rfind("!"), trimmed.rfind("?"))
    if last_period > 0:
        trimmed = trimmed[:last_period + 1]
    return trimmed


_AI_PROMPT = """\
You are a script formatter for a YouTube storytelling channel. Your only job is:
1. Add a short conversational hook (2 sentences max) before the story — something like "So this person posts... and it gets wild fast." or "Okay so this one had me absolutely speechless."
2. Insert brief narration transitions between story sections: "And then...", "But here's where it gets interesting...", "So naturally...", "Wait — it gets worse."
3. Add commas and ellipses where natural for TTS pacing. Short punchy sentences.
4. End with the outro provided.
5. DO NOT rewrite, alter, summarize, or fabricate any story content. The story text must appear verbatim.

Structure the output as: Hook → Setup → Tension → Twist → Fallout

Then separately extract:
- keywords: 5–8 single visual words for b-roll (e.g. "phone", "argument", "office", "night", "car"). No phrases.
- titles: exactly 3 YouTube title options using curiosity + emotion. Never use the original Reddit title. Never reveal the ending. Use formats like "She Found Something In His Phone..." or "He Did WHAT At Their Wedding?"

Return ONLY valid JSON (no markdown fences) in this exact shape:
{
  "script": "...",
  "keywords": ["word1", "word2", ...],
  "titles": ["Title 1", "Title 2", "Title 3"]
}"""


def _ai_format(post: dict, body: str) -> dict | None:
    """
    Calls OpenAI to format the script and extract keywords + titles.
    Returns dict with script/keywords/titles, or None on failure.
    """
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return None

    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key)

        user_content = (
            f"Subreddit: r/{post['subreddit']}\n"
            f"Original title: {post['title']}\n\n"
            f"--- STORY (verbatim, do not alter) ---\n{body}\n---\n\n"
            f"Outro to append: {config.OUTRO}"
        )

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": _AI_PROMPT},
                {"role": "user", "content": user_content},
            ],
            temperature=0.4,
            max_tokens=2000,
        )

        raw = response.choices[0].message.content.strip()
        # Strip markdown fences if model added them
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
        result = json.loads(raw)

        # Validate shape
        if not all(k in result for k in ("script", "keywords", "titles")):
            raise ValueError("Missing required keys in AI response")
        if len(result["titles"]) < 3:
            raise ValueError("Expected 3 titles")

        return result

    except Exception as e:
        log.warning(f"AI script formatting failed: {e}")
        return None


def _fallback_format(post: dict, body: str) -> dict:
    """
    Non-AI fallback: wraps verbatim story with minimal narration.
    Returns same dict shape as _ai_format.
    """
    intro = config.INTRO_TEMPLATE.format(subreddit=post["subreddit"])
    title = post["title"].strip().rstrip(".")
    script = f"{intro}\n\n{title}.\n\n{body}\n\n{config.OUTRO}"

    # Simple keyword extraction: most common meaningful nouns
    stop = {"i", "me", "my", "we", "our", "the", "a", "an", "and", "or", "but",
            "is", "was", "were", "he", "she", "they", "his", "her", "their",
            "this", "that", "in", "on", "at", "to", "for", "of", "with",
            "had", "have", "has", "so", "just", "got", "get", "not", "be",
            "it", "its", "are", "been", "what", "when", "how", "all", "about",
            "said", "told", "did", "do", "don't", "didn't", "can", "would"}
    words = re.findall(r"\b[a-z]{4,}\b", body.lower())
    freq: dict[str, int] = {}
    for w in words:
        if w not in stop:
            freq[w] = freq.get(w, 0) + 1
    keywords = [w for w, _ in sorted(freq.items(), key=lambda x: -x[1])[:8]]

    titles = [
        f"You Won't Believe What Happened In r/{post['subreddit']}",
        f"This Story From Reddit Had Everyone Talking...",
        f"He Said WHAT? A Reddit Story That Broke The Internet",
    ]

    return {"script": script, "keywords": keywords, "titles": titles}


def build_script(post: dict) -> dict:
    """
    Takes a post dict and returns:
      {
        "script":   str  — TTS-ready narration script,
        "keywords": list — 5-8 visual b-roll keywords,
        "titles":   list — 3 candidate YouTube titles
      }
    """
    body = _clean_text(post["body"])

    # Reserve words for intro/outro overhead
    reserved = len(config.INTRO_TEMPLATE.split()) + len(config.OUTRO.split()) + 30
    body_limit = config.MAX_SCRIPT_WORDS - reserved
    body = _trim_to_word_limit(body, body_limit)

    result = _ai_format(post, body)
    if result is None:
        log.info("Using fallback (non-AI) script formatting.")
        result = _fallback_format(post, body)

    return result
