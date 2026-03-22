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


_AI_PROMPT_BASE = """\
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


def _build_system_prompt(angle: dict | None) -> str:
    """
    Builds the system prompt, optionally injecting a commentary angle.
    The angle shapes hook tone and transition style only — never story content.
    """
    if angle is None:
        return _AI_PROMPT_BASE

    angle_block = (
        f"\nCOMMENTARY ANGLE (for hook and transition tone only — do NOT alter story):\n"
        f"  Title: {angle['title']}\n"
        f"  Core Take: {angle['core_take']}\n"
        f"  Style: {angle['style']}\n"
        f"Let this angle inform the hook wording and transition energy. "
        f"Story body must remain verbatim.\n"
    )
    return _AI_PROMPT_BASE + angle_block


def _ai_format(post: dict, body: str, angle: dict | None = None) -> dict | None:
    """
    Calls OpenAI to format the script and extract keywords + titles.
    Accepts an optional angle dict to shape hook/transition tone.
    Returns dict with script/keywords/titles, or None on failure.
    """
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return None

    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key)

        system_prompt = _build_system_prompt(angle)

        user_content = (
            f"Subreddit: r/{post['subreddit']}\n"
            f"Original title: {post['title']}\n\n"
            f"--- STORY (verbatim, do not alter) ---\n{body}\n---\n\n"
            f"Outro to append: {config.OUTRO}"
        )

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
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


_COMMENTARY_PROMPT = """\
You are a YouTube commentary creator.

Write a high-retention script based on this angle.

RULES:
- conversational tone
- slightly sarcastic or opinionated
- short sentences
- fast pacing
- no fluff
- no formal language

STRUCTURE:
1. Hook (first 5-8 seconds, strong opinion or curiosity)
2. Context (quick setup)
3. Commentary (your take)
4. Escalation (why this matters or gets worse)
5. Punchline / takeaway

STYLE:
- sound like a real person talking
- use emphasis words (weird, insane, awkward, wild)
- break sentences naturally
- include pauses when appropriate (use ...)
- avoid robotic phrasing

OUTPUT:
Only the final script, nothing else.

Angle: {angle_title} — {angle_core_take}
Topic: {topic_summary}"""


def build_commentary_script(post: dict, angle: dict | None = None) -> dict:
    """
    Builds a commentary-style script using the Anthropic API.
    Used for non-Reddit-personal-story sources (commentary, hn, 4chan).

    Args:
        post:  Scraped post dict with title/body/source_type.
        angle: Optional angle dict from angle_selector.generate_angles().

    Returns:
        {"script": str, "keywords": list, "titles": list}
    """
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        log.warning("ANTHROPIC_API_KEY not set — falling back to non-AI script.")
        return _fallback_format(post, _clean_text(post["body"]))

    angle_title = angle["title"] if angle else "General commentary"
    angle_core_take = angle["core_take"] if angle else "Interesting story worth discussing"
    topic_summary = f"{post['title'].strip()} — {_clean_text(post['body'])[:400]}"

    system_prompt = _COMMENTARY_PROMPT.format(
        angle_title=angle_title,
        angle_core_take=angle_core_take,
        topic_summary=topic_summary,
    )

    user_content = (
        'Return JSON only:\n'
        '{\n'
        '  "script": "...",\n'
        '  "keywords": ["word1", "word2", ...],\n'
        '  "titles": ["Title 1", "Title 2", "Title 3"]\n'
        '}'
    )

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)

        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=2000,
            messages=[
                {"role": "user", "content": f"{system_prompt}\n\n{user_content}"},
            ],
        )

        raw = response.content[0].text.strip()
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)
        result = json.loads(raw)

        if not all(k in result for k in ("script", "keywords", "titles")):
            raise ValueError("Missing required keys in Anthropic response")
        if len(result["titles"]) < 3:
            raise ValueError("Expected 3 titles")

        return result

    except Exception as e:
        log.warning(f"Commentary script (Anthropic) failed: {e} — using fallback.")
        return _fallback_format(post, _clean_text(post["body"]))


def build_script(post: dict, angle: dict | None = None) -> dict:
    """
    Takes a post dict and returns:
      {
        "script":   str  — TTS-ready narration script,
        "keywords": list — 5-8 visual b-roll keywords,
        "titles":   list — 3 candidate YouTube titles
      }

    Args:
        post:  Scraped post dict with title/body/subreddit/id/score.
        angle: Optional selected angle dict from angle_selector.generate_angles().
               Shapes hook tone and transition energy; never alters story content.
    """
    body = _clean_text(post["body"])

    # Reserve words for intro/outro overhead
    reserved = len(config.INTRO_TEMPLATE.split()) + len(config.OUTRO.split()) + 30
    body_limit = config.MAX_SCRIPT_WORDS - reserved
    body = _trim_to_word_limit(body, body_limit)

    result = _ai_format(post, body, angle=angle)
    if result is None:
        log.info("Using fallback (non-AI) script formatting.")
        result = _fallback_format(post, body)

    return result
