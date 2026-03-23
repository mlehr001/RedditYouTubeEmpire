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


_MYSTERY_TOP5_PROMPT = """\
You are a mystery documentary narrator. Your tone is eerie, curious, and dramatic.
Think LEMMiNO meets Bedtime Stories.

Write a Top 5 YouTube countdown script about: {topic_title}

STRICT RULES:
- Cold open hook BEFORE revealing the topic (5-8 seconds, most unsettling tease)
- Each entry flows naturally with a clear countdown marker
- Build dread as list counts DOWN to #1
- #1 is ALWAYS the most disturbing or unexplained entry
- Mix short punchy sentences with slower dramatic ones
- Use pauses (...) for effect
- Treat real victims with gravity — never sensationalize suffering
- Reference actual evidence and real footage when available
- Style words: eerie, unsettling, strange, unexplained, bizarre, chilling, disturbing, forgotten, vanished

STRUCTURE — use these EXACT markers for the editor:
[COLD OPEN] Most unsettling tease — do NOT reveal the topic yet
[INTRO] Brief setup (2-3 sentences max)
[NUMBER 5: {entry5_title}] Entry 5 story + evidence + why it matters
[NUMBER 4: {entry4_title}] Entry 4
[NUMBER 3: {entry3_title}] Entry 3
[NUMBER 2: {entry2_title}] Entry 2
[NUMBER 1: {entry1_title}] Most disturbing — save best for last
[OUTRO] Leave audience unsettled, thinking, or wanting more

ENTRIES (use these real facts — do not invent new ones):
{entries_block}

{angle_block}

Return ONLY valid JSON (no markdown fences):
{{
  "script": "full script with [NUMBER X: Title] markers",
  "entries": [
    {{
      "number": 5,
      "title": "entry title",
      "script_section": "just this entry's narration text",
      "media_query": "search term for real footage of this entry"
    }}
  ],
  "keywords": ["dark", "mystery", "abandoned", "forest", "signal"],
  "titles": ["Title Option 1", "Title Option 2", "Title Option 3"]
}}"""


def build_mystery_top5_script(
    topic: dict, entries: list, angle: dict | None = None
) -> dict:
    """
    Writes a Top 5 mystery countdown script using Anthropic Claude (primary).
    Falls back to OpenAI if ANTHROPIC_API_KEY is missing.

    Args:
        topic:   Topic dict from mystery_scraper.get_mystery_topic().
        entries: List of scored entries (top 5 used for script).
        angle:   Optional angle dict from angle_selector.generate_angles().

    Returns:
        {
          "script": str,              # full narration with [NUMBER X] markers
          "entries": list,            # per-entry breakdown with media_query
          "keywords": list,           # 5–8 visual keywords for B-roll
          "titles": list              # 3 candidate YouTube titles
        }
    """
    from modules.pipeline_logger import log_pipeline

    top5 = entries[:5]
    topic_title = topic.get("title", "Top 5 Mysteries")

    entries_block = "\n\n".join([
        f"Entry {i + 1} (#{5 - i}): {e['title']}\n"
        f"Summary: {e['summary'][:300]}\n"
        f"Source: {e.get('source_url', '')}"
        for i, e in enumerate(top5)
    ])

    angle_block = ""
    if angle:
        angle_block = (
            f"COMMENTARY ANGLE (shapes tone only — do not invent facts):\n"
            f"  {angle['title']}: {angle['core_take']}\n"
            f"  Style: {angle['style']}"
        )

    # Assign titles for prompt markers (pad if fewer than 5)
    padded = (top5 + [{"title": "Unknown Entry"}] * 5)[:5]
    prompt = _MYSTERY_TOP5_PROMPT.format(
        topic_title=topic_title,
        entry5_title=padded[0]["title"],
        entry4_title=padded[1]["title"],
        entry3_title=padded[2]["title"],
        entry2_title=padded[3]["title"],
        entry1_title=padded[4]["title"],
        entries_block=entries_block,
        angle_block=angle_block,
    )

    topic_id = topic.get("topic_id", "mystery")
    anthropic_key = os.getenv("ANTHROPIC_API_KEY")
    openai_key = os.getenv("OPENAI_API_KEY")

    # Try Anthropic first (primary for creative writing)
    if anthropic_key:
        try:
            import anthropic
            client = anthropic.Anthropic(api_key=anthropic_key)
            response = client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=4000,
                messages=[{"role": "user", "content": prompt}],
            )
            tokens_used = response.usage.input_tokens + response.usage.output_tokens
            log_pipeline(topic_id, "mystery_script_writing", "anthropic/claude-sonnet-4-6",
                         tokens_used)

            raw = response.content[0].text.strip()
            raw = re.sub(r"^```(?:json)?\s*", "", raw)
            raw = re.sub(r"\s*```$", "", raw)
            result = json.loads(raw)

            if "script" in result and "entries" in result:
                print("[OK] Mystery script written by Claude")
                return result

        except Exception as e:
            log.warning(f"Claude mystery script failed: {e} — trying OpenAI.")

    # Fallback: OpenAI
    if openai_key:
        try:
            from openai import OpenAI
            client = OpenAI(api_key=openai_key)
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.7,
                max_tokens=4000,
            )
            log_pipeline(topic_id, "mystery_script_writing", "openai/gpt-4o-mini",
                         response.usage.total_tokens)

            raw = response.choices[0].message.content.strip()
            raw = re.sub(r"^```(?:json)?\s*", "", raw)
            raw = re.sub(r"\s*```$", "", raw)
            result = json.loads(raw)

            if "script" in result and "entries" in result:
                print("[OK] Mystery script written by OpenAI (fallback)")
                return result

        except Exception as e:
            log.warning(f"OpenAI mystery script also failed: {e} — using fallback.")

    # Hard fallback: minimal non-AI script
    log.warning("All AI script generation failed — building minimal fallback script.")
    fallback_script_parts = [
        "[COLD OPEN] Something happened... and nobody can explain it.",
        "[INTRO] These are five of the most unsettling mysteries ever documented.",
    ]
    fallback_entries = []
    for i, entry in enumerate(top5):
        num = 5 - i
        marker = f"[NUMBER {num}: {entry['title']}]"
        fallback_script_parts.append(f"{marker} {entry['summary'][:300]}")
        fallback_entries.append({
            "number": num,
            "title": entry["title"],
            "script_section": entry["summary"][:300],
            "media_query": entry["title"],
        })
    fallback_script_parts.append("[OUTRO] The truth is still out there. Subscribe for more.")

    return {
        "script": "\n\n".join(fallback_script_parts),
        "entries": fallback_entries,
        "keywords": ["mystery", "dark", "forest", "abandoned", "shadow"],
        "titles": [
            f"Top 5 {topic_title} (You Won't Sleep After This)",
            f"These 5 Mysteries Have Never Been Solved...",
            f"The Most Disturbing {topic_title} Ever Documented",
        ],
    }


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
