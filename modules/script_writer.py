"""
script_writer.py — Formats a Reddit post into a clean TTS-ready script
"""

import re
import config


def _clean_text(text):
    """Remove markdown, URLs, excessive whitespace, and Reddit-isms."""
    # Remove URLs
    text = re.sub(r"http\S+", "", text)
    # Remove markdown bold/italic
    text = re.sub(r"\*{1,3}(.*?)\*{1,3}", r"\1", text)
    # Remove markdown headers
    text = re.sub(r"^#{1,6}\s+", "", text, flags=re.MULTILINE)
    # Remove Reddit username mentions
    text = re.sub(r"u/\w+", "someone", text)
    # Remove subreddit mentions
    text = re.sub(r"r/\w+", "this subreddit", text)
    # Remove edit notes (common in Reddit posts)
    text = re.sub(r"(?i)(edit\s*\d*[:.]?|update[:.]?|tldr[:.]?).*", "", text)
    # Collapse multiple newlines
    text = re.sub(r"\n{2,}", "\n\n", text)
    # Remove excessive spaces
    text = re.sub(r" {2,}", " ", text)
    return text.strip()


def _trim_to_word_limit(text, max_words):
    """Trim text to max_words, ending at a sentence boundary."""
    words = text.split()
    if len(words) <= max_words:
        return text

    trimmed = " ".join(words[:max_words])
    # Find the last sentence-ending punctuation
    last_period = max(trimmed.rfind("."), trimmed.rfind("!"), trimmed.rfind("?"))
    if last_period > 0:
        trimmed = trimmed[:last_period + 1]
    return trimmed


def build_script(post):
    """
    Takes a post dict and returns a clean, TTS-ready script string.
    """
    intro = config.INTRO_TEMPLATE.format(subreddit=post["subreddit"])
    title = post["title"].strip().rstrip(".")
    body = _clean_text(post["body"])

    # Reserve words for intro/outro
    reserved = len(intro.split()) + len(config.OUTRO.split()) + len(title.split()) + 20
    body_limit = config.MAX_SCRIPT_WORDS - reserved
    body = _trim_to_word_limit(body, body_limit)

    script = f"{intro}\n\n{title}.\n\n{body}\n\n{config.OUTRO}"
    return script
