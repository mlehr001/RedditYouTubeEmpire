"""
scraper.py — Pulls top posts from configured subreddits (PRAW or public JSON) or Hacker News.
Public Reddit JSON API is used automatically when PRAW credentials are not set.
"""

import os
import re
import html as html_module
import requests
import praw
import config


# ─── Reddit Public JSON API (no auth required) ────────────────────────────────

REDDIT_JSON_HEADERS = {
    "User-Agent": "Mozilla/5.0 StoryEngine/2.0 (personal project)"
}

# Keywords that signal a personal story worth narrating
_STORY_SIGNALS = [
    "my boyfriend", "my girlfriend", "my wife", "my husband", "my partner",
    "my friend", "my boss", "my family", "my mom", "my dad", "my sister",
    "my brother", "my coworker", "my roommate",
    "cheated", "cheating", "broke up", "breakup", "ghosted", "blocked",
    "confronted", "found out", "i was", "i got", "i quit", "i lost",
    "argument", "fight", "drama", "revenge", "betrayed", "lied to",
    "he told me", "she told me", "they said", "we were", "he cheated",
    "she cheated", "walked in on", "text message", "caught",
]

# Hard kill — no news, no politics, no "requires context"
_HARD_KILL = [
    "trump", "biden", "harris", "maga", "election", "voting", "congress",
    "senate", "political", "politics", "politician", "white house",
    "breaking news", "just in:", "[update]", "update to my",
    "requires context", "long post ahead", "tw:", "trigger warning",
    "part 1", "part 2", "part 3",
]


def _passes_story_filter(title: str, body: str) -> bool:
    combined = (title + " " + body[:600]).lower()
    if any(kill in combined for kill in _HARD_KILL):
        return False
    return any(sig in combined for sig in _STORY_SIGNALS)


def get_reddit_json_post(min_words: int = 150) -> dict | None:
    """
    Fetches a personal story post from Reddit using the public JSON API.
    No PRAW credentials required. Returns same dict shape as get_post().
    """
    used = _load_used_posts() if config.SKIP_USED_POSTS else set()

    for subreddit_name in config.SUBREDDITS:
        url = (
            f"https://www.reddit.com/r/{subreddit_name}/top.json"
            f"?t={config.TIME_FILTER}&limit={config.POST_LIMIT}"
        )
        try:
            resp = requests.get(url, headers=REDDIT_JSON_HEADERS, timeout=12)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"  [WARN] Reddit JSON API failed for r/{subreddit_name}: {e}")
            continue

        posts = data.get("data", {}).get("children", [])
        for child in posts:
            post = child.get("data", {})
            post_id = post.get("id", "")

            if post_id in used:
                continue
            if post.get("score", 0) < config.MIN_POST_SCORE:
                continue
            if post.get("is_self") is False:
                continue  # link post — no body

            body = (post.get("selftext") or "").strip()
            if body in ("[removed]", "[deleted]", ""):
                continue
            if len(body.split()) < min_words:
                continue

            title = (post.get("title") or "").strip()
            if not _passes_story_filter(title, body):
                continue

            return {
                "id": post_id,
                "title": title,
                "body": body,
                "score": post.get("score", 0),
                "subreddit": subreddit_name,
                "url": f"https://reddit.com{post.get('permalink', '')}",
                "author": post.get("author", "unknown"),
            }

    return None


HN_API = "https://hacker-news.firebaseio.com/v0"
HN_HEADERS = {"User-Agent": "StoryEngine/1.0"}

# Keywords that signal a personal/narrative story worth narrating
HN_STORY_KEYWORDS = [
    "ask hn", "i quit", "i was fired", "i got fired", "i left",
    "my story", "confession", "i built", "show hn", "lessons learned",
    "what i learned", "failed", "scammed", "divorced", "burnout",
    "left my job", "moved abroad", "quit my job", "changed my life",
    "how i", "why i", "i realized", "my experience", "i decided",
    "after years", "my journey", "i survived", "we almost", "i lost",
]


def _strip_hn_html(html: str) -> str:
    text = re.sub(r"<p>", "\n\n", html, flags=re.IGNORECASE)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = html_module.unescape(text)
    return text.strip()


def _is_story_content(title: str, body: str) -> bool:
    combined = (title + " " + body[:500]).lower()
    return any(kw in combined for kw in HN_STORY_KEYWORDS)


def _fetch_hn_item(item_id: int) -> dict | None:
    try:
        resp = requests.get(f"{HN_API}/item/{item_id}.json", headers=HN_HEADERS, timeout=8)
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return None


def get_hn_post(min_score: int = 50, min_words: int = 150, max_check: int = 200) -> dict | None:
    """
    Fetches the best narrative story from Hacker News.
    No API key required. Returns same dict shape as get_post().
    """
    used = _load_used_posts() if config.SKIP_USED_POSTS else set()

    for feed in ["askstories", "topstories", "showstories"]:
        try:
            resp = requests.get(f"{HN_API}/{feed}.json", headers=HN_HEADERS, timeout=10)
            resp.raise_for_status()
            story_ids = resp.json()[:max_check]
        except Exception as e:
            print(f"  [WARN] HN feed '{feed}' failed: {e}")
            continue

        for story_id in story_ids:
            if str(story_id) in used:
                continue

            item = _fetch_hn_item(story_id)
            if not item or item.get("deleted") or item.get("dead"):
                continue
            if item.get("type") not in ("story", "ask"):
                continue
            if item.get("score", 0) < min_score:
                continue

            title = (item.get("title") or "").strip()
            raw_text = item.get("text") or ""
            body = _strip_hn_html(raw_text) if raw_text else ""

            # If no body text, try building from top comments
            if len(body.split()) < min_words:
                kids = (item.get("kids") or [])[:10]
                comments = []
                for kid_id in kids:
                    kid = _fetch_hn_item(kid_id)
                    if not kid or kid.get("deleted") or kid.get("dead"):
                        continue
                    kid_text = _strip_hn_html(kid.get("text") or "")
                    if len(kid_text.split()) > 20:
                        comments.append(kid_text)
                if len(comments) >= 3:
                    body = "\n\n".join(comments)

            if len(body.split()) < min_words:
                continue
            if not _is_story_content(title, body):
                continue

            return {
                "id": str(story_id),
                "title": title,
                "body": body,
                "score": item.get("score", 0),
                "subreddit": "HackerNews",
                "url": item.get("url") or f"https://news.ycombinator.com/item?id={story_id}",
                "author": item.get("by", "unknown"),
            }

    return None


CHAN_API = "https://a.4cdn.org"
CHAN_HEADERS = {"User-Agent": "StoryEngine/1.0"}

CHAN_BOARDS = ["r9k", "adv", "fit", "biz", "x"]

CHAN_STORY_SIGNALS = [
    "story time", "storytime", "true story", "happened to me",
    "my ex", "my girlfriend", "my boyfriend", "my wife", "my husband",
    "my mom", "my dad", "my boss", "i was", "i got", "i quit",
    "i lost", "i found", "i fucked up", "confession", "vent",
    "need advice", "advice needed", "am i wrong", "relationship",
    "cheating", "revenge", "fired", "debt", "family",
]


def _clean_4chan_post(html_text: str) -> str:
    if not html_text:
        return ""
    text = re.sub(r"<br\s*/?>", "\n", html_text, flags=re.IGNORECASE)
    text = re.sub(r'<span class="quote">', "", text)
    text = re.sub(r'<a[^>]*>&gt;&gt;\d+</a>', "", text)
    text = re.sub(r"<[^>]+>", "", text)
    text = html_module.unescape(text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def get_4chan_post(min_replies: int = 10, min_words: int = 100) -> dict | None:
    """
    Fetches a story-rich thread from 4chan. No API key required.
    Returns same dict shape as get_post().
    """
    used = _load_used_posts() if config.SKIP_USED_POSTS else set()

    import random
    boards = CHAN_BOARDS.copy()
    random.shuffle(boards)  # randomize board order each run

    for board in boards:
        try:
            resp = requests.get(f"{CHAN_API}/{board}/threads.json", headers=CHAN_HEADERS, timeout=10)
            resp.raise_for_status()
        except Exception as e:
            print(f"  [WARN] 4chan /{board}/ failed: {e}")
            continue

        # Flatten and sort by reply count
        all_threads = []
        for page in resp.json():
            for t in page.get("threads", []):
                all_threads.append(t)
        all_threads.sort(key=lambda t: t.get("replies", 0), reverse=True)

        for thread_meta in all_threads[:30]:
            thread_no = thread_meta.get("no")
            if not thread_no or str(f"4chan_{board}_{thread_no}") in used:
                continue
            if thread_meta.get("replies", 0) < min_replies:
                continue

            try:
                import time as _time
                resp = requests.get(
                    f"{CHAN_API}/{board}/thread/{thread_no}.json",
                    headers=CHAN_HEADERS,
                    timeout=10,
                )
                if resp.status_code == 404:
                    continue
                resp.raise_for_status()
                _time.sleep(0.5)
            except Exception:
                continue

            posts = resp.json().get("posts", [])
            if not posts:
                continue

            op = posts[0]
            if op.get("sticky") or op.get("closed"):
                continue

            op_text = _clean_4chan_post(op.get("com", ""))
            op_subject = _clean_4chan_post(op.get("sub", "")).strip()

            if len(op_text.split()) < 50:
                continue

            combined = (op_subject + " " + op_text).lower()
            if not any(sig in combined for sig in CHAN_STORY_SIGNALS):
                continue

            # Build title
            if op_subject and len(op_subject) > 10:
                title = op_subject[:200]
            else:
                first_sent = re.split(r'(?<=[.!?])\s', op_text)[0]
                title = first_sent[:200] if first_sent else op_text[:100]

            # Build body: OP + top replies
            body_parts = [op_text]
            for post in posts[1:21]:
                post_text = _clean_4chan_post(post.get("com", ""))
                if not post_text or len(post_text.split()) < 10:
                    continue
                if post_text.startswith(">") and len(post_text.split("\n")) <= 2:
                    continue
                body_parts.append(post_text)
                if len(body_parts) >= 11:
                    break

            body = "\n\n".join(body_parts)
            if len(body.split()) < min_words:
                continue

            return {
                "id": f"4chan_{board}_{thread_no}",
                "title": title,
                "body": body,
                "score": thread_meta.get("replies", 0),
                "subreddit": f"4chan/{board}",
                "url": f"https://boards.4channel.org/{board}/thread/{thread_no}",
                "author": "Anonymous",
            }

    return None


def _load_used_posts():
    if not os.path.exists("used_posts.txt"):
        return set()
    with open("used_posts.txt", "r") as f:
        return set(line.strip() for line in f if line.strip())


def get_reddit_client():
    return praw.Reddit(
        client_id=os.getenv("REDDIT_CLIENT_ID"),
        client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
        user_agent=os.getenv("REDDIT_USER_AGENT", "RedditYouTubeEmpire/1.0"),
    )


def get_post():
    """
    Finds the best unused post across all configured subreddits.
    Returns a dict with post data, or None if nothing found.
    """
    reddit = get_reddit_client()
    used = _load_used_posts() if config.SKIP_USED_POSTS else set()

    for subreddit_name in config.SUBREDDITS:
        subreddit = reddit.subreddit(subreddit_name)

        for post in subreddit.top(time_filter=config.TIME_FILTER, limit=config.POST_LIMIT):
            # Skip already used posts
            if post.id in used:
                continue

            # Skip posts below score threshold
            if post.score < config.MIN_POST_SCORE:
                continue

            # Skip posts without body text (link posts, images, etc.)
            if not post.selftext or post.selftext.strip() in ("[removed]", "[deleted]", ""):
                continue

            # Skip very short posts
            if len(post.selftext.split()) < 100:
                continue

            return {
                "id": post.id,
                "title": post.title,
                "body": post.selftext,
                "score": post.score,
                "subreddit": subreddit_name,
                "url": f"https://reddit.com{post.permalink}",
                "author": str(post.author),
            }

    return None
