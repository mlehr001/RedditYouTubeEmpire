"""
scraper.py — Pulls top posts from configured subreddits using PRAW
"""

import os
import praw
import config


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
