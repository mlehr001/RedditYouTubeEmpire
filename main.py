"""
RedditYouTubeEmpire — Full Pipeline (Video 2)
Reddit personal stories → Conversational script → ElevenLabs TTS → Keyword b-roll → Edit → Upload
"""

import os
import sys
from dotenv import load_dotenv

load_dotenv()

from modules.scraper import get_reddit_json_post, get_post, get_hn_post, get_4chan_post
from modules.script_writer import build_script
from modules.tts import generate_audio
from modules.broll import get_clips_for_keywords
from modules.editor import create_video
from modules.uploader import upload_to_youtube
import config


def run():
    print("\n[START] RedditYouTubeEmpire starting...\n")

    # 1. Scrape — Reddit personal stories (public JSON, no creds needed)
    #    Fall back to PRAW -> HN -> 4chan
    post = None
    source_label = None

    print("[SCRAPE] Scraping Reddit personal stories (public API)...")
    post = get_reddit_json_post()
    if post:
        source_label = f"r/{post['subreddit']}"
    else:
        # Try PRAW if credentials are configured
        reddit_ready = bool(os.getenv("REDDIT_CLIENT_ID") and os.getenv("REDDIT_CLIENT_SECRET"))
        if reddit_ready:
            print("  -> Public API failed -- trying PRAW...")
            post = get_post()
            if post:
                source_label = f"r/{post['subreddit']} (PRAW)"

    if not post:
        print("  -> Reddit unavailable -- trying Hacker News...")
        post = get_hn_post()
        source_label = "Hacker News"

    if not post:
        print("  -> HN failed -- trying 4chan...")
        post = get_4chan_post()
        source_label = "4chan"

    if not post:
        print("[ERROR] No suitable post found. Try again later.")
        sys.exit(1)

    print(f"[OK] Found: '{post['title'][:80]}' ({source_label}, score: {post['score']})")

    # 2. Build script (AI-assisted: conversational framing + keywords + titles)
    print("\n[SCRIPT] Writing script...")
    result = build_script(post)
    script = result["script"]
    keywords = result["keywords"]
    titles = result["titles"]
    word_count = len(script.split())
    print(f"[OK] Script ready ({word_count} words)")
    print(f"     Keywords: {', '.join(keywords)}")

    # 3. Generate TTS audio
    print(f"\n[TTS] Generating audio ({config.TTS_ENGINE})...")
    audio_path = generate_audio(script, post["id"])
    print(f"[OK] Audio saved: {audio_path}")

    # 4. Fetch one b-roll clip per keyword
    print(f"\n[BROLL] Fetching clips for {len(keywords)} keywords...")
    clip_paths = get_clips_for_keywords(keywords)
    print(f"[OK] {len(clip_paths)} clips ready")

    # 5. Edit video — Ken Burns + clip-per-keyword switching
    print("\n[EDIT] Editing video...")
    video_path = create_video(audio_path, clip_paths, post)
    print(f"[OK] Video saved: {video_path}")

    # 6. Upload to YouTube
    print("\n[UPLOAD] Uploading to YouTube...")
    video_url = upload_to_youtube(video_path, post)
    if video_url:
        print(f"[OK] Uploaded: {video_url}")
    else:
        print("[WARN] Upload skipped or failed. Video saved locally.")

    # 7. Mark post as used
    _mark_post_used(post["id"])

    # 8. Print title options
    print(f"\n{'-' * 60}")
    print("TITLE OPTIONS -- pick the best one:")
    for i, title in enumerate(titles, 1):
        print(f"  {i}. {title}")
    print(f"{'-' * 60}")
    print(f"\n[DONE] Video: {video_path}\n")


def _mark_post_used(post_id):
    with open("used_posts.txt", "a") as f:
        f.write(post_id + "\n")


if __name__ == "__main__":
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    os.makedirs(config.ASSETS_DIR, exist_ok=True)
    run()
