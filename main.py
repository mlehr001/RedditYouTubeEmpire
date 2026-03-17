"""
RedditYouTubeEmpire — Full Pipeline
Scrape Reddit → Write Script → TTS → B-Roll → Edit → Upload to YouTube
"""

import os
import sys
from dotenv import load_dotenv

load_dotenv()

from modules.scraper import get_post
from modules.script_writer import build_script
from modules.tts import generate_audio
from modules.broll import get_background_clip
from modules.editor import create_video
from modules.uploader import upload_to_youtube
import config

def run():
    print("\n🚀 RedditYouTubeEmpire starting...\n")

    # 1. Scrape Reddit
    print("📡 Scraping Reddit...")
    post = get_post()
    if not post:
        print("❌ No suitable post found. Try again later or adjust config.py settings.")
        sys.exit(1)
    print(f"✅ Found post: '{post['title']}' (r/{post['subreddit']}, {post['score']} upvotes)")

    # 2. Build TTS script
    print("\n✍️  Writing script...")
    script = build_script(post)
    word_count = len(script.split())
    print(f"✅ Script ready ({word_count} words)")

    # 3. Generate TTS audio
    print(f"\n🎙️  Generating TTS audio ({config.TTS_ENGINE})...")
    audio_path = generate_audio(script, post['id'])
    print(f"✅ Audio saved: {audio_path}")

    # 4. Get background clip
    print("\n🎬 Fetching background video...")
    broll_path = get_background_clip()
    print(f"✅ Background clip ready: {broll_path}")

    # 5. Edit video
    print("\n🎞️  Editing video...")
    video_path = create_video(audio_path, broll_path, post)
    print(f"✅ Video saved: {video_path}")

    # 6. Upload to YouTube
    print("\n📤 Uploading to YouTube...")
    video_url = upload_to_youtube(video_path, post)
    if video_url:
        print(f"✅ Uploaded! {video_url}")
    else:
        print("⚠️  Upload skipped or failed. Video is saved locally.")

    # 7. Mark post as used
    _mark_post_used(post['id'])

    print(f"\n🎉 Done! Video: {video_path}\n")


def _mark_post_used(post_id):
    with open("used_posts.txt", "a") as f:
        f.write(post_id + "\n")


if __name__ == "__main__":
    # Create output dirs if they don't exist
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    os.makedirs(config.ASSETS_DIR, exist_ok=True)
    run()
