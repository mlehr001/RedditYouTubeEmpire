"""
RedditYouTubeEmpire — Full Pipeline (Video 2)
Reddit personal stories → Conversational script → ElevenLabs TTS → Keyword b-roll → Edit → Upload
"""

import os
import sys
from dotenv import load_dotenv

load_dotenv()

from modules.scraper import get_reddit_json_post, get_post, get_hn_post, get_4chan_post
from modules.script_writer import build_script, build_commentary_script
from modules.tts import generate_audio
from modules.broll import get_clips_for_keywords, get_clips_for_beats
from modules.editor import create_video, create_video_from_beats
from modules.uploader import upload_to_youtube
from modules.angle_selector import build_topic_summary, generate_angles, prompt_angle_selection
from modules.hook_generator import (
    generate_hooks,
    prompt_hook_selection,
    prepend_hook,
    store_hooks,
    log_hook,
    query_hook_performance,
)
from modules.title_generator import (
    generate_titles,
    prompt_title_selection,
    store_titles,
    log_title,
    query_title_performance,
)
from modules.beat_mapper import (
    generate_beats,
    store_beats,
    log_beats,
    query_beat_performance,
)
import config
import json


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

    # 2. Generate commentary angles (AI-assist — shapes hook/tone, never alters story)
    selected_angle = None
    angles_result = None
    print("\n[ANGLES] Generating commentary angles...")
    try:
        topic_summary = build_topic_summary(post)
        angles_result = generate_angles(topic_summary)
        chosen_idx = prompt_angle_selection(angles_result)
        selected_angle = angles_result["angles"][chosen_idx]
        _store_angles(post["id"], angles_result)
    except Exception as e:
        print(f"[WARN] Angle generation failed ({e}) — continuing without angle.")

    # 3. Build script — route by source type
    source_type = post.get("source_type", "reddit_story")
    print("\n[SCRIPT] Writing script...")
    if source_type == "reddit_story":
        print(f"[SCRIPT] Using build_script (verbatim story, source: {source_type})")
        result = build_script(post, angle=selected_angle)
    else:
        print(f"[SCRIPT] Using build_commentary_script (source: {source_type})")
        result = build_commentary_script(post, angle=selected_angle)
    script = result["script"]
    keywords = result["keywords"]
    titles = result["titles"]
    word_count = len(script.split())
    print(f"[OK] Script ready ({word_count} words)")
    print(f"     Keywords: {', '.join(keywords)}")

    # 4. Generate hooks and prepend selected hook to script
    print("\n[HOOKS] Generating hooks...")
    hooks_result = generate_hooks(script)
    chosen_hook_idx = prompt_hook_selection(hooks_result)
    hooks_result["selected"] = chosen_hook_idx
    selected_hook = hooks_result["hooks"][chosen_hook_idx]
    script = prepend_hook(script, selected_hook["text"])
    store_hooks(post["id"], hooks_result)
    log_hook(post["id"], selected_hook)
    query_hook_performance()

    # 5. Generate titles and select final title
    print("\n[TITLES] Generating titles...")
    titles_result = generate_titles(script)
    chosen_title_idx = prompt_title_selection(titles_result)
    titles_result["selected"] = chosen_title_idx
    selected_title = titles_result["titles"][chosen_title_idx]
    final_title = selected_title["text"]
    store_titles(post["id"], titles_result)
    log_title(post["id"], selected_title)
    query_title_performance()

    # 6. Generate visual beat map
    print("\n[BEATS] Mapping script to visual beats...")
    beats_result = generate_beats(script)
    beats = beats_result["beats"]
    store_beats(post["id"], beats_result)
    log_beats(post["id"], beats)
    query_beat_performance()
    print(f"[OK] {len(beats)} beats mapped ({beats_result['total_duration']}s total)")
    for i, b in enumerate(beats, 1):
        print(f"     {i:2d}. [{b['emotion']:12s}] {b['name']} — {b['visual_direction'][:60]}")

    # 7. Generate TTS audio
    print(f"\n[TTS] Generating audio ({config.TTS_ENGINE})...")
    audio_path = generate_audio(script, post["id"])
    print(f"[OK] Audio saved: {audio_path}")

    # 8. Fetch one unique Pexels clip per beat
    print(f"\n[BROLL] Fetching beat-mapped clips ({len(beats)} beats)...")
    beat_clips = get_clips_for_beats(beats)
    print(f"[OK] {len(beat_clips)} clips ready")

    # 9. Edit video — beat-timed Ken Burns segments
    print("\n[EDIT] Editing video (beat-mapped)...")
    video_path = create_video_from_beats(audio_path, beat_clips, post)
    print(f"[OK] Video saved: {video_path}")

    # 10. Upload to YouTube with selected title
    print("\n[UPLOAD] Uploading to YouTube...")
    video_url = upload_to_youtube(video_path, post, final_title=final_title)
    if video_url:
        print(f"[OK] Uploaded: {video_url}")
        print(f"     Title used: {final_title}")
    else:
        print("[WARN] Upload skipped or failed. Video saved locally.")

    # 11. Mark post as used
    _mark_post_used(post["id"])

    print(f"\n[DONE] Video: {video_path}\n")


def _store_angles(post_id: str, angles_result: dict) -> None:
    """
    Persists the angles_json for this post to a sidecar file in output/.
    When a scripts DB table is active, insert angles_result as angles_json
    keyed by post_id. Stored separately from story content (CLAUDE.md rule #4).
    """
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    path = os.path.join(config.OUTPUT_DIR, f"{post_id}_angles.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(angles_result, f, indent=2, ensure_ascii=False)


def _mark_post_used(post_id):
    with open("used_posts.txt", "a") as f:
        f.write(post_id + "\n")


if __name__ == "__main__":
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    os.makedirs(config.ASSETS_DIR, exist_ok=True)
    run()
