"""
pipelines/story.py — CH1: Reddit personal story pipeline.
"""

import os
import sys

import config
from pipelines.shared import _store_json, _mark_post_used

from modules.tts import generate_audio
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
    review_and_approve_beats,
)
from modules.scraper import get_reddit_json_post, get_post, get_hn_post, get_4chan_post
from modules.script_writer import build_script, build_commentary_script
from modules.script_reviewer import review_script
from modules.broll import get_clips_for_keywords, get_clips_for_beats
from modules.editor import create_video, create_video_from_beats


def _store_angles(post_id: str, angles_result: dict) -> None:
    """Persist angles JSON for a post to output/ (CLAUDE.md rule #4)."""
    _store_json(post_id, "angles", angles_result)


def run_story():
    while True:
        # 1. Scrape
        post = None
        source_label = None

        print("[SCRAPE] Scraping Reddit personal stories (public API)...")
        post = get_reddit_json_post()
        if post:
            source_label = f"r/{post['subreddit']}"
        else:
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

        # 2. Angles
        selected_angle = None
        print("\n[ANGLES] Generating commentary angles...")
        try:
            topic_summary = build_topic_summary(post)
            angles_result = generate_angles(topic_summary)
            chosen_idx = prompt_angle_selection(angles_result)
            selected_angle = angles_result["angles"][chosen_idx]
            _store_angles(post["id"], angles_result)
        except Exception as e:
            print(f"[WARN] Angle generation failed ({e}) — continuing without angle.")

        # 3. Script
        source_type = post.get("source_type", "reddit_story")
        print("\n[SCRIPT] Writing script...")
        if source_type == "reddit_story":
            result = build_script(post, angle=selected_angle)
        else:
            result = build_commentary_script(post, angle=selected_angle)
        script = result["script"]
        keywords = result["keywords"]
        word_count = len(script.split())
        print(f"[OK] Script ready ({word_count} words) | Keywords: {', '.join(keywords)}")

        # 3b. Script review
        print("\n[REVIEW] Opening script for review...")
        script, review_action = review_script(script, post["id"])
        if review_action == "rejected":
            continue  # restart — fetch a new story

        # 4. Hooks
        print("\n[HOOKS] Generating hooks...")
        hooks_result = generate_hooks(script)
        chosen_hook_idx = prompt_hook_selection(hooks_result)
        hooks_result["selected"] = chosen_hook_idx
        selected_hook = hooks_result["hooks"][chosen_hook_idx]
        script = prepend_hook(script, selected_hook["text"])
        store_hooks(post["id"], hooks_result)
        log_hook(post["id"], selected_hook)
        query_hook_performance()

        # 5. Titles
        print("\n[TITLES] Generating titles...")
        titles_result = generate_titles(script)
        chosen_title_idx = prompt_title_selection(titles_result)
        titles_result["selected"] = chosen_title_idx
        selected_title = titles_result["titles"][chosen_title_idx]
        final_title = selected_title["text"]
        store_titles(post["id"], titles_result)
        log_title(post["id"], selected_title)
        query_title_performance()

        # 6. Beats
        print("\n[BEATS] Mapping script to visual beats...")
        beats_result = generate_beats(script)
        beats = beats_result["beats"]
        store_beats(post["id"], beats_result)
        log_beats(post["id"], beats)
        query_beat_performance()
        print(f"[OK] {len(beats)} beats mapped ({beats_result['total_duration']}s total)")

        # Review & approve beats before fetching any clips
        beats_result = review_and_approve_beats(post["id"], beats_result)
        beats = beats_result["beats"]

        # 7. TTS
        print(f"\n[TTS] Generating audio ({config.TTS_ENGINE})...")
        audio_path = generate_audio(script, post["id"])
        print(f"[OK] Audio saved: {audio_path}")

        # 8. B-roll — only after beat approval
        print(f"\n[BROLL] Fetching beat-mapped clips ({len(beats)} beats)...")
        beat_clips = get_clips_for_beats(beats)
        print(f"[OK] {len(beat_clips)} clips ready")

        # 9. Edit
        print("\n[EDIT] Editing video (beat-mapped)...")
        video_path = create_video_from_beats(audio_path, beat_clips, post)
        print(f"[OK] Video saved: {video_path}")

        # 10. Upload
        print("\n[UPLOAD] Uploading to YouTube...")
        video_url = upload_to_youtube(video_path, post, final_title=final_title)
        if video_url:
            print(f"[OK] Uploaded: {video_url}")
            print(f"     Title used: {final_title}")
        else:
            print("[WARN] Upload skipped or failed. Video saved locally.")

        # 11. Mark used
        _mark_post_used(post["id"])
        print(f"\n[CH1 DONE] Video: {video_path}\n")
        break  # pipeline complete — exit retry loop
