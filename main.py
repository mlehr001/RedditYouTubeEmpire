"""
RedditYouTubeEmpire — Full Pipeline

CH1 (CHANNEL_TYPE=story):  Reddit personal stories → script → TTS → b-roll → edit → upload
CH2 (CHANNEL_TYPE=mystery): Mystery Top 5 countdown → script → TTS → number cards → edit → upload
"""

import os
import sys
import json
from dotenv import load_dotenv

load_dotenv()

import config

# ─── Shared imports ───────────────────────────────────────────────────────────
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
)

# ─── CH1 imports ──────────────────────────────────────────────────────────────
from modules.scraper import get_reddit_json_post, get_post, get_hn_post, get_4chan_post
from modules.script_writer import build_script, build_commentary_script
from modules.broll import get_clips_for_keywords, get_clips_for_beats
from modules.editor import create_video, create_video_from_beats

# ─── CH2 imports ──────────────────────────────────────────────────────────────
from modules.mystery_scraper import get_mystery_topic
from modules.media_fetcher import fetch_media_for_topic
from modules.script_writer import build_mystery_top5_script
from modules.number_frames import generate_all_cards
from modules.music_manager import get_music_for_category
from modules.editor import create_mystery_video


# ─── Entry point ──────────────────────────────────────────────────────────────

def run():
    channel = config.CHANNEL_TYPE

    if channel == "mystery":
        print("\n[CH2 MYSTERY] Mystery Top 5 pipeline starting...\n")
        run_mystery()
    else:
        print("\n[CH1 STORY] Reddit story pipeline starting...\n")
        run_story()


# ─── CH1: Reddit Story Pipeline ───────────────────────────────────────────────

def run_story():
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
    for i, b in enumerate(beats, 1):
        print(f"     {i:2d}. [{b['emotion']:12s}] {b['name']} — {b['visual_direction'][:60]}")

    # 7. TTS
    print(f"\n[TTS] Generating audio ({config.TTS_ENGINE})...")
    audio_path = generate_audio(script, post["id"])
    print(f"[OK] Audio saved: {audio_path}")

    # 8. B-roll
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


# ─── CH2: Mystery Top 5 Pipeline ──────────────────────────────────────────────

def run_mystery():
    category = config.MYSTERY_CATEGORY
    print(f"[CH2] Category: {category}\n")

    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    os.makedirs(config.MYSTERY_FRAMES_DIR, exist_ok=True)
    os.makedirs(config.MYSTERY_MUSIC_DIR, exist_ok=True)

    # 1. Mystery scraper — get topic + entries
    print("[1/13] SCRAPE — Mystery topic + entries")
    topic = get_mystery_topic(category)
    entries = topic["entries"]
    topic_id = topic["topic_id"]
    print(f"[OK] Topic: '{topic['title']}' — {len(entries)} entries")
    _store_json(topic_id, "topic", topic)

    if len(entries) < 5:
        print(f"[ERROR] Need at least 5 entries, got {len(entries)}. Try again later.")
        sys.exit(1)

    top5 = entries[:5]

    # 2. Media fetcher — get real footage per entry
    print("\n[2/13] MEDIA — Fetch real footage per entry")
    topic = fetch_media_for_topic(topic)
    _store_json(topic_id, "media", topic)

    # 3. Angle selector — 3 angles (Claude)
    selected_angle = None
    print("\n[3/13] ANGLES — Generate 3 commentary angles (Claude)")
    try:
        topic_summary = f"{topic['title']} — Top 5 mystery countdown featuring: " + \
                        ", ".join(e["title"] for e in top5[:3])
        angles_result = generate_angles(topic_summary)
        chosen_idx = prompt_angle_selection(angles_result)
        selected_angle = angles_result["angles"][chosen_idx]
        _store_json(topic_id, "angles", angles_result)
    except Exception as e:
        print(f"[WARN] Angle generation failed ({e}) — continuing without angle.")

    # 4. Mystery script — write Top 5 script (Claude)
    print("\n[4/13] SCRIPT — Write mystery Top 5 script (Claude)")
    script_result = build_mystery_top5_script(topic, top5, angle=selected_angle)
    script = script_result["script"]
    script_entries = script_result.get("entries", [])
    keywords = script_result.get("keywords", ["mystery", "dark", "eerie"])
    word_count = len(script.split())
    print(f"[OK] Script: {word_count} words, {len(script_entries)} sections")
    _store_json(topic_id, "script", script_result)

    # 5. Hook generator — 5 hooks (Claude)
    print("\n[5/13] HOOKS — Generate hooks (Claude)")
    hooks_result = generate_hooks(script)
    chosen_hook_idx = prompt_hook_selection(hooks_result)
    hooks_result["selected"] = chosen_hook_idx
    selected_hook = hooks_result["hooks"][chosen_hook_idx]
    script = prepend_hook(script, selected_hook["text"])
    store_hooks(topic_id, hooks_result)
    log_hook(topic_id, selected_hook)
    query_hook_performance()

    # 6. Title generator — 5 titles (OpenAI)
    print("\n[6/13] TITLES — Generate titles (OpenAI)")
    # Combine script_result titles with AI title generator output
    titles_result = generate_titles(script)
    # Prepend Claude's mystery-specific titles from script_result
    mystery_titles = [{"text": t, "style": "mystery", "type": "mystery", "score": 0}
                      for t in script_result.get("titles", [])]
    if mystery_titles:
        titles_result["titles"] = mystery_titles + titles_result.get("titles", [])

    chosen_title_idx = prompt_title_selection(titles_result)
    titles_result["selected"] = chosen_title_idx
    selected_title = titles_result["titles"][chosen_title_idx]
    final_title = selected_title["text"]
    store_titles(topic_id, titles_result)
    log_title(topic_id, selected_title)
    query_title_performance()
    print(f"[OK] Title: '{final_title}'")

    # 7. Beat mapper — map beats (Claude)
    print("\n[7/13] BEATS — Map beats (Claude)")
    beats_result = generate_beats(script)
    beats = beats_result["beats"]
    store_beats(topic_id, beats_result)
    log_beats(topic_id, beats)
    query_beat_performance()
    print(f"[OK] {len(beats)} beats mapped ({beats_result['total_duration']}s estimated)")

    # 8. Number frames — generate countdown cards
    print("\n[8/13] FRAMES — Generate countdown entry cards")
    if script_entries:
        cards_input = script_entries
    else:
        # Build from top5 entries in countdown order
        cards_input = [
            {"number": 5 - i, "title": e["title"]}
            for i, e in enumerate(top5)
        ]
    number_frame_data = generate_all_cards(cards_input)
    print(f"[OK] {len(number_frame_data)} countdown cards ready")

    # 9. Music manager — select + download track
    print("\n[9/13] MUSIC — Select background music")
    music_path = get_music_for_category(category)
    if music_path:
        print(f"[OK] Music: {os.path.basename(music_path)}")
    else:
        print("[OK] No music — narration-only audio")

    # 10. TTS — ElevenLabs narration
    print(f"\n[10/13] TTS — Generate narration ({config.TTS_ENGINE})")
    audio_path = generate_audio(script, topic_id)
    print(f"[OK] Audio: {audio_path}")

    # 11. B-roll — fetch real footage + Pexels fallback
    print(f"\n[11/13] BROLL — Fetch beat-mapped clips ({len(beats)} beats)")
    # Use mystery-themed keywords to guide Pexels search
    mystery_beats = _inject_mystery_keywords(beats, keywords)
    beat_clips = get_clips_for_beats(mystery_beats)
    print(f"[OK] {len(beat_clips)} clips ready")

    # 12. Create mystery video — assemble with music
    print("\n[12/13] EDIT — Assemble mystery video")
    # Build a synthetic post dict for editor compatibility
    post = {
        "id": topic_id,
        "title": final_title,
        "subreddit": "mystery",
        "score": 0,
        "source_type": "mystery",
    }
    video_path = create_mystery_video(
        audio_path=audio_path,
        beat_clips=beat_clips,
        post=post,
        number_frames=number_frame_data,
        music_path=music_path,
    )
    print(f"[OK] Video: {video_path}")

    # 13. Upload — YouTube with selected title
    print("\n[13/13] UPLOAD — Upload to YouTube")
    video_url = upload_to_youtube(video_path, post, final_title=final_title)
    if video_url:
        print(f"[OK] Uploaded: {video_url}")
        print(f"     Title: {final_title}")
    else:
        print("[WARN] Upload skipped or failed. Video saved locally.")

    _mark_post_used(topic_id)
    print(f"\n[CH2 MYSTERY DONE] Video: {video_path}\n")


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _inject_mystery_keywords(beats: list, extra_keywords: list) -> list:
    """
    Supplement beat keywords with mystery-themed terms so Pexels returns
    atmospheric, thematic footage instead of generic clips.
    """
    mystery_terms = ["dark", "fog", "forest", "shadow", "abandoned", "night"] + extra_keywords
    for i, beat in enumerate(beats):
        existing = beat.get("keywords", [])
        # Inject one mystery term per beat (rotating)
        injected = existing + [mystery_terms[i % len(mystery_terms)]]
        beat["keywords"] = list(dict.fromkeys(injected))[:5]  # dedupe, cap at 5
    return beats


def _store_json(topic_id: str, label: str, data: dict) -> None:
    """Save a JSON sidecar to output/ for debugging and audit trail."""
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    path = os.path.join(config.OUTPUT_DIR, f"{topic_id}_{label}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def _store_angles(post_id: str, angles_result: dict) -> None:
    """Persist angles_json for a post to output/ (CLAUDE.md rule #4)."""
    _store_json(post_id, "angles", angles_result)


def _mark_post_used(post_id: str) -> None:
    with open("used_posts.txt", "a") as f:
        f.write(post_id + "\n")


if __name__ == "__main__":
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    os.makedirs(config.ASSETS_DIR, exist_ok=True)
    run()
