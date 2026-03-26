"""
pipelines/mystery.py — CH2: Mystery Top 5 countdown pipeline.
"""

import os
import sys
from collections import deque

import config
from pipelines.shared import _store_json, _mark_post_used

from modules.tts import generate_audio
from modules.uploader import upload_to_youtube
from modules.angle_selector import generate_angles, prompt_angle_selection
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
from modules.mystery_scraper import get_mystery_topic
from modules.media_fetcher import fetch_media_for_topic
from modules.script_writer import build_mystery_top5_script
from modules.script_reviewer import review_script
from modules.number_frames import generate_all_cards
from modules.music_manager import get_music_for_category
from modules.broll import get_clips_for_beats
from modules.editor import create_mystery_video

def select_theme():
    # TEMP: hardcoded for now (we automate later)
    return "alien sightings"

def _attach_media_items(beats: list, topic: dict) -> list:
    """
    For every beat with visual_source == "real_media", find the matching
    media_item from topic entries and attach it to beat["media_item"].

    Matching logic:
      - script_position "entry_N" → entry_number N
      - beat name "real_photo"   → media item type "photo"
      - beat name "real_video"   → media item type "video"
      - beat name "real_audio"   → media item type "audio"

    Degrades gracefully: if no matching media item is found, the beat's
    visual_source is reset to "broll" so it gets a Pexels clip instead.
    """
    entries = topic.get("entries", [])

    # Build lookup: entry_number → [media_items]
    entry_media: dict[int, list] = {}
    for entry in entries:
        n = entry.get("entry_number", 0)
        entry_media[n] = entry.get("media_items", [])

    for beat in beats:
        if beat.get("visual_source") != "real_media":
            continue

        # Derive entry number from script_position (e.g. "entry_3" → 3)
        pos          = beat.get("script_position", "")
        entry_number = 0
        for n in range(1, 6):
            if f"entry_{n}" in pos:
                entry_number = n
                break

        beat_name    = beat.get("name", "").lower()
        desired_type = None
        if "real_photo" in beat_name:
            desired_type = "photo"
        elif "real_video" in beat_name:
            desired_type = "video"
        elif "real_audio" in beat_name:
            desired_type = "audio"

        candidates = entry_media.get(entry_number, [])
        if not candidates:
            # No entry found — search all entries as fallback
            candidates = [m for items in entry_media.values() for m in items]

        if desired_type:
            typed = [m for m in candidates if m.get("type") == desired_type]
            match = typed[0] if typed else (candidates[0] if candidates else None)
        else:
            match = candidates[0] if candidates else None

        if match:
            beat["media_item"] = match
            # Populate caption_text for audio beats if transcript available
            if beat_name == "real_audio" and not beat.get("caption_text"):
                transcript = match.get("transcript", "")
                credit     = match.get("credit", "Audio Recording")
                beat["caption_text"] = (
                    transcript[:300] if transcript
                    else f"[ Audio Recording ]\n{credit}"
                )
            print(f"  [MEDIA ATTACH] Beat '{beat.get('name')}' "
                  f"(entry {entry_number}) → {match['type']} — {match['credit'][:50]}")
        else:
            # No media available — degrade to broll
            beat["visual_source"]    = "broll"
            beat["narration_active"] = True
            beat["music_active"]     = True
            beat["music_volume"]     = 0.10
            print(f"  [MEDIA ATTACH] Beat '{beat.get('name')}' — no media found, "
                  f"degraded to broll")

    return beats


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


def run_mystery():
    theme = select_theme()
    print(f"[CH2] Category: {theme}")

    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    os.makedirs(config.MYSTERY_FRAMES_DIR, exist_ok=True)
    os.makedirs(config.MYSTERY_MUSIC_DIR, exist_ok=True)

    while True:
        # 1. Mystery scraper — get topic + entries
        print("[1/13] SCRAPE — Mystery topic + entries")
        topic = get_mystery_topic(theme)
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
        script_result = build_mystery_top5_script(
            topic,
            top5,
            theme=theme,
            angle=selected_angle
        )   
        script = script_result["script"]
        script_entries = script_result.get("entries", [])
        keywords = script_result.get("keywords", ["mystery", "dark", "eerie"])
        word_count = len(script.split())
        print(f"[OK] Script: {word_count} words, {len(script_entries)} sections")
        _store_json(topic_id, "script", script_result)

        # 4b. Script review
        print("\n[REVIEW] Opening script for review...")
        script, review_action = review_script(script, topic_id)
        if review_action == "rejected":
            continue  # restart — fetch a new topic

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

        # Review & approve beats before fetching any clips
        beats_result = review_and_approve_beats(topic_id, beats_result)
        beats = beats_result["beats"]

        # 7b. Attach real media items to real_media beats
        print("\n[7b] MEDIA ATTACH — Linking real media items to beats")
        beats = _attach_media_items(beats, topic)
        beats_result["beats"] = beats
        real_media_count = sum(
            1 for b in beats if b.get("visual_source") == "real_media"
        )
        print(f"[OK] {real_media_count} real_media beat(s) wired to media items")

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
        music_path = get_music_for_category(theme))
        if music_path:
            print(f"[OK] Music: {os.path.basename(music_path)}")
        else:
            print("[OK] No music — narration-only audio")

        # 10. TTS — ElevenLabs narration
        print(f"\n[10/13] TTS — Generate narration ({config.TTS_ENGINE})")
        audio_path = generate_audio(script, topic_id)
        print(f"[OK] Audio: {audio_path}")

        # 11. B-roll — only for broll beats; real_media beats skip Pexels
        broll_beats = [b for b in beats if b.get("visual_source", "broll") == "broll"]
        print(f"\n[11/13] BROLL — Fetch Pexels clips "
              f"({len(broll_beats)} broll beats / {len(beats)} total)")

        if broll_beats:
            enhanced_broll = _inject_mystery_keywords(broll_beats, keywords)
            broll_clips    = get_clips_for_beats(enhanced_broll, video_id=topic_id)

            # Stitch paths back to the original broll beats (order-preserved)
            clip_queue = deque(broll_clips)
            for beat in beats:
                if beat.get("visual_source", "broll") == "broll":
                    clip = clip_queue.popleft() if clip_queue else None
                    if clip:
                        beat["path"] = clip["path"]

            print(f"[OK] {len(broll_clips)} broll clips fetched and attached")
        else:
            print("[OK] No broll beats — all real media")

        # Pass the full beats list to the editor (broll beats have path,
        # real_media beats have media_item)
        beat_clips = beats

        # 12. Create mystery video — assemble with music
        print("\n[12/13] EDIT — Assemble mystery video")
        post = {
            "id":          topic_id,
            "title":       final_title,
            "subreddit":   "mystery",
            "score":       0,
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
        break  # pipeline complete — exit retry loop
