"""
editor.py — Combines TTS audio with keyword-matched b-roll clips using MoviePy.
Each clip plays for 4-5 seconds with a Ken Burns slow-zoom effect.
Output: 1920x1080 MP4 at 30fps

Supports three real-media beat types alongside standard b-roll:
  real_photo — still image with Ken Burns zoom + credit caption
  real_video — embed-notice frame + credit caption (embed-only safety)
  real_audio — dark background + transcript captions + audio layer
"""

import os
import re
import random
import numpy as np
import config
from moviepy import (
    VideoFileClip,
    AudioFileClip,
    CompositeVideoClip,
    TextClip,
    ColorClip,
    concatenate_videoclips,
    concatenate_audioclips,
)


def _ken_burns(clip, zoom_ratio: float = 1.05):
    """
    Apply a slow Ken Burns zoom-in effect over the clip duration.
    Uses frame-level transform with PIL for accurate crop-to-original-size.
    """
    from PIL import Image

    w_out, h_out = clip.size  # (width, height) in MoviePy 2.x

    def zoom_frame(get_frame, t):
        img = get_frame(t)
        scale = 1.0 + (zoom_ratio - 1.0) * (t / max(clip.duration, 0.001))
        new_w = int(w_out * scale)
        new_h = int(h_out * scale)
        pil = Image.fromarray(img)
        pil = pil.resize((new_w, new_h), Image.LANCZOS)
        left = (new_w - w_out) // 2
        top = (new_h - h_out) // 2
        pil = pil.crop((left, top, left + w_out, top + h_out))
        return np.array(pil)

    return clip.transform(zoom_frame)


def _make_segment(clip_path: str, seg_duration: float) -> VideoFileClip:
    """
    Loads a clip, resizes to target resolution, loops if needed,
    trims to seg_duration, then applies Ken Burns zoom.
    """
    clip = VideoFileClip(clip_path).without_audio()
    clip = clip.resized((config.VIDEO_WIDTH, config.VIDEO_HEIGHT))

    # Loop to cover seg_duration if the clip is shorter
    if clip.duration < seg_duration:
        loops = int(seg_duration / clip.duration) + 1
        clip = concatenate_videoclips([clip] * loops)

    clip = clip.subclipped(0, seg_duration)
    clip = _ken_burns(clip, zoom_ratio=1.05)
    return clip


def _get_fallback_clips() -> list:
    """Returns cached B-roll paths from assets/broll/ for emergency fallback."""
    broll_dir = config.ASSETS_DIR
    if os.path.exists(broll_dir):
        return [os.path.join(broll_dir, f)
                for f in os.listdir(broll_dir) if f.endswith(".mp4")]
    return []


def create_caption_overlay(text: str, duration: float,
                            position: str = "center",
                            style: str = "audio"):
    """
    Create a text overlay clip.

    style="audio"  — large centered white text with black stroke, for audio/video notices
    style="credit" — small bottom-left white text for photo/video source credits
    """
    if not text or not text.strip():
        return None

    if style == "audio":
        try:
            return (
                TextClip(
                    text=text,
                    font_size=48,
                    color="white",
                    stroke_color="black",
                    stroke_width=3,
                    size=(config.VIDEO_WIDTH - 160, None),
                    method="caption",
                )
                .with_position("center")
                .with_duration(duration)
            )
        except Exception as e:
            print(f"  [WARN] Audio caption failed: {e}")
            return None

    else:  # "credit"
        try:
            return (
                TextClip(
                    text=text,
                    font_size=22,
                    color="white",
                    stroke_color="black",
                    stroke_width=1,
                    size=(config.VIDEO_WIDTH // 2, None),
                    method="caption",
                )
                .with_position(("left", config.VIDEO_HEIGHT - 60))
                .with_duration(duration)
            )
        except Exception as e:
            print(f"  [WARN] Credit caption failed: {e}")
            return None


def _make_photo_segment(media_item: dict, duration: float):
    """
    Load a still photo, apply Ken Burns zoom, overlay credit caption.
    Falls back to a dark atmospheric ColorClip if image unavailable.
    """
    from PIL import Image

    local_path = media_item.get("local_path", "")
    credit     = media_item.get("credit", "")

    # Attempt to load the image
    clip = None
    if local_path and os.path.exists(local_path):
        try:
            img    = Image.open(local_path).convert("RGB")
            # Resize to fill the frame (cover crop)
            img_w, img_h = img.size
            target_ratio = config.VIDEO_WIDTH / config.VIDEO_HEIGHT
            img_ratio    = img_w / img_h
            if img_ratio > target_ratio:
                new_h = config.VIDEO_HEIGHT
                new_w = int(new_h * img_ratio)
            else:
                new_w = config.VIDEO_WIDTH
                new_h = int(new_w / img_ratio)
            img  = img.resize((new_w, new_h), Image.LANCZOS)
            # Center crop to target size
            left = (new_w - config.VIDEO_WIDTH)  // 2
            top  = (new_h - config.VIDEO_HEIGHT) // 2
            img  = img.crop((left, top,
                              left + config.VIDEO_WIDTH,
                              top  + config.VIDEO_HEIGHT))

            from moviepy import ImageClip
            clip = ImageClip(np.array(img), duration=duration)
            clip = _ken_burns(clip, zoom_ratio=1.08)
            print(f"    [PHOTO] Loaded: {os.path.basename(local_path)}")
        except Exception as e:
            print(f"  [WARN] Photo load failed ({e}) — using dark fallback")
            clip = None

    if clip is None:
        clip = ColorClip(
            size=(config.VIDEO_WIDTH, config.VIDEO_HEIGHT),
            color=(15, 12, 20),
            duration=duration,
        )

    # Credit caption bottom-left
    layers = [clip]
    if credit:
        caption = create_caption_overlay(credit, duration,
                                          position="bottom_left",
                                          style="credit")
        if caption:
            layers.append(caption)

    return CompositeVideoClip(layers) if len(layers) > 1 else clip


def _make_video_notice_segment(beat: dict, duration: float):
    """
    Display an embed-notice frame for real_video beats.
    We never download YouTube/news footage; this frame tells the viewer
    where to find it and shows the credit.
    """
    media_item = beat.get("media_item") or {}
    credit     = media_item.get("credit", "Real Footage")
    embed_url  = media_item.get("embed_url", "")

    notice_lines = ["[ Real Footage ]", credit[:70]]
    if embed_url:
        notice_lines.append("Link in description")
    notice_text = "\n".join(notice_lines)

    bg      = ColorClip(
        size=(config.VIDEO_WIDTH, config.VIDEO_HEIGHT),
        color=(10, 10, 18),
        duration=duration,
    )
    layers  = [bg]

    main_cap = create_caption_overlay(notice_text, duration,
                                       position="center", style="audio")
    if main_cap:
        layers.append(main_cap)

    credit_cap = create_caption_overlay(credit, duration,
                                         position="bottom_left", style="credit")
    if credit_cap:
        layers.append(credit_cap)

    return CompositeVideoClip(layers)


def _make_audio_segment_visual(beat: dict, duration: float):
    """
    Build the VISUAL track for a real_audio beat (dark background + captions).
    Audio is handled separately in create_mystery_video via real_audio_inserts.
    """
    media_item  = beat.get("media_item") or {}
    credit      = media_item.get("credit", "Audio Recording")
    transcript  = media_item.get("transcript", "") or ""

    # Caption text: use transcript if available, else generic label
    if transcript.strip():
        caption_text = transcript[:300]  # clip long transcripts
    else:
        caption_text = f"[ Audio Recording ]\n{credit}"

    beat_caption = beat.get("caption_text", "")
    if beat_caption:
        caption_text = beat_caption  # manual override wins

    bg     = ColorClip(
        size=(config.VIDEO_WIDTH, config.VIDEO_HEIGHT),
        color=(8, 8, 12),
        duration=duration,
    )
    layers = [bg]

    main_cap = create_caption_overlay(caption_text, duration,
                                       position="center", style="audio")
    if main_cap:
        layers.append(main_cap)

    credit_cap = create_caption_overlay(credit, duration,
                                         position="bottom_left", style="credit")
    if credit_cap:
        layers.append(credit_cap)

    return CompositeVideoClip(layers)


def _make_real_media_segment(beat: dict, duration: float):
    """
    Dispatch to the correct real-media renderer based on media_item type.
    Falls back to a dark atmospheric ColorClip if type is unrecognised.
    """
    media_item  = beat.get("media_item") or {}
    media_type  = media_item.get("type", "")

    if media_type == "photo":
        return _make_photo_segment(media_item, duration)
    elif media_type == "video":
        return _make_video_notice_segment(beat, duration)
    elif media_type == "audio":
        return _make_audio_segment_visual(beat, duration)
    else:
        # Unknown / missing — dark fallback
        return ColorClip(
            size=(config.VIDEO_WIDTH, config.VIDEO_HEIGHT),
            color=(12, 10, 16),
            duration=duration,
        )


def _make_beat_segment(beat: dict, seg_dur: float, fallback_clips: list):
    """
    Build one video segment for any beat type.
    Routes to real-media or standard B-roll renderer.
    Falls back gracefully on any failure.
    """
    visual_source = beat.get("visual_source", "broll")

    # ── Real media ────────────────────────────────────────────────────────────
    if visual_source == "real_media" and beat.get("media_item"):
        try:
            return _make_real_media_segment(beat, seg_dur)
        except Exception as e:
            print(f"  [WARN] Real media segment failed: {e} — falling back to B-roll")

    # ── B-roll ────────────────────────────────────────────────────────────────
    path = beat.get("path", "")
    if path and os.path.exists(path):
        try:
            return _make_segment(path, seg_dur)
        except Exception as e:
            print(f"  [WARN] B-roll segment failed ({path}): {e}")

    # ── Emergency fallback: cached clip ───────────────────────────────────────
    if fallback_clips:
        try:
            return _make_segment(random.choice(fallback_clips), seg_dur)
        except Exception as e:
            print(f"  [WARN] Fallback clip failed: {e}")

    # ── Ultimate fallback: black frame ─────────────────────────────────────────
    return ColorClip(
        size=(config.VIDEO_WIDTH, config.VIDEO_HEIGHT),
        color=(0, 0, 0),
        duration=seg_dur,
    )


def _build_music_track_enveloped(music_path: str, envelope: list,
                                   total_duration: float):
    """
    Build a music AudioClip whose volume follows per-beat envelope entries.

    envelope: list of (start_t, duration, volume) tuples covering total_duration.
    Volume 0.0 = silence (for real_audio beats).
    Returns concatenated AudioClip, or None on failure.
    """
    if not envelope:
        return None

    raw       = AudioFileClip(music_path)
    raw_dur   = raw.duration
    loops     = int(total_duration / raw_dur) + 2
    extended  = concatenate_audioclips([raw] * loops).subclipped(0, total_duration)

    vol_segs  = []
    for start_t, dur, vol in envelope:
        end_t = min(start_t + dur, total_duration)
        seg_d = end_t - start_t
        if seg_d < 0.01:
            continue
        seg = extended.subclipped(start_t, end_t).with_volume_scaled(
            max(0.0, float(vol))
        )
        vol_segs.append(seg)

    if not vol_segs:
        return extended.with_volume_scaled(0.10)

    return concatenate_audioclips(vol_segs)


def create_video_from_beats(audio_path: str, beat_clips: list, post: dict) -> str:
    """
    Assembles a video using beat-mapped clips with per-beat durations.

    beat_clips: list of dicts from get_clips_for_beats():
      [{"path": str, "duration": int, "beat_name": str, "emotion": str}, ...]

    Each beat gets its exact duration. If the TTS audio runs longer than the
    total beat duration, beats cycle from the start to cover the remainder.
    Returns path to the output video file.
    """
    output_filename = f"{post['id']}_final.mp4"
    output_path = os.path.join(config.OUTPUT_DIR, output_filename)

    print(f"  Loading audio: {audio_path}")
    audio = AudioFileClip(audio_path)
    total_duration = audio.duration

    segments = []
    elapsed = 0.0
    beat_index = 0
    total_beats = len(beat_clips)

    print(f"  Building {total_duration:.1f}s video from {total_beats} beat-mapped clips...")
    while elapsed < total_duration:
        beat = beat_clips[beat_index % total_beats]
        remaining = total_duration - elapsed
        seg_duration = min(float(beat["duration"]), remaining)

        print(
            f"  -> Beat {beat_index + 1}: '{beat['beat_name']}' [{beat['emotion']}] "
            f"{os.path.basename(beat['path'])} ({seg_duration:.1f}s)"
        )
        seg = _make_segment(beat["path"], seg_duration)
        segments.append(seg)
        elapsed += seg_duration
        beat_index += 1

    background = concatenate_videoclips(segments)
    background = background.with_volume_scaled(config.BROLL_VOLUME)
    final = background.with_audio(audio)

    # Subtle title card for first 5 seconds
    try:
        title_text = post["title"]
        if len(title_text) > 80:
            title_text = title_text[:77] + "..."

        title_clip = (
            TextClip(
                title_text,
                font_size=36,
                color="white",
                stroke_color="black",
                stroke_width=2,
                size=(config.VIDEO_WIDTH - 80, None),
                method="caption",
            )
            .with_position(("center", 40))
            .with_duration(min(5, total_duration))
        )
        final = CompositeVideoClip([final, title_clip])
    except Exception as e:
        print(f"  [WARN] Title card skipped: {e}")

    print(f"  Rendering video to {output_path}...")
    final.write_videofile(
        output_path,
        fps=config.VIDEO_FPS,
        codec="libx264",
        audio_codec="aac",
        temp_audiofile=os.path.join(config.OUTPUT_DIR, "temp_audio.m4a"),
        remove_temp=True,
        logger=None,
    )

    audio.close()
    background.close()

    return output_path


def _parse_script_sections(script: str) -> list:
    """
    Parse a mystery script into timed sections using [NUMBER X] markers.
    Returns list of {marker, text, word_count} dicts in script order.
    """
    # Split on markers — keep markers in output
    pattern = r"(\[(?:COLD OPEN|INTRO|NUMBER \d+[^]]*|OUTRO)\])"
    parts = re.split(pattern, script)

    sections = []
    current_marker = "[COLD OPEN]"
    current_text = ""

    for part in parts:
        part = part.strip()
        if not part:
            continue
        if re.match(r"\[(?:COLD OPEN|INTRO|NUMBER \d|OUTRO)", part):
            if current_text:
                sections.append({
                    "marker": current_marker,
                    "text": current_text,
                    "word_count": len(current_text.split()),
                })
            current_marker = part
            current_text = ""
        else:
            current_text += " " + part

    if current_text:
        sections.append({
            "marker": current_marker,
            "text": current_text,
            "word_count": len(current_text.split()),
        })

    return sections


def _estimate_section_duration(word_count: int, wpm: float = 140.0) -> float:
    """Estimate TTS duration from word count at ~140 words per minute."""
    return max(2.0, (word_count / wpm) * 60.0)


def create_mystery_video(
    audio_path: str,
    beat_clips: list,
    post: dict,
    number_frames: list,
    music_path: str | None = None,
) -> str:
    """
    Assemble a mystery Top 5 video with countdown cards and layered audio.

    Assembly per entry:
      [NUMBER CARD 2.5s] → [B-roll / real footage clips]

    Audio layers:
      Layer 1: ElevenLabs narration (full volume)
      Layer 2: Background music (0.10 volume, ducked to 0.04 under number cards)

    Args:
        audio_path:    Path to the ElevenLabs narration .mp3/.wav.
        beat_clips:    List of B-roll clip dicts from get_clips_for_beats().
        post:          Post dict — must have 'id' and 'title' keys.
        number_frames: List of card dicts from number_frames.generate_all_cards().
                       Each: {"number": int, "title": str, "card_path": str}
        music_path:    Path to background music file, or None for narration-only.

    Returns:
        Path to the rendered output .mp4.
    """
    from modules.music_manager import MUSIC_VOLUME, MUSIC_VOLUME_DUCK

    output_filename = f"{post['id']}_mystery_final.mp4"
    output_path = os.path.join(config.OUTPUT_DIR, output_filename)

    print(f"  [MYSTERY EDIT] Loading audio: {audio_path}")
    narration = AudioFileClip(audio_path)
    total_duration = narration.duration

    # Build a map of number → card path for fast lookup
    card_map = {c["number"]: c["card_path"] for c in number_frames}

    # ── Build video segments ──────────────────────────────────────────────────
    # Strategy: interleave number cards + b-roll to fill narration duration.
    # Number cards are 2.5s fixed. B-roll fills the estimated section length.
    # If we can't estimate sections, fall back to even distribution.

    segments          = []
    music_envelope    = []   # (start_t, dur, volume)
    real_audio_inserts= []   # (start_t, local_path, dur)
    elapsed           = 0.0
    fallback_clips    = _get_fallback_clips()

    # Sort cards descending (5 → 1)
    sorted_cards = sorted(number_frames, key=lambda c: c["number"], reverse=True)
    n_entries    = len(sorted_cards)
    beat_index   = 0
    total_beats  = max(len(beat_clips), 1)

    if n_entries > 0:
        card_total      = n_entries * 2.5
        broll_per_entry = max(3.0, (total_duration - card_total) / n_entries)

        for card_info in sorted_cards:
            if elapsed >= total_duration:
                break

            # ── Number card ──────────────────────────────────────────────────
            card_path = card_info.get("card_path", "")
            if card_path and os.path.exists(card_path):
                card_dur = min(2.5, total_duration - elapsed)
                if card_dur > 0.1:
                    try:
                        card_clip = VideoFileClip(card_path).without_audio()
                        card_clip = card_clip.resized(
                            (config.VIDEO_WIDTH, config.VIDEO_HEIGHT))
                        card_clip = card_clip.subclipped(0, card_dur)
                        segments.append(card_clip)
                        music_envelope.append((elapsed, card_dur, MUSIC_VOLUME_DUCK))
                        elapsed += card_dur
                        print(f"    -> Card #{card_info['number']}: {card_dur:.1f}s")
                    except Exception as e:
                        print(f"  [WARN] Card load failed ({e}) — skipping card.")

            # ── Beat segments for this entry ──────────────────────────────────
            remaining_for_entry = min(broll_per_entry, total_duration - elapsed)
            filled = 0.0
            while filled < remaining_for_entry - 0.1 and elapsed < total_duration:
                beat    = beat_clips[beat_index % total_beats]
                seg_dur = min(float(beat.get("duration", 4)),
                              remaining_for_entry - filled,
                              total_duration - elapsed)
                if seg_dur < 0.1:
                    break

                beat_vol = beat.get("music_volume", MUSIC_VOLUME)
                if not beat.get("music_active", True):
                    beat_vol = 0.0

                try:
                    seg = _make_beat_segment(beat, seg_dur, fallback_clips)
                    segments.append(seg)
                    music_envelope.append((elapsed, seg_dur, beat_vol))

                    # Track real audio inserts
                    if (beat.get("visual_source") == "real_media"
                            and beat.get("media_item", {}) is not None
                            and beat.get("media_item", {}).get("type") == "audio"):
                        lp = beat["media_item"].get("local_path", "")
                        if lp and os.path.exists(lp):
                            real_audio_inserts.append({
                                "start_t": elapsed,
                                "path":    lp,
                                "dur":     seg_dur,
                            })

                    print(
                        f"    -> Beat {beat_index + 1}: '{beat.get('name', beat.get('beat_name', '?'))}' "
                        f"[{beat.get('emotion', '')}] "
                        f"src={beat.get('visual_source', 'broll')} "
                        f"({seg_dur:.1f}s)"
                    )
                    filled  += seg_dur
                    elapsed += seg_dur
                    beat_index += 1
                except Exception as e:
                    print(f"  [WARN] Beat segment failed: {e}")
                    beat_index += 1
                    break

    # ── Fill any remaining time with cycling beats ────────────────────────────
    while elapsed < total_duration - 0.1 and beat_clips:
        remaining = total_duration - elapsed
        beat      = beat_clips[beat_index % total_beats]
        seg_dur   = min(float(beat.get("duration", 4)), remaining)
        if seg_dur < 0.1:
            break

        beat_vol = beat.get("music_volume", MUSIC_VOLUME)
        if not beat.get("music_active", True):
            beat_vol = 0.0

        try:
            seg = _make_beat_segment(beat, seg_dur, fallback_clips)
            segments.append(seg)
            music_envelope.append((elapsed, seg_dur, beat_vol))
            elapsed    += seg_dur
            beat_index += 1
        except Exception as e:
            print(f"  [WARN] Fill segment failed: {e}")
            beat_index += 1
            break

    if not segments:
        raise RuntimeError("No video segments could be assembled for mystery video.")

    # ── Concatenate video track ───────────────────────────────────────────────
    print(f"  [MYSTERY EDIT] Concatenating {len(segments)} segments ({elapsed:.1f}s)...")
    background = concatenate_videoclips(segments)

    if background.duration > total_duration + 0.1:
        background = background.subclipped(0, total_duration)

    # ── Build audio mix ───────────────────────────────────────────────────────
    from moviepy import CompositeAudioClip

    audio_layers = [narration]

    if music_path and os.path.exists(music_path):
        try:
            music_track = _build_music_track_enveloped(
                music_path, music_envelope, total_duration
            )
            if music_track:
                music_track = music_track.audio_fadein(3.0).audio_fadeout(5.0)
                audio_layers.append(music_track)
                print(f"  [MYSTERY EDIT] Music layered (enveloped): "
                      f"{os.path.basename(music_path)}")
        except Exception as e:
            # Fallback to simple global-volume music
            print(f"  [WARN] Music envelope failed ({e}) — simple mix fallback.")
            try:
                music_raw = AudioFileClip(music_path)
                if music_raw.duration < total_duration:
                    loops     = int(total_duration / music_raw.duration) + 1
                    music_raw = concatenate_audioclips([music_raw] * loops)
                music_raw = music_raw.subclipped(0, total_duration)
                music_raw = music_raw.with_volume_scaled(MUSIC_VOLUME)
                music_raw = music_raw.audio_fadein(3.0).audio_fadeout(5.0)
                audio_layers.append(music_raw)
            except Exception as e2:
                print(f"  [WARN] Music fallback also failed ({e2}) — narration only.")

    # Real audio inserts (911 calls, interviews, etc.)
    for insert in real_audio_inserts:
        try:
            ra  = AudioFileClip(insert["path"])
            dur = min(ra.duration, insert["dur"])
            ra  = ra.subclipped(0, dur).with_start(insert["start_t"])
            audio_layers.append(ra)
            print(f"  [MYSTERY EDIT] Real audio insert at "
                  f"{insert['start_t']:.1f}s: {os.path.basename(insert['path'])}")
        except Exception as e:
            print(f"  [WARN] Real audio insert failed: {e}")

    mixed = CompositeAudioClip(audio_layers)
    final = background.with_audio(mixed)

    # ── Render ────────────────────────────────────────────────────────────────
    print(f"  [MYSTERY EDIT] Rendering -> {output_path}...")
    final.write_videofile(
        output_path,
        fps=config.VIDEO_FPS,
        codec="libx264",
        audio_codec="aac",
        temp_audiofile=os.path.join(config.OUTPUT_DIR, "temp_mystery_audio.m4a"),
        remove_temp=True,
        logger=None,
    )

    narration.close()
    background.close()

    return output_path


def create_video(audio_path: str, clip_paths: list, post: dict) -> str:
    """
    Combines TTS audio with multiple b-roll clips, switching every 4-5 seconds.
    Each clip has a Ken Burns slow-zoom effect applied.
    Returns path to the output video file.
    """
    output_filename = f"{post['id']}_final.mp4"
    output_path = os.path.join(config.OUTPUT_DIR, output_filename)

    print(f"  Loading audio: {audio_path}")
    audio = AudioFileClip(audio_path)
    total_duration = audio.duration

    seg_duration = 4.5  # seconds per clip segment
    segments = []
    elapsed = 0.0
    clip_index = 0

    print(f"  Building {total_duration:.1f}s video from {len(clip_paths)} keyword clips...")
    while elapsed < total_duration:
        remaining = total_duration - elapsed
        this_seg = min(seg_duration, remaining)
        path = clip_paths[clip_index % len(clip_paths)]
        print(f"  -> Segment {clip_index + 1}: {os.path.basename(path)} ({this_seg:.1f}s)")
        seg = _make_segment(path, this_seg)
        segments.append(seg)
        elapsed += this_seg
        clip_index += 1

    background = concatenate_videoclips(segments)
    background = background.with_volume_scaled(config.BROLL_VOLUME)
    final = background.with_audio(audio)

    # Subtle title card for first 5 seconds
    try:
        title_text = post["title"]
        if len(title_text) > 80:
            title_text = title_text[:77] + "..."

        title_clip = (
            TextClip(
                title_text,
                font_size=36,
                color="white",
                stroke_color="black",
                stroke_width=2,
                size=(config.VIDEO_WIDTH - 80, None),
                method="caption",
            )
            .with_position(("center", 40))
            .with_duration(min(5, total_duration))
        )
        final = CompositeVideoClip([final, title_clip])
    except Exception as e:
        print(f"  [WARN] Title card skipped: {e}")

    print(f"  Rendering video to {output_path}...")
    final.write_videofile(
        output_path,
        fps=config.VIDEO_FPS,
        codec="libx264",
        audio_codec="aac",
        temp_audiofile=os.path.join(config.OUTPUT_DIR, "temp_audio.m4a"),
        remove_temp=True,
        logger=None,
    )

    audio.close()
    background.close()

    return output_path
