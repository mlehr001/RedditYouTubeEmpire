"""
editor.py — Combines TTS audio with keyword-matched b-roll clips using MoviePy.
Each clip plays for 4-5 seconds with a Ken Burns slow-zoom effect.
Output: 1920x1080 MP4 at 30fps
"""

import os
import re
import numpy as np
import config
from moviepy import (
    VideoFileClip,
    AudioFileClip,
    CompositeVideoClip,
    TextClip,
    concatenate_videoclips,
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

    segments = []
    elapsed = 0.0

    # Sort cards descending (5 → 1)
    sorted_cards = sorted(number_frames, key=lambda c: c["number"], reverse=True)
    n_entries = len(sorted_cards)
    beat_index = 0
    total_beats = max(len(beat_clips), 1)

    if n_entries > 0:
        # Approximate section duration excluding card time
        card_total = n_entries * 2.5
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
                        card_clip = card_clip.resized((config.VIDEO_WIDTH, config.VIDEO_HEIGHT))
                        card_clip = card_clip.subclipped(0, card_dur)
                        segments.append(card_clip)
                        elapsed += card_dur
                        print(f"    -> Card #{card_info['number']}: {card_dur:.1f}s")
                    except Exception as e:
                        print(f"  [WARN] Card load failed ({e}) — skipping card.")

            # ── B-roll for this entry ─────────────────────────────────────────
            remaining_for_entry = min(broll_per_entry, total_duration - elapsed)
            filled = 0.0
            while filled < remaining_for_entry - 0.1 and elapsed < total_duration:
                beat = beat_clips[beat_index % total_beats]
                seg_dur = min(float(beat["duration"]), remaining_for_entry - filled,
                              total_duration - elapsed)
                if seg_dur < 0.1:
                    break
                try:
                    seg = _make_segment(beat["path"], seg_dur)
                    segments.append(seg)
                    filled += seg_dur
                    elapsed += seg_dur
                    beat_index += 1
                except Exception as e:
                    print(f"  [WARN] B-roll segment failed: {e}")
                    beat_index += 1
                    break

    # Fill any remaining time with cycling b-roll
    while elapsed < total_duration - 0.1 and beat_clips:
        remaining = total_duration - elapsed
        beat = beat_clips[beat_index % total_beats]
        seg_dur = min(float(beat["duration"]), remaining)
        if seg_dur < 0.1:
            break
        try:
            seg = _make_segment(beat["path"], seg_dur)
            segments.append(seg)
            elapsed += seg_dur
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

    # Trim or extend to match narration exactly
    if background.duration > total_duration + 0.1:
        background = background.subclipped(0, total_duration)

    # ── Mix narration + background music ──────────────────────────────────────
    if music_path and os.path.exists(music_path):
        try:
            from moviepy import CompositeAudioClip, afx

            music = AudioFileClip(music_path)

            # Loop music to cover full narration duration
            if music.duration < total_duration:
                loops = int(total_duration / music.duration) + 1
                from moviepy import concatenate_audioclips
                music = concatenate_audioclips([music] * loops)

            music = music.subclipped(0, total_duration)
            music = music.with_volume_scaled(MUSIC_VOLUME)

            # Fade in/out
            music = music.audio_fadein(3.0).audio_fadeout(5.0)

            mixed = CompositeAudioClip([narration, music])
            final = background.with_audio(mixed)
            print(f"  [MYSTERY EDIT] Music layered: {os.path.basename(music_path)}")
        except Exception as e:
            print(f"  [WARN] Music mixing failed ({e}) — narration only.")
            final = background.with_audio(narration)
    else:
        final = background.with_audio(narration)

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
