"""
editor.py — Combines TTS audio with keyword-matched b-roll clips using MoviePy.
Each clip plays for 4-5 seconds with a Ken Burns slow-zoom effect.
Output: 1920x1080 MP4 at 30fps
"""

import os
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
