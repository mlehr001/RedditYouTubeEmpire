"""
editor.py — Combines TTS audio with looping background video using MoviePy
Output: 1920x1080 MP4 at 30fps
"""

import os
import config
from moviepy.editor import (
    VideoFileClip,
    AudioFileClip,
    CompositeVideoClip,
    TextClip,
    concatenate_videoclips,
)


def _loop_video_to_duration(video_path, target_duration):
    """Loops a video clip to fill the target duration."""
    clip = VideoFileClip(video_path).without_audio()

    # Resize to target resolution
    clip = clip.resize((config.VIDEO_WIDTH, config.VIDEO_HEIGHT))

    if clip.duration >= target_duration:
        return clip.subclip(0, target_duration)

    # Loop by repeating the clip
    loops_needed = int(target_duration / clip.duration) + 1
    looped = concatenate_videoclips([clip] * loops_needed)
    return looped.subclip(0, target_duration)


def create_video(audio_path, broll_path, post):
    """
    Combines audio + background video into a finished MP4.
    Returns path to the output video file.
    """
    output_filename = f"{post['id']}_final.mp4"
    output_path = os.path.join(config.OUTPUT_DIR, output_filename)

    print(f"  Loading audio: {audio_path}")
    audio = AudioFileClip(audio_path)
    duration = audio.duration

    print(f"  Looping background video to {duration:.1f}s...")
    background = _loop_video_to_duration(broll_path, duration)

    # Lower the background video volume
    background = background.volumex(config.BROLL_VOLUME)

    # Set the audio track
    final = background.set_audio(audio)

    # Add a subtle title card at the top for the first 5 seconds
    try:
        title_text = post["title"]
        if len(title_text) > 80:
            title_text = title_text[:77] + "..."

        title_clip = (
            TextClip(
                title_text,
                fontsize=36,
                color="white",
                stroke_color="black",
                stroke_width=2,
                size=(config.VIDEO_WIDTH - 80, None),
                method="caption",
            )
            .set_position(("center", 40))
            .set_duration(min(5, duration))
        )
        final = CompositeVideoClip([final, title_clip])
    except Exception as e:
        print(f"  ⚠️  Title card skipped (ImageMagick not installed?): {e}")

    print(f"  Rendering video to {output_path}...")
    final.write_videofile(
        output_path,
        fps=config.VIDEO_FPS,
        codec="libx264",
        audio_codec="aac",
        temp_audiofile=os.path.join(config.OUTPUT_DIR, "temp_audio.m4a"),
        remove_temp=True,
        verbose=False,
        logger=None,
    )

    # Clean up
    audio.close()
    background.close()

    return output_path
