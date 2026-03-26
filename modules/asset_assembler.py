"""
Video Assembler - Sync audio and video by scene timestamps
"""

import os
import subprocess
from typing import List, Dict, Optional
from dataclasses import dataclass
from pathlib import Path
import tempfile

@dataclass
class AssemblyConfig:
    video_codec: str = "libx264"
    audio_codec: str = "aac"
    preset: str = "slow"
    crf: int = 18
    resolution: tuple = (1920, 1080)
    fps: int = 30

class VideoAssembler:
    """
    Assembles final video from scenes using FFmpeg.
    More reliable than MoviePy for production use.
    """

    def __init__(self, output_path: str, temp_path: str = None):
        self.output_path = output_path
        self.temp_path = temp_path or os.path.join(output_path, "temp")
        os.makedirs(self.temp_path, exist_ok=True)
        os.makedirs(os.path.join(output_path, "scenes"), exist_ok=True)
        os.makedirs(os.path.join(output_path, "videos"), exist_ok=True)

        self.config = AssemblyConfig()

    def assemble_scene(
        self,
        scene,
        asset_path: str,
        audio_path: str,
        effects: List[str] = None,
        text_overlay: Optional[str] = None
    ) -> str:
        """
        Assemble single scene:
        1. Trim/loop B-roll to match audio duration
        2. Apply effects (grain, color, zoom)
        3. Add text overlay if specified
        4. Mix with audio
        """
        scene_file = os.path.join(
            self.output_path,
            "scenes",
            f"{scene.scene_id}.mp4"
        )

        # Get audio duration
        audio_duration = self._get_duration(audio_path)
        target_duration = min(audio_duration, scene.duration)

        # Build FFmpeg filter complex
        filters = []

        # 1. Video input and trim/loop
        video_filters = []

        # Loop if video is shorter than target
        video_duration = self._get_duration(asset_path)
        if video_duration < target_duration:
            # Loop
            loops = int(target_duration / video_duration) + 1
            video_filters.append(f"loop=loop={loops}:size={int(video_duration*30)}:start=0")

        # Trim to exact duration
        video_filters.append(f"trim=duration={target_duration}")
        video_filters.append("setpts=PTS-STARTPTS")

        # 2. Effects
        if effects:
            if "grain" in effects:
                video_filters.append("noise=alls=20:allf=t+u")
            if "vintage" in effects:
                video_filters.append("curves=r='0/0 0.5/0.4 1/0.8':g='0/0 0.5/0.5 1/0.9':b='0/0 0.5/0.6 1/1'")
            if "ken_burns" in effects:
                # Slow zoom in
                video_filters.append("zoompan=z='min(zoom+0.0015,1.5)':d={}:x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':s=1920x1080".format(int(target_duration*30)))

        # Scale to target resolution
        video_filters.append(f"scale={self.config.resolution[0]}:{self.config.resolution[1]}:force_original_aspect_ratio=decrease,pad={self.config.resolution[0]}:{self.config.resolution[1]}:(ow-iw)/2:(oh-ih)/2")

        # 3. Text overlay
        if text_overlay:
            # Escape text for FFmpeg
            safe_text = text_overlay.replace(":", "\\:").replace("'", "\\'")
            video_filters.append(
                f"drawtext=text='{safe_text}':fontcolor=white:fontsize=48:"
                f"x=(w-text_w)/2:y=h-text_h-50:"
                f"borderw=2:bordercolor=black:"
                f"fontfile=/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
            )

        # Build command
        filter_str = ",".join(video_filters)

        cmd = [
            "ffmpeg", "-y",
            "-i", asset_path,
            "-i", audio_path,
            "-filter_complex",
            f"[0:v]{filter_str}[v];[1:a]anull[a]",
            "-map", "[v]",
            "-map", "[a]",
            "-c:v", self.config.video_codec,
            "-preset", self.config.preset,
            "-crf", str(self.config.crf),
            "-c:a", self.config.audio_codec,
            "-b:a", "192k",
            "-pix_fmt", "yuv420p",
            "-movflags", "+faststart",
            scene_file
        ]

        # Execute
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True
            )
            return scene_file
        except subprocess.CalledProcessError as e:
            print(f"FFmpeg error: {e.stderr}")
            return None

    def composite_final(
        self,
        scene_files: List[str],
        background_music: Optional[str] = None,
        output_filename: str = "final.mp4"
    ) -> str:
        """
        Concatenate all scenes, add music, transitions.
        """
        if not scene_files:
            return None

        final_path = os.path.join(self.output_path, "videos", output_filename)

        # Create concat file list
        concat_file = os.path.join(self.temp_path, "concat_list.txt")
        with open(concat_file, "w") as f:
            for scene_file in scene_files:
                if os.path.exists(scene_file):
                    # Use absolute path, escape backslashes
                    abs_path = os.path.abspath(scene_file)
                    f.write(f"file '{abs_path}'\n")

        # Build command
        cmd = [
            "ffmpeg", "-y",
            "-f", "concat",
            "-safe", "0",
            "-i", concat_file,
        ]

        # Add music if provided
        filter_complex = ""
        if background_music and os.path.exists(background_music):
            cmd.extend(["-i", background_music])
            # Mix audio with ducking
            filter_complex = (
                "[0:a][1:a]amix=inputs=2:duration=first:dropout_transition=3[a];"
                "[a]loudnorm=I=-16:TP=-1.5:LRA=11[audio]"
            )
        else:
            filter_complex = "[0:a]loudnorm=I=-16:TP=-1.5:LRA=11[audio]"

        cmd.extend([
            "-filter_complex", filter_complex,
            "-map", "0:v",
            "-map", "[audio]",
            "-c:v", self.config.video_codec,
            "-preset", self.config.preset,
            "-crf", str(self.config.crf),
            "-c:a", self.config.audio_codec,
            "-b:a", "192k",
            "-pix_fmt", "yuv420p",
            "-movflags", "+faststart",
            final_path
        ])

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            print(f"Final video: {final_path}")
            return final_path
        except subprocess.CalledProcessError as e:
            print(f"FFmpeg error: {e.stderr}")
            return None

    def apply_ken_burns(
        self,
        image_path: str,
        duration: float,
        zoom_direction: str = "in",
        output_path: Optional[str] = None
    ) -> str:
        """Convert still image to video with Ken Burns effect"""
        if not output_path:
            output_path = os.path.join(
                self.temp_path,
                f"ken_burns_{Path(image_path).stem}.mp4"
            )

        # Zoom parameters
        if zoom_direction == "in":
            zoom_expr = "zoom+0.0015"
            start_zoom = "1.0"
        else:
            zoom_expr = "zoom-0.0015"
            start_zoom = "1.5"

        frames = int(duration * 30)

        cmd = [
            "ffmpeg", "-y",
            "-loop", "1",
            "-i", image_path,
            "-vf",
            f"zoompan=z='if(lte(zoom,1.0),{start_zoom},{zoom_expr})':"
            f"d={frames}:x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':s=1920x1080",
            "-c:v", self.config.video_codec,
            "-t", str(duration),
            "-pix_fmt", "yuv420p",
            output_path
        ]

        subprocess.run(cmd, capture_output=True, check=True)
        return output_path

    def _get_duration(self, media_path: str) -> float:
        """Get media duration using ffprobe"""
        cmd = [
            "ffprobe", "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            media_path
        ]

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True
            )
            return float(result.stdout.strip())
        except:
            return 0.0
