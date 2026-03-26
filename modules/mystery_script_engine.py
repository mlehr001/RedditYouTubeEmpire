"""
Script Engine - Structured script format with timing and visual cues
"""

import re
from typing import List, Dict, Optional
from dataclasses import dataclass, field
from datetime import datetime
import json

@dataclass
class Scene:
    """Represents a single scene in the video"""
    scene_id: str
    start_time: float  # seconds from video start
    duration: float      # target duration in seconds
    word_count: int      # calculated from script

    # Content
    audio_script: str
    audio_file: Optional[str] = None

    # Visual direction
    visual_description: str = ""  # e.g., "EXT. ABANDONED HOUSE - NIGHT"
    b_roll_tags: List[str] = field(default_factory=list)  # ["ext_night", "house"]
    mood: str = "neutral"  # tense, melancholy, reveal, conclusion

    # Production
    text_overlay: Optional[str] = None  # "1987 - Vermont"
    transition_in: str = "cut"  # cut, fade, dissolve
    transition_duration: float = 0.5

    # Generated fields
    end_time: float = 0.0  # calculated

    def calculate_end_time(self):
        self.end_time = self.start_time + self.duration

class ScriptEngine:
    """
    Manages script creation and parsing for mystery videos.
    Target: 18-20 minutes, 5 cases, ~150 words/minute
    """

    def __init__(
        self,
        target_duration: int = 1200,  # 20 minutes
        words_per_minute: int = 150,
        cases_per_video: int = 5
    ):
        self.target_duration = target_duration
        self.words_per_minute = words_per_minute
        self.cases_per_video = cases_per_video
        self.wpm_with_padding = words_per_minute * 0.85  # 15% breathing room

    def create_template(self, topic: str, case_titles: List[str]) -> List[Scene]:
        """
        Generate complete scene structure for Top 5 Mystery video.

        Structure:
        - Intro (0:00-0:45)
        - Case 1-5: Setup, Mystery, Theories (3-4 min each)
        - Outro (last 1:00)
        """
        scenes = []
        current_time = 0.0

        # Intro
        intro_duration = 45
        intro_words = self._calculate_word_count(intro_duration)

        scenes.append(Scene(
            scene_id="intro",
            start_time=current_time,
            duration=intro_duration,
            word_count=intro_words,
            audio_script=f"Welcome back to Unsolved. Tonight, we're diving into {topic}. Five mysteries that defy explanation. Let's begin.",
            visual_description="INT. STUDIO - NIGHT",
            b_roll_tags=["studio", "dark", "atmospheric"],
            mood="tense",
            transition_in="fade"
        ))
        current_time += intro_duration

        # Cases
        case_time = (self.target_duration - 45 - 60) / self.cases_per_video

        for i, case_title in enumerate(case_titles[:self.cases_per_video], 1):
            case_scenes = self._create_case_scenes(
                case_num=i,
                case_title=case_title,
                start_time=current_time,
                total_duration=case_time
            )
            scenes.extend(case_scenes)
            current_time += case_time

        # Outro
        outro_duration = 60
        scenes.append(Scene(
            scene_id="outro",
            start_time=current_time,
            duration=outro_duration,
            word_count=self._calculate_word_count(outro_duration),
            audio_script="Thanks for watching. If you enjoyed these mysteries, subscribe for more unsolved cases every week. Until next time, stay curious.",
            visual_description="INT. STUDIO - NIGHT",
            b_roll_tags=["studio", "outro"],
            mood="conclusion",
            transition_in="fade"
        ))

        # Calculate all end times
        for scene in scenes:
            scene.calculate_end_time()

        return scenes

    def _create_case_scenes(
        self,
        case_num: int,
        case_title: str,
        start_time: float,
        total_duration: float
    ) -> List[Scene]:
        """Create 3 scenes per case: Setup, Mystery, Theories"""
        scenes = []

        # Split time: 30% setup, 50% mystery, 20% theories
        setup_duration = total_duration * 0.30
        mystery_duration = total_duration * 0.50
        theories_duration = total_duration * 0.20

        # Setup
        scenes.append(Scene(
            scene_id=f"case{case_num}_setup",
            start_time=start_time,
            duration=setup_duration,
            word_count=self._calculate_word_count(setup_duration),
            audio_script=f"Case {case_num}. {case_title}. The story begins in...",
            visual_description="EXT. LOCATION - DAY/FLASHBACK",
            b_roll_tags=["establishing", "location", "vintage"],
            mood="investigation",
            text_overlay=f"CASE {case_num}"
        ))

        # Mystery
        scenes.append(Scene(
            scene_id=f"case{case_num}_mystery",
            start_time=start_time + setup_duration,
            duration=mystery_duration,
            word_count=self._calculate_word_count(mystery_duration),
            audio_script="Then, everything changed. The events of that night remain unexplained...",
            visual_description="EXT. LOCATION - NIGHT",
            b_roll_tags=["ext_night", "mystery", "dark"],
            mood="tense"
        ))

        # Theories
        scenes.append(Scene(
            scene_id=f"case{case_num}_theories",
            start_time=start_time + setup_duration + mystery_duration,
            duration=theories_duration,
            word_count=self._calculate_word_count(theories_duration),
            audio_script="Investigators considered several possibilities, but none fully explained what happened...",
            visual_description="INT. POLICE STATION - NIGHT",
            b_roll_tags=["police", "files", "investigation"],
            mood="melancholy"
        ))

        return scenes

    def _calculate_word_count(self, duration: float) -> int:
        """Calculate target word count for duration"""
        minutes = duration / 60
        return int(minutes * self.wpm_with_padding)

    def parse_script_markdown(self, markdown: str) -> List[Scene]:
        """
        Parse markdown format:

        [SCENE 1 - INTRO - 0:00-0:45]
        AUDIO: Welcome back...
        VISUAL: [INT. STUDIO - NIGHT] Dark, atmospheric
        MOOD: tense
        B-ROLL: studio, dark, atmospheric
        TEXT: "CASE 1"
        TRANSITION: fade
        """
        scenes = []

        # Split by scene headers
        pattern = r'\[SCENE (\d+) - ([^\]]+) - ([\d:]+)-([\d:]+)\]'
        parts = re.split(pattern, markdown)

        if len(parts) < 2:
            return scenes

        # Parse each scene
        for i in range(1, len(parts), 5):
            if i + 4 > len(parts):
                break

            scene_num = parts[i]
            scene_name = parts[i+1]
            start_str = parts[i+2]
            end_str = parts[i+3]
            content = parts[i+4]

            # Parse times
            start_time = self._time_to_seconds(start_str)
            end_time = self._time_to_seconds(end_str)
            duration = end_time - start_time

            # Extract fields
            audio = self._extract_field(content, "AUDIO")
            visual = self._extract_field(content, "VISUAL")
            mood = self._extract_field(content, "MOOD") or "neutral"
            b_roll = self._extract_field(content, "B-ROLL")
            text = self._extract_field(content, "TEXT")
            transition = self._extract_field(content, "TRANSITION") or "cut"

            word_count = len(audio.split()) if audio else 0

            scene = Scene(
                scene_id=f"s{scene_num}_{scene_name.lower().replace(' ', '_')}",
                start_time=start_time,
                duration=duration,
                word_count=word_count,
                audio_script=audio,
                visual_description=visual,
                b_roll_tags=b_roll.split(", ") if b_roll else [],
                mood=mood,
                text_overlay=text,
                transition_in=transition,
                end_time=end_time
            )
            scenes.append(scene)

        return scenes

    def _time_to_seconds(self, time_str: str) -> float:
        """Convert MM:SS to seconds"""
        parts = time_str.split(":")
        if len(parts) == 2:
            return int(parts[0]) * 60 + int(parts[1])
        return 0

    def _extract_field(self, content: str, field: str) -> Optional[str]:
        """Extract field value from content"""
        pattern = f"{field}:\\s*(.+?)(?=\\n[A-Z]+:|\\Z)"
        match = re.search(pattern, content, re.DOTALL | re.IGNORECASE)
        if match:
            return match.group(1).strip()
        return None

    def export_tts_segments(self, scenes: List[Scene]) -> List[Dict]:
        """Export audio scripts for TTS generation"""
        segments = []

        for scene in scenes:
            if not scene.audio_script:
                continue

            segment = {
                "scene_id": scene.scene_id,
                "text": scene.audio_script,
                "output_filename": f"{scene.scene_id}_tts.wav",
                "target_duration": scene.duration,
                "word_count": scene.word_count
            }
            segments.append(segment)

        return segments

    def validate_script(self, scenes: List[Scene]) -> Dict:
        """Validate script for timing and consistency"""
        issues = []
        total_duration = 0

        for i, scene in enumerate(scenes):
            # Check gaps
            if i > 0:
                prev_end = scenes[i-1].end_time
                if scene.start_time != prev_end:
                    issues.append(f"Gap between {scenes[i-1].scene_id} and {scene.scene_id}")

            # Check word count vs duration
            expected_words = self._calculate_word_count(scene.duration)
            if abs(scene.word_count - expected_words) > 20:
                issues.append(f"{scene.scene_id}: Word count mismatch ({scene.word_count} vs {expected_words})")

            total_duration += scene.duration

        return {
            "valid": len(issues) == 0,
            "total_duration": total_duration,
            "target_duration": self.target_duration,
            "variance": abs(total_duration - self.target_duration),
            "issues": issues
        }
