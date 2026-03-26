"""
Asset Indexer - Auto-tagging and Library Management
"""

import os
import json
import cv2
import numpy as np
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from pathlib import Path
import imagehash
from PIL import Image
import asyncio
from sklearn.cluster import KMeans

@dataclass
class VideoAnalysis:
    """Analysis results for a video"""
    duration: float
    width: int
    height: int
    fps: float
    brightness_avg: float
    brightness_std: float
    dominant_colors: List[Tuple[int, int, int]]
    motion_score: float  # 0-1, higher = more camera movement
    scene_changes: List[float]  # timestamps of scene cuts
    has_faces: bool
    is_night_scene: bool

class AssetIndexer:
    """
    Indexes downloaded assets with auto-tagging:
    - Scene type detection (interior/exterior, day/night)
    - Mood analysis (tense, calm, etc.)
    - Camera movement detection
    - Quality scoring
    """

    def __init__(self, library_path: str, database):
        self.library_path = library_path
        self.database = database
        self.thumbnail_path = os.path.join(library_path, "thumbnails")
        os.makedirs(self.thumbnail_path, exist_ok=True)

    async def index_asset(
        self,
        local_path: str,
        source_metadata: dict
    ) -> Optional[str]:
        """
        Analyze video and add to index.
        Returns asset_id if successful.
        """
        if not os.path.exists(local_path):
            print(f"File not found: {local_path}")
            return None

        try:
            # Analyze video
            analysis = await self._analyze_video(local_path)

            # Generate asset ID
            asset_id = f"{source_metadata['source']}_{source_metadata['source_id']}"

            # Extract thumbnails at key frames
            thumbnails = await self._extract_thumbnails(local_path, analysis)

            # Calculate perceptual hash
            phash = self._calculate_phash(thumbnails[0]) if thumbnails else None

            # Determine tags
            tags = self._generate_tags(analysis, source_metadata)
            scene_type = self._determine_scene_type(analysis)
            mood = self._determine_mood(analysis)

            # Calculate quality score
            quality_score = self._calculate_quality(analysis)

            # Create asset record
            from database.models import Asset

            asset = Asset(
                asset_id=asset_id,
                source=source_metadata["source"],
                source_id=source_metadata["source_id"],
                original_url=source_metadata.get("url", ""),
                local_path=local_path,
                filename=os.path.basename(local_path),
                duration=analysis.duration,
                width=analysis.width,
                height=analysis.height,
                fps=analysis.fps,
                file_size=os.path.getsize(local_path),
                format=os.path.splitext(local_path)[1][1:],
                tags=tags,
                scene_type=scene_type,
                mood=mood,
                camera_movement="pan" if analysis.motion_score > 0.5 else "static",
                dominant_colors=[f"rgb{c}" for c in analysis.dominant_colors[:3]],
                brightness_score=analysis.brightness_avg,
                quality_score=quality_score,
                perceptual_hash=str(phash) if phash else None
            )

            # Save to database
            success = await self.database.insert_asset(asset)

            if success:
                print(f"Indexed: {asset.filename} ({scene_type}, {mood}, {len(tags)} tags)")
                return asset_id

        except Exception as e:
            print(f"Error indexing {local_path}: {e}")
            return None

    async def _analyze_video(self, video_path: str) -> VideoAnalysis:
        """Extract video metadata and visual features"""
        cap = cv2.VideoCapture(video_path)

        # Basic metadata
        fps = cap.get(cv2.CAP_PROP_FPS)
        frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        duration = frame_count / fps if fps > 0 else 0
        width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))

        # Sample frames for analysis
        sample_frames = []
        frame_times = []

        # Sample every 2 seconds
        sample_interval = int(fps * 2)
        frame_idx = 0

        brightness_values = []
        prev_frame = None
        motion_scores = []
        scene_changes = []

        while True:
            ret, frame = cap.read()
            if not ret:
                break

            if frame_idx % sample_interval == 0:
                sample_frames.append(frame)
                frame_times.append(frame_idx / fps)

                # Brightness
                gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
                brightness = np.mean(gray)
                brightness_values.append(brightness)

                # Motion detection
                if prev_frame is not None:
                    diff = cv2.absdiff(prev_frame, gray)
                    motion = np.mean(diff)
                    motion_scores.append(motion)

                    # Scene change detection (simple threshold)
                    if motion > 50:  # threshold
                        scene_changes.append(frame_idx / fps)

                prev_frame = gray

            frame_idx += 1

        cap.release()

        # Calculate dominant colors from middle frame
        dominant_colors = []
        if sample_frames:
            mid_frame = sample_frames[len(sample_frames) // 2]
            dominant_colors = self._extract_dominant_colors(mid_frame)

        # Determine if night scene (low brightness, blue tones)
        avg_brightness = np.mean(brightness_values) if brightness_values else 0.5
        is_night = avg_brightness < 80

        # Normalize motion score
        motion_score = min(1.0, np.mean(motion_scores) / 30) if motion_scores else 0

        return VideoAnalysis(
            duration=duration,
            width=width,
            height=height,
            fps=fps,
            brightness_avg=avg_brightness / 255,  # normalize to 0-1
            brightness_std=np.std(brightness_values) / 255 if brightness_values else 0,
            dominant_colors=dominant_colors,
            motion_score=motion_score,
            scene_changes=scene_changes[:10],  # limit stored changes
            has_faces=False,  # Would need face detection model
            is_night_scene=is_night
        )

    def _extract_dominant_colors(self, frame: np.ndarray, n_colors: int = 3) -> List[Tuple]:
        """Extract dominant colors using K-means"""
        # Resize for speed
        small = cv2.resize(frame, (100, 100))
        pixels = small.reshape(-1, 3)

        # K-means clustering
        kmeans = KMeans(n_clusters=n_colors, random_state=42, n_init=10)
        kmeans.fit(pixels)

        colors = kmeans.cluster_centers_.astype(int)
        return [tuple(c) for c in colors]

    async def _extract_thumbnails(
        self,
        video_path: str,
        analysis: VideoAnalysis
    ) -> List[str]:
        """Extract thumbnails at key moments"""
        cap = cv2.VideoCapture(video_path)
        thumbnails = []

        # Extract at: 10%, 50%, 90% of duration
        positions = [0.1, 0.5, 0.9]

        for pos in positions:
            time_sec = analysis.duration * pos
            cap.set(cv2.CAP_PROP_POS_MSEC, time_sec * 1000)

            ret, frame = cap.read()
            if ret:
                # Save thumbnail
                thumb_filename = f"{Path(video_path).stem}_{int(pos*100)}.jpg"
                thumb_path = os.path.join(self.thumbnail_path, thumb_filename)
                cv2.imwrite(thumb_path, frame)
                thumbnails.append(thumb_path)

        cap.release()
        return thumbnails

    def _calculate_phash(self, image_path: str) -> Optional[imagehash.ImageHash]:
        """Calculate perceptual hash for deduplication"""
        try:
            with Image.open(image_path) as img:
                return imagehash.phash(img)
        except Exception:
            return None

    def _generate_tags(self, analysis: VideoAnalysis, metadata: dict) -> List[str]:
        """Generate searchable tags"""
        tags = []

        # Visual tags
        if analysis.is_night_scene:
            tags.extend(["night", "dark", "atmospheric"])
        else:
            tags.extend(["day", "bright"])

        if analysis.motion_score > 0.3:
            tags.append("movement")
        else:
            tags.append("static")

        # Resolution tags
        if analysis.height >= 1080:
            tags.append("hd")
        if analysis.height >= 2160:
            tags.append("4k")

        # Duration tags
        if analysis.duration < 5:
            tags.append("short")
        elif analysis.duration > 30:
            tags.append("long")

        # Source-specific tags
        if metadata.get("source") == "archive_org":
            tags.extend(["vintage", "historical", "archive"])

        # Original tags from source
        original_tags = metadata.get("tags", [])
        if isinstance(original_tags, str):
            original_tags = original_tags.split(", ")
        tags.extend(original_tags)

        return list(set(t.lower() for t in tags if t))

    def _determine_scene_type(self, analysis: VideoAnalysis) -> Optional[str]:
        """Classify scene type"""
        if analysis.is_night_scene:
            if analysis.brightness_std > 0.2:
                return "ext_night"  # Night exterior with variation
            else:
                return "int_night"  # Dark interior
        else:
            if analysis.motion_score > 0.4:
                return "ext_day"  # Likely outdoor
            else:
                return "int_day"  # Likely indoor

    def _determine_mood(self, analysis: VideoAnalysis) -> Optional[str]:
        """Determine mood from visual characteristics"""
        if analysis.is_night_scene:
            if analysis.brightness_std > 0.15:
                return "tense"  # High contrast, dramatic
            else:
                return "melancholy"  # Flat, dark
        else:
            if analysis.motion_score > 0.5:
                return "action"
            elif analysis.brightness_avg > 0.7:
                return "calm"
            else:
                return "investigation"

    def _calculate_quality(self, analysis: VideoAnalysis) -> float:
        """Calculate quality score 0-1"""
        scores = []

        # Resolution (0-0.3)
        if analysis.height >= 2160:
            scores.append(0.3)
        elif analysis.height >= 1080:
            scores.append(0.25)
        elif analysis.height >= 720:
            scores.append(0.15)
        else:
            scores.append(0.05)

        # Duration appropriateness (0-0.2)
        # Ideal: 5-20 seconds for B-roll
        if 5 <= analysis.duration <= 20:
            scores.append(0.2)
        elif 3 <= analysis.duration <= 30:
            scores.append(0.15)
        else:
            scores.append(0.05)

        # Visual stability (0-0.25)
        # Not too dark, not blown out
        if 0.2 < analysis.brightness_avg < 0.8:
            scores.append(0.25)
        else:
            scores.append(0.1)

        # Motion (0-0.25)
        # Some movement is good, too much is hard to use
        if 0.1 < analysis.motion_score < 0.5:
            scores.append(0.25)
        else:
            scores.append(0.15)

        return sum(scores)

    async def scan_and_index(self, source_filter: Optional[str] = None):
        """Scan library folder and index unindexed files"""
        raw_path = os.path.join(self.library_path, "raw")

        if not os.path.exists(raw_path):
            print(f"Library path not found: {raw_path}")
            return

        indexed = 0

        for source in os.listdir(raw_path):
            if source_filter and source != source_filter:
                continue

            source_path = os.path.join(raw_path, source)
            if not os.path.isdir(source_path):
                continue

            for filename in os.listdir(source_path):
                if filename.startswith("."):
                    continue

                # Check if already indexed
                asset_id = f"{source}_{filename.split('.')[0]}"
                existing = await self.database.get_asset(asset_id)

                if existing:
                    continue

                # Index it
                file_path = os.path.join(source_path, filename)
                metadata = {
                    "source": source,
                    "source_id": filename.split('.')[0],
                    "url": ""
                }

                result = await self.index_asset(file_path, metadata)
                if result:
                    indexed += 1

        print(f"Indexed {indexed} new assets")

    async def query_assets(
        self,
        scene_type=None,
        mood=None,
        tags=None,
        min_duration=0,
        max_duration=9999,
        exclude_assets=None,
        limit=10
    ):
        """Query assets via database with optional exclude set"""
        results = await self.database.query_assets(
            scene_type=scene_type,
            mood=mood,
            tags=tags,
            min_duration=min_duration,
            max_duration=max_duration,
            limit=limit * 2  # over-fetch to allow exclusion filtering
        )
        if exclude_assets:
            results = [a for a in results if a.asset_id not in exclude_assets]
        return results[:limit]
