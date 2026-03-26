"""
B-Roll Matcher - Match script scenes to indexed assets
"""

from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
import random

@dataclass
class MatchResult:
    """Result of matching a scene to assets"""
    scene_id: str
    primary_match: Optional[str]  # asset_id
    alternatives: List[str]     # backup asset_ids
    confidence: float           # 0-1 match quality
    fallback_strategy: str      # exact_match, mood_match, generated, text_overlay

class BRollMatcher:
    """
    Matches script scenes to library assets based on:
    - Tag overlap
    - Scene type compatibility
    - Mood matching
    - Freshness (avoid recent reuse)
    """

    def __init__(self, asset_indexer, usage_tracker):
        self.indexer = asset_indexer
        self.tracker = usage_tracker

        # Fallback strategies in order of preference
        self.fallbacks = [
            "exact_match",
            "mood_match",
            "scene_type_match",
            "generated_fill",
            "text_overlay"
        ]

    async def match_scenes(
        self,
        scenes: List,
        video_id: str,
        variety_target: int = 3
    ) -> Dict[str, MatchResult]:
        """
        Match all scenes to assets.
        Returns: {scene_id: MatchResult}
        """
        results = {}

        # Get recently used assets to avoid
        recent_assets = await self.tracker.get_recent_assets(
            video_id,
            lookback=variety_target
        )

        for scene in scenes:
            match = await self._match_single_scene(
                scene,
                video_id,
                exclude_assets=recent_assets
            )
            results[scene.scene_id] = match

            # Update recent assets to prevent reuse within this video
            if match.primary_match:
                recent_assets.add(match.primary_match)

        return results

    async def _match_single_scene(
        self,
        scene,
        video_id: str,
        exclude_assets: set,
        max_alternatives: int = 2
    ) -> MatchResult:
        """Find best asset match for a single scene"""

        # Strategy 1: Exact tag match
        if scene.b_roll_tags:
            candidates = await self.indexer.query_assets(
                tags=scene.b_roll_tags,
                mood=scene.mood,
                min_duration=3,
                max_duration=scene.duration + 5,
                exclude_assets=exclude_assets,
                limit=5
            )

            if candidates:
                # Score by tag overlap
                scored = []
                for asset in candidates:
                    score = self._calculate_tag_overlap(
                        scene.b_roll_tags,
                        asset.tags
                    )
                    scored.append((asset, score))

                # Sort by score
                scored.sort(key=lambda x: x[1], reverse=True)

                return MatchResult(
                    scene_id=scene.scene_id,
                    primary_match=scored[0][0].asset_id,
                    alternatives=[a.asset_id for a, _ in scored[1:max_alternatives+1]],
                    confidence=scored[0][1],
                    fallback_strategy="exact_match"
                )

        # Strategy 2: Mood match
        if scene.mood:
            candidates = await self.indexer.query_assets(
                mood=scene.mood,
                min_duration=3,
                max_duration=scene.duration + 5,
                exclude_assets=exclude_assets,
                limit=3
            )

            if candidates:
                return MatchResult(
                    scene_id=scene.scene_id,
                    primary_match=candidates[0].asset_id,
                    alternatives=[a.asset_id for a in candidates[1:max_alternatives+1]],
                    confidence=0.6,
                    fallback_strategy="mood_match"
                )

        # Strategy 3: Scene type match
        scene_type = self._infer_scene_type(scene.visual_description)
        if scene_type:
            candidates = await self.indexer.query_assets(
                scene_type=scene_type,
                min_duration=3,
                max_duration=scene.duration + 5,
                exclude_assets=exclude_assets,
                limit=3
            )

            if candidates:
                return MatchResult(
                    scene_id=scene.scene_id,
                    primary_match=candidates[0].asset_id,
                    alternatives=[a.asset_id for a in candidates[1:max_alternatives+1]],
                    confidence=0.5,
                    fallback_strategy="scene_type_match"
                )

        # Strategy 4: Any unused asset
        candidates = await self.indexer.query_assets(
            min_duration=3,
            max_duration=30,
            exclude_assets=exclude_assets,
            limit=1
        )

        if candidates:
            return MatchResult(
                scene_id=scene.scene_id,
                primary_match=candidates[0].asset_id,
                alternatives=[],
                confidence=0.3,
                fallback_strategy="generic"
            )

        # Strategy 5: Generate or text overlay
        return MatchResult(
            scene_id=scene.scene_id,
            primary_match=None,
            alternatives=[],
            confidence=0.0,
            fallback_strategy="generated_fill"
        )

    def _calculate_tag_overlap(
        self,
        scene_tags: List[str],
        asset_tags: List[str]
    ) -> float:
        """Calculate Jaccard similarity between tag sets"""
        scene_set = set(t.lower() for t in scene_tags)
        asset_set = set(t.lower() for t in asset_tags)

        if not scene_set:
            return 0.0

        intersection = scene_set & asset_set
        union = scene_set | asset_set

        return len(intersection) / len(union) if union else 0.0

    def _infer_scene_type(self, visual_description: str) -> Optional[str]:
        """Infer scene type from visual description"""
        desc = visual_description.lower()

        if "ext." in desc or "exterior" in desc:
            if "night" in desc:
                return "ext_night"
            else:
                return "ext_day"
        elif "int." in desc or "interior" in desc:
            if "night" in desc or "dark" in desc:
                return "int_night"
            else:
                return "int_day"

        return None

    def generate_missing_prompt(self, scene) -> str:
        """Generate AI image generation prompt for missing B-roll"""

        # Extract key elements from scene
        visual = scene.visual_description
        mood = scene.mood

        # Base prompt structure
        prompt_parts = []

        # Scene type
        if "ext." in visual.lower():
            prompt_parts.append("exterior wide shot")
        elif "int." in visual.lower():
            prompt_parts.append("interior cinematic shot")

        # Time of day
        if "night" in visual.lower():
            prompt_parts.append("night, dark, atmospheric lighting")
        else:
            prompt_parts.append("daylight, natural lighting")

        # Subject
        # Extract nouns from visual description (simplified)
        subjects = []
        keywords = ["house", "forest", "street", "room", "station", "car", "road"]
        for kw in keywords:
            if kw in visual.lower():
                subjects.append(kw)

        if subjects:
            prompt_parts.append(", ".join(subjects))

        # Mood
        mood_styles = {
            "tense": "dramatic shadows, high contrast, ominous",
            "melancholy": "muted colors, soft focus, nostalgic",
            "investigation": "documentary style, neutral tones",
            "reveal": "sharp focus, dramatic lighting, clarity",
            "conclusion": "wide shot, peaceful, resolved"
        }

        if mood in mood_styles:
            prompt_parts.append(mood_styles[mood])

        # Technical specs
        prompt_parts.extend([
            "cinematic composition",
            "16:9 aspect ratio",
            "high detail",
            "professional photography"
        ])

        return ", ".join(prompt_parts)

    def slice_asset_to_segments(
        self,
        asset,
        target_duration: float,
        num_segments: int = 3
    ) -> List[Tuple[float, float]]:
        """
        Slice long asset into multiple usable segments.
        Returns list of (start_time, end_time) tuples.
        """
        if asset.duration <= target_duration:
            # Use whole clip
            return [(0, asset.duration)]

        segments = []
        segment_duration = target_duration

        # Create segments at different points
        # Start, middle, end
        positions = [0.1, 0.5, 0.8]

        for pos in positions[:num_segments]:
            start = asset.duration * pos
            end = min(start + segment_duration, asset.duration)

            # Ensure minimum duration
            if end - start >= 3:
                segments.append((start, end))

        return segments

    async def get_freshness_report(self) -> Dict:
        """Generate report on asset usage patterns"""

        # Get all assets with usage counts
        all_assets = await self.indexer.query_assets(limit=9999)

        total = len(all_assets)
        never_used = sum(1 for a in all_assets if a.usage_count == 0)
        overused = sum(1 for a in all_assets if a.usage_count > 5)

        # Most used assets
        most_used = sorted(all_assets, key=lambda a: a.usage_count, reverse=True)[:10]

        # Underused gems (high quality, low usage)
        underused = [
            a for a in all_assets
            if a.quality_score > 0.7 and a.usage_count < 2
        ]

        return {
            "total_assets": total,
            "never_used": never_used,
            "overused": overused,
            "utilization_rate": (total - never_used) / total if total > 0 else 0,
            "most_used": [(a.asset_id, a.usage_count) for a in most_used],
            "underused_gems": [a.asset_id for a in underused[:20]],
            "rotation_health": "good" if overused < total * 0.1 else "needs_attention"
        }
