"""
Mystery Pipeline - Main Orchestrator
Top 5 Mystery video production pipeline
"""

import os
import asyncio
import yaml
from typing import List, Optional
from pathlib import Path

# Import components
from database.models import Database, Asset
from modules.asset_searcher import MultiSourceSearcher
from modules.asset_downloader import DownloadManager
from modules.asset_indexer import AssetIndexer
from modules.mystery_script_engine import ScriptEngine, Scene
from modules.asset_matcher import BRollMatcher
from modules.asset_assembler import VideoAssembler
from modules.asset_tracker import UsageTracker

class MysteryPipeline:
    """
    Main pipeline orchestrator for mystery video production.

    Usage:
        pipeline = MysteryPipeline("config/pipeline_config.yaml")
        await pipeline.init()

        # Phase 1: Build library
        await pipeline.phase1_build_library([
            "abandoned house night",
            "dark forest road",
            "vintage police station",
            "old newspaper",
            "mystery fog"
        ], target_clips=500)

        # Phase 2: Produce video
        video_path = await pipeline.phase2_produce_video(
            topic="5 Unsolved Disappearances",
            case_titles=[
                "The Vanishing at Lake Bodom",
                "The Springfield Three",
                "The Flannan Isles Lighthouse",
                "The Roanoke Colony",
                "The Mary Celeste"
            ],
            video_id="video_001"
        )
    """

    def __init__(self, config_path: str = "config/pipeline_config.yaml"):
        self.config_path = config_path
        self.config = self._load_config()

        # Paths
        self.library_path = self.config["library"]["path"]
        self.output_path = self.config["production"]["output_path"]
        self.db_path = "./database/pipeline.db"

        # Components (initialized in init())
        self.db = None
        self.searcher = None
        self.downloader = None
        self.indexer = None
        self.script_engine = None
        self.matcher = None
        self.assembler = None
        self.tracker = None

    def _load_config(self) -> dict:
        """Load YAML configuration"""
        with open(self.config_path, 'r') as f:
            return yaml.safe_load(f)

    async def init(self):
        """Initialize all components"""
        print("Initializing Mystery Pipeline...")

        # Database
        self.db = Database(self.db_path)
        await self.db.init()
        print("Database initialized")

        # Sourcing
        self.searcher = MultiSourceSearcher(
            pexels_key=os.getenv("PEXELS_API_KEY"),
            pixabay_key=os.getenv("PIXABAY_API_KEY"),
            cache_db=self.db
        )

        self.downloader = DownloadManager(
            library_path=self.library_path,
            database=self.db
        )

        self.indexer = AssetIndexer(self.library_path, self.db)

        # Production
        self.script_engine = ScriptEngine(
            target_duration=self.config["pipeline"]["target_duration"],
            words_per_minute=self.config["pipeline"]["word_per_minute"],
            cases_per_video=self.config["pipeline"]["cases_per_video"]
        )

        self.tracker = UsageTracker(self.db)
        self.matcher = BRollMatcher(self.indexer, self.tracker)

        self.assembler = VideoAssembler(
            output_path=self.output_path,
            temp_path=os.path.join(self.output_path, "temp")
        )

        print("All components ready")

    async def phase1_build_library(
        self,
        search_queries: List[str],
        target_clips: int = 1000
    ):
        """
        Background job: Fill asset library from multiple sources.
        Run this overnight or in batches to build your B-roll collection.
        """
        print(f"\nPhase 1: Building Library (target: {target_clips} clips)")

        all_results = []

        for query in search_queries:
            print(f"  Searching: {query}")
            results = await self.searcher.search(query, use_cache=True)

            # Filter for quality
            quality_results = [
                r for r in results
                if r.width >= 1080 and r.duration and r.duration >= 3
            ]

            all_results.extend(quality_results)
            print(f"    Found {len(quality_results)} quality clips")

            # Early stop if we have enough
            if len(all_results) >= target_clips * 2:  # 2x for deduplication
                break

        # Deduplicate
        seen_urls = set()
        unique_results = []
        for r in all_results:
            if r.url not in seen_urls:
                seen_urls.add(r.url)
                unique_results.append(r)

        print(f"  Total unique clips: {len(unique_results)}")

        # Queue downloads
        await self.downloader.add_to_queue(unique_results, priority=0)

        # Process downloads
        print("  Starting downloads...")
        await self.downloader.process_queue()

        # Index new downloads
        print("  Indexing assets...")
        await self.indexer.scan_and_index()

        # Report
        stats = await self.downloader.get_stats()
        print(f"\nLibrary build complete:")
        print(f"  Total clips: {stats['total_downloaded']}")
        print(f"  By source: {stats['by_source']}")
        print(f"  Total size: {stats['total_size_mb']:.1f} MB")

    async def phase2_produce_video(
        self,
        topic: str,
        case_titles: List[str],
        video_id: str,
        music_track: Optional[str] = None
    ) -> str:
        """
        Main production flow:
        1. Generate script with scenes
        2. Match B-roll to scenes
        3. Generate TTS audio
        4. Assemble scenes
        5. Composite final video
        """
        print(f"\nPhase 2: Producing Video '{video_id}'")

        # 1. Generate script
        print("  1. Generating script...")
        scenes = self.script_engine.create_template(topic, case_titles)

        # Validate
        validation = self.script_engine.validate_script(scenes)
        if not validation["valid"]:
            print(f"  Script issues: {validation['issues']}")

        print(f"  {len(scenes)} scenes, {validation['total_duration']:.0f}s total")

        # 2. Match B-roll
        print("  2. Matching B-roll...")
        matches = await self.matcher.match_scenes(
            scenes,
            video_id,
            variety_target=self.config["library"]["freshness_lookback"]
        )

        # Report matches
        matched = sum(1 for m in matches.values() if m.primary_match)
        generated = sum(1 for m in matches.values() if m.fallback_strategy == "generated_fill")
        print(f"  Matched: {matched}/{len(scenes)}, Need generation: {generated}")

        # 3. Generate TTS audio
        print("  3. Generating audio...")
        audio_files = await self._generate_tts(scenes, video_id)

        # 4. Assemble scenes
        print("  4. Assembling scenes...")
        scene_files = []

        for scene in scenes:
            match = matches[scene.scene_id]

            if not match.primary_match:
                # Generate placeholder or use fallback
                print(f"    {scene.scene_id}: No B-roll, using generated")
                # TODO: Generate AI image or use color background
                continue

            # Get asset
            asset = await self.db.get_asset(match.primary_match)
            if not asset:
                continue

            # Get audio
            audio_path = audio_files.get(scene.scene_id)
            if not audio_path:
                continue

            # Determine effects
            effects = []
            if asset.source == "archive_org":
                effects.append("vintage")
            if scene.mood == "tense":
                effects.append("grain")

            # Assemble
            scene_file = self.assembler.assemble_scene(
                scene,
                asset.local_path,
                audio_path,
                effects=effects,
                text_overlay=scene.text_overlay
            )

            if scene_file:
                scene_files.append(scene_file)
                # Record usage
                await self.tracker.record_usage(
                    video_id, scene.scene_id, asset.asset_id
                )

        print(f"  Assembled {len(scene_files)} scenes")

        # 5. Composite final
        print("  5. Compositing final video...")
        final_path = self.assembler.composite_final(
            scene_files,
            background_music=music_track,
            output_filename=f"{video_id}.mp4"
        )

        if final_path:
            print(f"\nVideo complete: {final_path}")

        return final_path

    async def _generate_tts(self, scenes: List[Scene], video_id: str) -> dict:
        """Generate TTS audio for all scenes"""
        import edge_tts
        import asyncio

        audio_dir = os.path.join(self.output_path, "audio", video_id)
        os.makedirs(audio_dir, exist_ok=True)

        audio_files = {}

        for scene in scenes:
            if not scene.audio_script:
                continue

            output_file = os.path.join(audio_dir, f"{scene.scene_id}.mp3")

            # Skip if exists
            if os.path.exists(output_file):
                audio_files[scene.scene_id] = output_file
                continue

            # Generate with edge-tts
            communicate = edge_tts.Communicate(
                scene.audio_script,
                voice="en-US-GuyNeural",  # Deep, documentary voice
                rate="-10%"  # Slightly slower for drama
            )
            await communicate.save(output_file)

            audio_files[scene.scene_id] = output_file

        return audio_files

    async def get_library_stats(self) -> dict:
        """Get current library statistics"""
        download_stats = await self.downloader.get_stats()
        freshness_report = await self.tracker.generate_freshness_report()

        return {
            **download_stats,
            **freshness_report
        }

    async def suggest_topics(self, count: int = 10) -> List[str]:
        """Suggest underused B-roll themes for next videos"""
        # Get scene types with low usage
        # TODO: Implement based on actual usage patterns
        return [
            "abandoned hospital",
            "vintage police car",
            "old photograph",
            "forest at dusk",
            "small town main street"
        ][:count]


# CLI entry point
async def main():
    """Example usage"""
    pipeline = MysteryPipeline()
    await pipeline.init()

    # Example: Build library
    # await pipeline.phase1_build_library([
    #     "abandoned place night",
    #     "mystery fog forest",
    #     "vintage investigation"
    # ], target_clips=100)

    # Example: Produce video
    # await pipeline.phase2_produce_video(
    #     topic="5 Unsolved Mysteries",
    #     case_titles=["Case A", "Case B", "Case C", "Case D", "Case E"],
    #     video_id="test_001"
    # )

    # Show stats
    stats = await pipeline.get_library_stats()
    print("\nLibrary Stats:")
    import json
    print(json.dumps(stats, indent=2))

if __name__ == "__main__":
    asyncio.run(main())
