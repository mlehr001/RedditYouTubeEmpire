"""
Content-to-Video Integration
Connects content sourcing directly to video production
"""

import asyncio
import json
from pathlib import Path
from typing import Optional
from datetime import datetime

from modules.content_sourcing import ContentCurator, AIScriptWriter
from kimi_pipeline import MysteryPipeline

class AutomatedChannel:
    """
    Fully automated mystery channel workflow:
    1. Source topics → 2. Generate AI prompts → 3. Produce video
    """

    def __init__(self):
        self.curator = ContentCurator()
        self.pipeline = MysteryPipeline()
        self.script_writer = AIScriptWriter()

    async def init(self):
        await self.pipeline.init()

    async def create_daily_video(
        self,
        video_id: Optional[str] = None,
        auto_generate_script: bool = False  # Set True if you have AI API
    ) -> str:
        """
        Full automation:
        - Find 5 mystery cases
        - Generate AI script prompts
        - (Optional) Call AI to write script
        - Produce video
        """
        if not video_id:
            video_id = f"mystery_{datetime.now().strftime('%Y%m%d')}"

        print(f"\nCreating video: {video_id}")

        # 1. Source topics
        print("\n1. Sourcing mystery topics...")
        cases = await self.curator.get_daily_topics(count=5)

        video_title = f"5 Unsolved Mysteries: {cases[0].title} and More"

        print(f"   Selected: {[c.title for c in cases]}")

        # 2. Generate production package
        print("\n2. Generating production package...")
        package = self.curator.generate_production_package(cases, video_title)

        # Save prompts for manual AI generation (or auto-call if enabled)
        prompts_dir = Path(f"./output/prompts/{video_id}")
        prompts_dir.mkdir(parents=True, exist_ok=True)

        with open(prompts_dir / "full_script_prompt.txt", "w") as f:
            f.write(package["ai_full_script_prompt"])

        with open(prompts_dir / "case_prompts.json", "w") as f:
            json.dump(package["ai_case_prompts"], f, indent=2)

        with open(prompts_dir / "research_notes.json", "w") as f:
            json.dump(package["research_notes"], f, indent=2)

        print(f"   Saved prompts to: {prompts_dir}")

        # 3. Download B-roll (parallel with script writing)
        print("\n3. Acquiring B-roll...")
        search_terms = package["research_notes"]["b_roll_search_terms"]

        # Check if we need more B-roll
        stats = await self.pipeline.get_library_stats()
        if stats["total_downloaded"] < 200:
            print(f"   Library low ({stats['total_downloaded']} clips), downloading...")
            await self.pipeline.phase1_build_library(search_terms[:5], target_clips=50)
        else:
            print(f"   Library sufficient ({stats['total_downloaded']} clips)")

        # 4. Produce video (with placeholder script or AI-generated)
        if auto_generate_script:
            # You'd implement AI call here
            print("\n4. Auto-generating script via AI...")
            # script = await self._call_ai_api(package["ai_full_script_prompt"])
            # scenes = self.script_writer.parse_ai_output_to_scenes(script)
            pass
        else:
            print("\n4. Ready for script generation:")
            print(f"   → Open: {prompts_dir}/full_script_prompt.txt")
            print(f"   → Paste to ChatGPT/Claude")
            print(f"   → Save output to: {prompts_dir}/ai_output.txt")
            print(f"   → Then run: python produce_with_ai_script.py {video_id}")

            # For now, use template script
            video_path = await self.pipeline.phase2_produce_video(
                topic=video_title,
                case_titles=[c.title for c in cases],
                video_id=video_id
            )

        return video_path

    async def produce_with_ai_script(self, video_id: str, ai_output_path: str):
        """
        Second step: After you paste AI output, produce the video.
        """
        print(f"\nProducing {video_id} with AI script...")

        # Load AI output
        with open(ai_output_path, "r") as f:
            ai_script = f.read()

        # Parse into scenes
        scenes = self.script_writer.parse_ai_output_to_scenes(ai_script)

        print(f"   Parsed {len(scenes)} scenes")

        # Match B-roll to each scene
        matches = await self.pipeline.matcher.match_scenes(
            scenes,
            video_id,
            variety_target=10
        )

        # Assemble
        # ... (rest of production flow)

        return "video_path"


# Quick CLI commands
async def quick_daily():
    """Generate today's video package"""
    channel = AutomatedChannel()
    await channel.init()

    video_path = await channel.create_daily_video(
        video_id=f"daily_{datetime.now().strftime('%m%d')}"
    )

    print(f"\nVideo ready: {video_path}")

async def quick_research():
    """Just get topics and prompts"""
    curator = ContentCurator()

    cases = await curator.get_daily_topics(5)
    package = curator.generate_production_package(cases)

    print("\nAI Script Prompt:")
    print("=" * 50)
    print(package["ai_full_script_prompt"][:1000] + "...")
    print("=" * 50)

    print(f"\nB-Roll Terms: {package['research_notes']['b_roll_search_terms'][:5]}")

if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "research":
        asyncio.run(quick_research())
    else:
        asyncio.run(quick_daily())
