#!/usr/bin/env python3
"""
Produce Video Script
Generate a single mystery video
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from kimi_pipeline import MysteryPipeline

# Example cases - replace with your research
EXAMPLE_CASES = [
    "The Vanishing at Lake Bodom",
    "The Springfield Three",
    "The Flannan Isles Lighthouse",
    "The Roanoke Colony",
    "The Mary Celeste"
]

async def main():
    topic = sys.argv[1] if len(sys.argv) > 1 else "5 Unsolved Mysteries"
    video_id = sys.argv[2] if len(sys.argv) > 2 else "video_001"

    print(f"Producing: {topic}")
    print(f"Video ID: {video_id}")

    pipeline = MysteryPipeline()
    await pipeline.init()

    video_path = await pipeline.phase2_produce_video(
        topic=topic,
        case_titles=EXAMPLE_CASES,
        video_id=video_id
    )

    print(f"\nOutput: {video_path}")

if __name__ == "__main__":
    asyncio.run(main())
