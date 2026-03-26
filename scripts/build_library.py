#!/usr/bin/env python3
"""
Build Library Script
Batch download B-roll from multiple sources
"""

import asyncio
import sys
from pathlib import Path

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from kimi_pipeline import MysteryPipeline

# Search terms for mystery content
SEARCH_TERMS = [
    # Atmosphere
    "abandoned house night",
    "dark forest road",
    "foggy landscape",
    "stormy sky",
    "old cemetery",

    # Investigation
    "vintage police station",
    "detective office",
    "evidence board",
    "file cabinet",
    "old newspaper",

    # Locations
    "small town main street",
    "isolated cabin",
    "lighthouse night",
    "train station vintage",
    "bridge night",

    # Objects
    "old photograph",
    "vintage camera",
    "typewriter",
    "microphone retro",
    "telephone old",

    # Abstract
    "shadows moving",
    "dust particles",
    "light leak",
    "film grain",
    "time lapse clouds"
]

async def main():
    target = int(sys.argv[1]) if len(sys.argv) > 1 else 500

    print(f"Building library with target: {target} clips")
    print(f"Search terms: {len(SEARCH_TERMS)}")

    pipeline = MysteryPipeline()
    await pipeline.init()

    await pipeline.phase1_build_library(
        search_queries=SEARCH_TERMS,
        target_clips=target
    )

    # Show final stats
    stats = await pipeline.get_library_stats()
    print(f"\nLibrary complete:")
    print(f"  Total clips: {stats['total_downloaded']}")
    print(f"  Size: {stats['total_size_mb']:.1f} MB")

if __name__ == "__main__":
    asyncio.run(main())
