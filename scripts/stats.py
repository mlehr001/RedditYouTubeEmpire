#!/usr/bin/env python3
"""
Library Stats Script
View current library health
"""

import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from kimi_pipeline import MysteryPipeline

async def main():
    pipeline = MysteryPipeline()
    await pipeline.init()

    stats = await pipeline.get_library_stats()

    print("\nLibrary Statistics\n")
    print(f"Total clips: {stats['total_downloaded']}")
    print(f"Storage used: {stats['total_size_mb']:.1f} MB")
    print(f"\nBy source:")
    for source, count in stats['by_source'].items():
        print(f"  {source}: {count}")

    print(f"\nFreshness:")
    print(f"  Never used: {stats['never_used']}")
    print(f"  Overused (>5x): {stats['overused']}")
    print(f"  Utilization: {stats['utilization_rate']*100:.1f}%")
    print(f"  Health: {stats['rotation_health']}")

    if stats['most_used']:
        print(f"\nMost used assets:")
        for asset_id, count in stats['most_used'][:5]:
            print(f"  {asset_id}: {count}x")

    if '--json' in sys.argv:
        print("\n" + json.dumps(stats, indent=2))

if __name__ == "__main__":
    asyncio.run(main())
