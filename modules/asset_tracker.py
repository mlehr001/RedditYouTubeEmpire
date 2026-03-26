"""
Usage Tracker - Prevent B-roll repetition across videos
"""

from typing import Set, List, Dict, Optional
from datetime import datetime, timedelta
import json
import aiosqlite

class UsageTracker:
    """
    Tracks asset usage across videos to prevent repetition.
    Implements freshness scoring and rotation recommendations.
    """

    def __init__(self, database):
        self.db = database

    async def record_usage(
        self,
        video_id: str,
        scene_id: str,
        asset_id: str
    ):
        """Log that an asset was used in a scene"""
        from database.models import UsageRecord

        record = UsageRecord(
            video_id=video_id,
            scene_id=scene_id,
            asset_id=asset_id
        )

        # Insert into database
        async with aiosqlite.connect(self.db.db_path) as conn:
            await conn.execute("""
                INSERT INTO usage (video_id, scene_id, asset_id, used_at)
                VALUES (?, ?, ?, ?)
            """, (record.video_id, record.scene_id, record.asset_id, record.used_at))
            await conn.commit()

        # Update asset usage count
        await self._increment_asset_usage(asset_id, video_id)

    async def _increment_asset_usage(self, asset_id: str, video_id: str):
        """Update asset usage count and last used"""
        async with aiosqlite.connect(self.db.db_path) as conn:
            await conn.execute("""
                UPDATE assets
                SET usage_count = usage_count + 1,
                    last_used = ?,
                    last_used_date = datetime('now')
                WHERE asset_id = ?
            """, (video_id, asset_id))
            await conn.commit()

    async def get_recent_assets(
        self,
        current_video_id: str,
        lookback: int = 10
    ) -> Set[str]:
        """
        Get set of asset IDs used in last N videos.
        Exclude these from current video to prevent repetition.
        """
        async with aiosqlite.connect(self.db.db_path) as conn:
            conn.row_factory = aiosqlite.Row

            # Get last N video IDs (excluding current)
            cursor = await conn.execute("""
                SELECT DISTINCT video_id FROM usage
                WHERE video_id != ?
                ORDER BY used_at DESC
                LIMIT ?
            """, (current_video_id, lookback))

            recent_videos = [row["video_id"] for row in await cursor.fetchall()]

            if not recent_videos:
                return set()

            # Get all assets used in those videos
            placeholders = ",".join(["?"] * len(recent_videos))
            cursor = await conn.execute(f"""
                SELECT DISTINCT asset_id FROM usage
                WHERE video_id IN ({placeholders})
            """, recent_videos)

            assets = [row["asset_id"] for row in await cursor.fetchall()]
            return set(assets)

    async def get_asset_rotation_score(self, asset_id: str) -> float:
        """
        Calculate freshness score 0.0-1.0
        0.0 = just used, avoid
        1.0 = fresh, priority use
        """
        async with aiosqlite.connect(self.db.db_path) as conn:
            conn.row_factory = aiosqlite.Row

            # Get asset info
            cursor = await conn.execute(
                "SELECT * FROM assets WHERE asset_id = ?",
                (asset_id,)
            )
            row = await cursor.fetchone()

            if not row:
                return 0.5  # New asset, neutral

            usage_count = row["usage_count"]
            last_used_date = row["last_used_date"]

            # Base score: inverse of usage count
            usage_score = 1.0 / (1 + usage_count * 0.5)

            # Time decay bonus
            time_score = 1.0
            if last_used_date:
                last = datetime.fromisoformat(last_used_date)
                days_since = (datetime.now() - last).days
                if days_since < 7:
                    time_score = 0.0  # Just used
                elif days_since < 30:
                    time_score = 0.5
                else:
                    time_score = min(1.0, days_since / 60)

            return (usage_score * 0.6) + (time_score * 0.4)

    async def generate_freshness_report(self) -> Dict:
        """Generate library health report"""
        async with aiosqlite.connect(self.db.db_path) as conn:
            conn.row_factory = aiosqlite.Row

            # Total assets
            cursor = await conn.execute("SELECT COUNT(*) as count FROM assets")
            total = (await cursor.fetchone())["count"]

            # Never used
            cursor = await conn.execute(
                "SELECT COUNT(*) as count FROM assets WHERE usage_count = 0"
            )
            never_used = (await cursor.fetchone())["count"]

            # Overused (>5 times)
            cursor = await conn.execute(
                "SELECT COUNT(*) as count FROM assets WHERE usage_count > 5"
            )
            overused = (await cursor.fetchone())["count"]

            # Most used
            cursor = await conn.execute("""
                SELECT asset_id, usage_count FROM assets
                ORDER BY usage_count DESC
                LIMIT 10
            """)
            most_used = [(r["asset_id"], r["usage_count"]) for r in await cursor.fetchall()]

            # Underused gems (high quality, low usage)
            cursor = await conn.execute("""
                SELECT asset_id FROM assets
                WHERE quality_score > 0.7 AND usage_count < 2
                ORDER BY quality_score DESC
                LIMIT 20
            """)
            underused = [r["asset_id"] for r in await cursor.fetchall()]

            return {
                "total_assets": total,
                "never_used": never_used,
                "overused": overused,
                "utilization_rate": (total - never_used) / total if total > 0 else 0,
                "most_used": most_used,
                "underused_gems": underused,
                "rotation_health": "good" if overused < total * 0.1 else "needs_attention",
                "generated_at": datetime.now().isoformat()
            }

    async def suggest_rotation(self, target_count: int = 10) -> List[str]:
        """Suggest assets to use next (high freshness score)"""
        async with aiosqlite.connect(self.db.db_path) as conn:
            conn.row_factory = aiosqlite.Row

            # Get candidates with low usage
            cursor = await conn.execute("""
                SELECT asset_id, usage_count, last_used_date, quality_score
                FROM assets
                WHERE usage_count < 3 AND quality_score > 0.5
                ORDER BY usage_count ASC, quality_score DESC
                LIMIT 50
            """)

            candidates = []
            for row in await cursor.fetchall():
                score = await self.get_asset_rotation_score(row["asset_id"])
                candidates.append((row["asset_id"], score))

            # Sort by score, return top
            candidates.sort(key=lambda x: x[1], reverse=True)
            return [a for a, _ in candidates[:target_count]]
