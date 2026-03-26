"""
Database Models for Mystery Pipeline
SQLite schema and data classes
"""

from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from datetime import datetime
import json
import aiosqlite
import sqlite3

@dataclass
class Asset:
    """Represents a video asset in the library"""
    asset_id: str
    source: str  # "pexels", "pixabay", "archive", "generated"
    source_id: str  # original ID from source
    original_url: str
    local_path: str
    filename: str

    # Technical specs
    duration: float
    width: int
    height: int
    fps: float
    file_size: int
    format: str

    # Content analysis
    tags: List[str] = field(default_factory=list)
    scene_type: Optional[str] = None  # "ext_night", "int_vintage", etc.
    mood: Optional[str] = None  # "tense", "melancholy", etc.
    camera_movement: str = "static"  # "static", "pan", "zoom", "handheld"
    dominant_colors: List[str] = field(default_factory=list)
    brightness_score: float = 0.5  # 0-1

    # Quality & usage
    quality_score: float = 0.0  # 0-1 calculated
    usage_count: int = 0
    last_used: Optional[str] = None  # video_id
    last_used_date: Optional[str] = None
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())

    # Deduplication
    perceptual_hash: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "asset_id": self.asset_id,
            "source": self.source,
            "source_id": self.source_id,
            "original_url": self.original_url,
            "local_path": self.local_path,
            "filename": self.filename,
            "duration": self.duration,
            "width": self.width,
            "height": self.height,
            "fps": self.fps,
            "file_size": self.file_size,
            "format": self.format,
            "tags": json.dumps(self.tags),
            "scene_type": self.scene_type,
            "mood": self.mood,
            "camera_movement": self.camera_movement,
            "dominant_colors": json.dumps(self.dominant_colors),
            "brightness_score": self.brightness_score,
            "quality_score": self.quality_score,
            "usage_count": self.usage_count,
            "last_used": self.last_used,
            "last_used_date": self.last_used_date,
            "created_at": self.created_at,
            "perceptual_hash": self.perceptual_hash
        }

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "Asset":
        return cls(
            asset_id=row["asset_id"],
            source=row["source"],
            source_id=row["source_id"],
            original_url=row["original_url"],
            local_path=row["local_path"],
            filename=row["filename"],
            duration=row["duration"],
            width=row["width"],
            height=row["height"],
            fps=row["fps"],
            file_size=row["file_size"],
            format=row["format"],
            tags=json.loads(row["tags"]) if row["tags"] else [],
            scene_type=row["scene_type"],
            mood=row["mood"],
            camera_movement=row["camera_movement"],
            dominant_colors=json.loads(row["dominant_colors"]) if row["dominant_colors"] else [],
            brightness_score=row["brightness_score"],
            quality_score=row["quality_score"],
            usage_count=row["usage_count"],
            last_used=row["last_used"],
            last_used_date=row["last_used_date"],
            created_at=row["created_at"],
            perceptual_hash=row["perceptual_hash"]
        )

@dataclass
class SearchCache:
    """Cache for API search results"""
    query_hash: str
    query_text: str
    source: str
    results_json: str
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())

@dataclass
class UsageRecord:
    """Track where assets are used"""
    id: Optional[int] = None
    video_id: str = ""
    scene_id: str = ""
    asset_id: str = ""
    used_at: str = field(default_factory=lambda: datetime.now().isoformat())

@dataclass
class DownloadQueue:
    """Queue for background downloads"""
    id: Optional[int] = None
    source: str = ""
    source_id: str = ""
    url: str = ""
    priority: int = 0  # 0 = high, 10 = low
    status: str = "queued"  # "queued", "downloading", "complete", "failed"
    error_message: Optional[str] = None
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    completed_at: Optional[str] = None

class Database:
    """Main database interface"""

    def __init__(self, db_path: str = "./database/pipeline.db"):
        self.db_path = db_path

    async def init(self):
        """Initialize database tables"""
        async with aiosqlite.connect(self.db_path) as db:
            # Assets table
            await db.execute("""
                CREATE TABLE IF NOT EXISTS assets (
                    asset_id TEXT PRIMARY KEY,
                    source TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    original_url TEXT,
                    local_path TEXT NOT NULL,
                    filename TEXT NOT NULL,
                    duration REAL,
                    width INTEGER,
                    height INTEGER,
                    fps REAL,
                    file_size INTEGER,
                    format TEXT,
                    tags TEXT,
                    scene_type TEXT,
                    mood TEXT,
                    camera_movement TEXT DEFAULT 'static',
                    dominant_colors TEXT,
                    brightness_score REAL DEFAULT 0.5,
                    quality_score REAL DEFAULT 0.0,
                    usage_count INTEGER DEFAULT 0,
                    last_used TEXT,
                    last_used_date TEXT,
                    created_at TEXT,
                    perceptual_hash TEXT,
                    UNIQUE(source, source_id)
                )
            """)

            # Search cache
            await db.execute("""
                CREATE TABLE IF NOT EXISTS search_cache (
                    query_hash TEXT PRIMARY KEY,
                    query_text TEXT,
                    source TEXT,
                    results_json TEXT,
                    created_at TEXT
                )
            """)

            # Usage tracking
            await db.execute("""
                CREATE TABLE IF NOT EXISTS usage (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    video_id TEXT NOT NULL,
                    scene_id TEXT NOT NULL,
                    asset_id TEXT NOT NULL,
                    used_at TEXT,
                    FOREIGN KEY (asset_id) REFERENCES assets(asset_id)
                )
            """)

            # Download queue
            await db.execute("""
                CREATE TABLE IF NOT EXISTS download_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    url TEXT NOT NULL,
                    priority INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'queued',
                    error_message TEXT,
                    created_at TEXT,
                    completed_at TEXT,
                    UNIQUE(source, source_id)
                )
            """)

            # Indexes
            await db.execute("CREATE INDEX IF NOT EXISTS idx_assets_tags ON assets(tags)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_assets_scene_type ON assets(scene_type)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_assets_mood ON assets(mood)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_usage_video ON usage(video_id)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_usage_asset ON usage(asset_id)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_queue_status ON download_queue(status)")

            await db.commit()

    async def insert_asset(self, asset: Asset) -> bool:
        """Insert or update asset"""
        try:
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute("""
                    INSERT OR REPLACE INTO assets
                    (asset_id, source, source_id, original_url, local_path, filename,
                     duration, width, height, fps, file_size, format,
                     tags, scene_type, mood, camera_movement, dominant_colors,
                     brightness_score, quality_score, usage_count, last_used, last_used_date,
                     created_at, perceptual_hash)
                    VALUES
                    (:asset_id, :source, :source_id, :original_url, :local_path, :filename,
                     :duration, :width, :height, :fps, :file_size, :format,
                     :tags, :scene_type, :mood, :camera_movement, :dominant_colors,
                     :brightness_score, :quality_score, :usage_count, :last_used, :last_used_date,
                     :created_at, :perceptual_hash)
                """, asset.to_dict())
                await db.commit()
                return True
        except Exception as e:
            print(f"Error inserting asset: {e}")
            return False

    async def get_asset(self, asset_id: str) -> Optional[Asset]:
        """Get asset by ID"""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM assets WHERE asset_id = ?", (asset_id,)
            ) as cursor:
                row = await cursor.fetchone()
                return Asset.from_row(row) if row else None

    async def query_assets(
        self,
        scene_type: Optional[str] = None,
        mood: Optional[str] = None,
        tags: Optional[List[str]] = None,
        min_duration: float = 0,
        max_duration: float = 9999,
        exclude_recent_from: Optional[str] = None,
        freshness_days: int = 30,
        limit: int = 10
    ) -> List[Asset]:
        """Query assets with filters"""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row

            query = "SELECT * FROM assets WHERE 1=1"
            params = []

            if scene_type:
                query += " AND scene_type = ?"
                params.append(scene_type)

            if mood:
                query += " AND mood = ?"
                params.append(mood)

            if min_duration:
                query += " AND duration >= ?"
                params.append(min_duration)

            if max_duration:
                query += " AND duration <= ?"
                params.append(max_duration)

            if exclude_recent_from:
                # Exclude assets used in recent videos
                query += """ AND asset_id NOT IN (
                    SELECT asset_id FROM usage
                    WHERE video_id != ?
                    AND used_at > datetime('now', '-{} days')
                )""".format(freshness_days)
                params.append(exclude_recent_from)

            # Order by quality and freshness
            query += " ORDER BY quality_score DESC, usage_count ASC, created_at DESC"
            query += f" LIMIT {limit}"

            async with db.execute(query, params) as cursor:
                rows = await cursor.fetchall()
                return [Asset.from_row(row) for row in rows]
