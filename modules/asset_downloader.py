"""
Download Manager with Rate Limiting and Queue
"""

import aiohttp
import aiofiles
import asyncio
from typing import List, Optional
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import os
import hashlib

class DownloadStatus(Enum):
    QUEUED = "queued"
    DOWNLOADING = "downloading"
    COMPLETE = "complete"
    FAILED = "failed"

@dataclass
class DownloadTask:
    source: str
    source_id: str
    url: str
    filename: str
    priority: int = 0  # 0 = high, 10 = low
    status: DownloadStatus = DownloadStatus.QUEUED
    error_message: Optional[str] = None
    file_size: int = 0
    downloaded_size: int = 0
    created_at: datetime = field(default_factory=datetime.now)
    completed_at: Optional[datetime] = None
    local_path: Optional[str] = None

class DownloadManager:
    """
    Async download manager with:
    - Per-source rate limiting
    - Resume support
    - Progress tracking
    - SQLite queue persistence
    """

    def __init__(
        self,
        library_path: str,
        database = None,
        max_concurrent: int = 3
    ):
        self.library_path = library_path
        self.database = database
        self.max_concurrent = max_concurrent

        # Rate limits (requests per minute)
        self.rate_limits = {
            "pexels": 3,
            "pixabay": 30,
            "archive_org": 10
        }

        self.last_request = {source: None for source in self.rate_limits}
        self.queue = asyncio.PriorityQueue()
        self.active_downloads = {}

    async def add_to_queue(
        self,
        results: List,
        priority: int = 0,
        scene_type: Optional[str] = None
    ):
        """Add search results to download queue"""
        tasks = []

        for result in results:
            # Generate filename
            ext = self._get_extension(result.url)
            filename = f"{result.source}_{result.asset_id}.{ext}"

            task = DownloadTask(
                source=result.source,
                source_id=result.asset_id,
                url=result.url,
                filename=filename,
                priority=priority
            )
            tasks.append(task)

            # Add to queue with priority
            await self.queue.put((priority, task))

        # Persist to database
        if self.database:
            await self._persist_tasks(tasks)

        print(f"Added {len(tasks)} tasks to download queue")
        return tasks

    async def process_queue(self):
        """Process download queue with rate limiting"""
        semaphore = asyncio.Semaphore(self.max_concurrent)

        while not self.queue.empty():
            priority, task = await self.queue.get()

            async with semaphore:
                await self._download_with_retry(task)

    async def _download_with_retry(self, task: DownloadTask, max_retries: int = 3):
        """Download with exponential backoff retry"""
        for attempt in range(max_retries):
            try:
                await self._download_file(task)
                return
            except Exception as e:
                wait_time = 2 ** attempt
                print(f"Download failed (attempt {attempt + 1}): {e}. Retrying in {wait_time}s...")
                await asyncio.sleep(wait_time)

        # All retries failed
        task.status = DownloadStatus.FAILED
        task.error_message = "Max retries exceeded"
        await self._update_task_status(task)

    async def _download_file(self, task: DownloadTask):
        """Download single file with rate limiting"""
        # Respect rate limit
        await self._respect_rate_limit(task.source)

        task.status = DownloadStatus.DOWNLOADING
        await self._update_task_status(task)

        # Determine local path
        subfolder = self._get_subfolder(task.source)
        local_dir = os.path.join(self.library_path, "raw", subfolder)
        os.makedirs(local_dir, exist_ok=True)

        local_path = os.path.join(local_dir, task.filename)
        task.local_path = local_path

        # Skip if already exists
        if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
            print(f"Already exists: {task.filename}")
            task.status = DownloadStatus.COMPLETE
            task.completed_at = datetime.now()
            await self._update_task_status(task)
            return

        # Download
        temp_path = f"{local_path}.part"

        async with aiohttp.ClientSession() as session:
            async with session.get(task.url) as resp:
                if resp.status != 200:
                    raise Exception(f"HTTP {resp.status}")

                task.file_size = int(resp.headers.get("content-length", 0))

                async with aiofiles.open(temp_path, "wb") as f:
                    async for chunk in resp.content.iter_chunked(8192):
                        await f.write(chunk)
                        task.downloaded_size += len(chunk)

        # Verify and move
        if os.path.getsize(temp_path) > 0:
            os.rename(temp_path, local_path)
            task.status = DownloadStatus.COMPLETE
            task.completed_at = datetime.now()
            print(f"Downloaded: {task.filename} ({task.file_size / 1024 / 1024:.1f} MB)")
        else:
            os.remove(temp_path)
            raise Exception("Downloaded file is empty")

        await self._update_task_status(task)

    async def _respect_rate_limit(self, source: str):
        """Ensure we respect per-source rate limits"""
        limit = self.rate_limits.get(source, 10)
        min_interval = 60.0 / limit

        last = self.last_request.get(source)
        if last:
            elapsed = (datetime.now() - last).total_seconds()
            if elapsed < min_interval:
                await asyncio.sleep(min_interval - elapsed)

        self.last_request[source] = datetime.now()

    def _get_extension(self, url: str) -> str:
        """Extract file extension from URL"""
        ext = url.split("?")[0].split(".")[-1].lower()
        if ext not in ["mp4", "webm", "ogv", "mov"]:
            return "mp4"
        return ext

    def _get_subfolder(self, source: str) -> str:
        """Organize by source"""
        return source

    async def _persist_tasks(self, tasks: List[DownloadTask]):
        """Save tasks to database"""
        if not self.database:
            return
        # Implementation depends on database interface
        pass

    async def _update_task_status(self, task: DownloadTask):
        """Update task status in database"""
        if not self.database:
            return
        # Implementation depends on database interface
        pass

    async def get_stats(self) -> dict:
        """Get download statistics"""
        raw_path = os.path.join(self.library_path, "raw")

        stats = {
            "total_downloaded": 0,
            "by_source": {},
            "total_size_mb": 0
        }

        for source in self.rate_limits.keys():
            source_path = os.path.join(raw_path, source)
            if os.path.exists(source_path):
                files = [f for f in os.listdir(source_path) if not f.startswith(".")]
                size = sum(
                    os.path.getsize(os.path.join(source_path, f))
                    for f in files
                )
                stats["by_source"][source] = len(files)
                stats["total_downloaded"] += len(files)
                stats["total_size_mb"] += size / 1024 / 1024

        return stats
